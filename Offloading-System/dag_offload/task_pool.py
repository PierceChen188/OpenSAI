"""进程池执行 + 资源控制手环（T4.2，进程管理版，无需容器）。

把 CPU 密集的算法执行放进**预建的 worker 进程池**：
  - 控并发 + 复用：最多 N 个任务同时算，worker 循环复用（不每任务现开现杀）；
  - 真并行：用进程而非线程，绕开 Python GIL，能真正用多核；
  - 资源控制手环：每个 worker 被**钉在一组 CPU 核上**（cpu_affinity，限算力，跨平台），
    并可选设内存硬上限（POSIX 的 RLIMIT_AS；Windows 暂无 RLIMIT，留作后续作业对象）。

健壮性要点（吃过的坑）：
  - 不用 multiprocessing.Manager 传核分配——Windows spawn 下 worker 连 Manager 代理会卡死；
    改为 worker 起好后，由**主进程**按 PID 直接 cpu_affinity 绑核（psutil 跨平台可用）。
  - 池在**后台线程**里构建并做**带超时的预热自检**：预热不过就判定不可用、返回 None，
    调用方回退到主进程内执行——保证设备照常注册、任务照常出结果。
  - 这些函数定义在独立模块（不在 __main__ 脚本里），避开 Windows spawn 重新执行入口脚本的坑。
"""
import concurrent.futures as _cf
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

_JOB_HANDLE = None   # 保活 Windows 作业对象句柄（关闭会解除限制），进程退出时自动释放


def _win_set_process_memory_limit(mem_bytes):
    """Windows 内存硬上限：把当前进程加入一个设了 ProcessMemoryLimit 的作业对象(Job Object)，
    超过该提交内存的分配将失败(Python 抛 MemoryError)。返回是否设置成功。"""
    global _JOB_HANDLE
    try:
        import ctypes
        from ctypes import wintypes
        k32 = ctypes.WinDLL('kernel32', use_last_error=True)

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [(n, ctypes.c_ulonglong) for n in (
                'ReadOperationCount', 'WriteOperationCount', 'OtherOperationCount',
                'ReadTransferCount', 'WriteTransferCount', 'OtherTransferCount')]

        class BASIC(ctypes.Structure):
            _fields_ = [('PerProcessUserTimeLimit', ctypes.c_int64),
                        ('PerJobUserTimeLimit', ctypes.c_int64),
                        ('LimitFlags', wintypes.DWORD),
                        ('MinimumWorkingSetSize', ctypes.c_size_t),
                        ('MaximumWorkingSetSize', ctypes.c_size_t),
                        ('ActiveProcessLimit', wintypes.DWORD),
                        ('Affinity', ctypes.c_size_t),
                        ('PriorityClass', wintypes.DWORD),
                        ('SchedulingClass', wintypes.DWORD)]

        class EXT(ctypes.Structure):
            _fields_ = [('BasicLimitInformation', BASIC), ('IoInfo', IO_COUNTERS),
                        ('ProcessMemoryLimit', ctypes.c_size_t),
                        ('JobMemoryLimit', ctypes.c_size_t),
                        ('PeakProcessMemoryUsed', ctypes.c_size_t),
                        ('PeakJobMemoryUsed', ctypes.c_size_t)]

        JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100
        JobObjectExtendedLimitInformation = 9
        k32.CreateJobObjectW.restype = wintypes.HANDLE
        k32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
        k32.SetInformationJobObject.restype = wintypes.BOOL
        k32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD]
        k32.AssignProcessToJobObject.restype = wintypes.BOOL
        k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        k32.GetCurrentProcess.restype = wintypes.HANDLE

        h = k32.CreateJobObjectW(None, None)
        if not h:
            return False
        info = EXT()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_PROCESS_MEMORY
        info.ProcessMemoryLimit = int(mem_bytes)
        if not k32.SetInformationJobObject(h, JobObjectExtendedLimitInformation,
                                           ctypes.byref(info), ctypes.sizeof(info)):
            return False
        if not k32.AssignProcessToJobObject(h, k32.GetCurrentProcess()):
            return False
        _JOB_HANDLE = h   # 句柄保活
        return True
    except Exception as e:  # noqa: BLE001
        logging.warning(f"[pool] Windows 内存上限设置失败: {e}")
        return False


def _worker_init():
    """每个 worker 进程启动时执行一次：（可选）内存上限 + 预热算法。绑核由主进程事后完成。"""
    mem_mb = int(os.environ.get('OPENSAI_TASK_MEM_MB', '0') or 0)
    if mem_mb > 0:
        cap = mem_mb * 1024 * 1024
        if sys.platform == 'win32':
            _win_set_process_memory_limit(cap)        # 作业对象硬上限
        else:
            try:
                import resource  # Unix-only
                resource.setrlimit(resource.RLIMIT_AS, (cap, cap))
            except Exception:
                pass
    # 每个 worker 各自在自己的主线程导入重型 C 扩展(numpy/torch)，避免首次在任务子线程导入死锁。
    try:
        from device.implements.actuator_impl import ActuatorImpl
        ActuatorImpl.preload_algorithms()
    except Exception as e:  # noqa: BLE001
        logging.warning(f"[pool] worker 预热失败: {e}")


def _warm():
    time.sleep(0.3)   # 略停，迫使多个 worker 都被拉起（而非一个 worker 连干）
    return os.getpid()


def run_task(algorithm_name, param_dict, data_dict):
    """在 worker 进程里直接执行算法（进程池+绑核就是资源控制机制，不再走 Docker）。
    只接收可序列化的 str/dict；输入/输出走磁盘文件路径，已在 param_dict 内。"""
    from device.implements.actuator_impl import ActuatorImpl
    return ActuatorImpl.run_actuator(algorithm_name, param_dict, data_dict)


def build_pool():
    """构建并自检进程池，返回 (executor, None, slices)；不可用则 (None, None, None)。
    N 由 OPENSAI_TASK_WORKERS 指定（默认 4）；预热超时由 OPENSAI_POOL_WARMUP_S（默认 60s）。
    建议在后台线程里调用（内部预热会阻塞数秒~数十秒）。"""
    try:
        import psutil
        n = max(1, int(os.environ.get('OPENSAI_TASK_WORKERS', '4') or 4))
        total = psutil.cpu_count(logical=True) or n
        k = max(1, total // n)
        slices = []
        for i in range(n):
            s = list(range(i * k, min((i + 1) * k, total)))
            slices.append(s if s else [i % total])

        ex = ProcessPoolExecutor(max_workers=n, initializer=_worker_init)

        # 预热自检：强制把 worker 都拉起并跑通一个空任务；超时则判定不可用、回退。
        warm = [ex.submit(_warm) for _ in range(n)]
        timeout = float(os.environ.get('OPENSAI_POOL_WARMUP_S', '15') or 15)
        done, pending = _cf.wait(warm, timeout=timeout)
        if pending:
            logging.error(f"[pool] 预热超时(>{timeout}s)，判定进程池不可用，回退线程内执行")
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            return None, None, None

        # worker 已就绪：由主进程按 PID 绑核（不依赖 Manager 代理，避开 Windows spawn 卡死）。
        try:
            pids = list(getattr(ex, '_processes', {}) or {})
            for i, pid in enumerate(pids):
                try:
                    psutil.Process(pid).cpu_affinity(slices[i % len(slices)])
                except Exception as e:  # noqa: BLE001
                    logging.warning(f"[pool] 绑核失败 pid={pid}: {e}")
        except Exception as e:  # noqa: BLE001
            logging.warning(f"[pool] 绑核阶段异常: {e}")

        logging.info(f"[pool] 进程池就绪: {len(pids)}/{n} 个 worker, 核分配={slices}")
        return ex, None, slices
    except Exception as e:  # noqa: BLE001
        logging.error(f"[pool] 进程池启动失败，将回退线程内执行: {e}")
        return None, None, None
