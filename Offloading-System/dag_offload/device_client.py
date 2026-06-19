import json
import logging
import threading
import time
import socket
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 将上一级目录添加到Python的模块搜索路径
# 添加容器管理接口导入
from device.interfaces.resource_control_interface import ResourceControlInterface
from config.logging_config import LOG_FORMAT
from config.network_config import HOST, PORT
from device.device_monitor import DeviceMonitor, get_gl_device_no
from device.implements.actuator_impl import ActuatorImpl
from network.utils.network_utils import NetworkUtils
from network.dataplane.quic_server import ArtifactServer
from network.dataplane.quic_client import fetch_blob, fetch_striped
from config.dataplane_config import DATA_PORT, ARTIFACTS_DIR
from dag_offload.task_pool import build_pool, run_task
from concurrent.futures.process import BrokenProcessPool
from concurrent.futures import ThreadPoolExecutor, as_completed

name = "DEVICE_" + get_gl_device_no()


class CoreAllocator:
    """设备侧 CPU 核租借器(T3.2-B)：任务运行时租 K 个空闲核(disjoint)、跑完归还，
    主进程把具体核号经 _alloc 传给 worker 去绑核。空闲不足时回退到核 0(软超额、不阻塞)。"""

    def __init__(self, total):
        self._total = max(1, int(total))
        self._free = list(range(self._total))
        self._lock = threading.Lock()

    def lease(self, k):
        k = max(1, int(k))
        with self._lock:
            n = min(k, len(self._free))
            if n == 0:
                return [0]                 # 无空闲核：与他人共享 0 号核（软超额）
            leased = self._free[:n]
            self._free = self._free[n:]
            return leased

    def release(self, cores):
        with self._lock:
            for c in cores:
                if 0 <= c < self._total and c not in self._free:
                    self._free.append(c)
            self._free.sort()

    def committed(self):
        with self._lock:
            return self._total - len(self._free)


class DeviceClient:
    # 添加容器管理器实例（支持容器化任务执行和复用）
    container_controller = ResourceControlInterface()
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    # 数据面：本机产物服务器（在 activate_client 中启动）
    artifact_server = None

    # T4.2 算力资源控制：进程池 + 绑核手环（在 activate_client 中构建）。
    # 任务的算法执行交给池里的 worker（控并发+复用+真并行，每个 worker 钉在一组核上=限算力）；
    # 池不可用时回退到主进程内执行（execute_task），保证仍能正常运行。
    pool = None
    _pool_mgr = None
    worker_slots = 1                  # 并行容量(=进程池 worker 数)；供放置做 capacity-aware 负载
    core_alloc = None                 # T3.2-B CPU 核租借器（activate_client 中按总核数初始化）
    committed_mem_mb = 0              # T3.2-B 当前已承诺内存(MB)，供上报"已承诺/可用"(T1.1)

    # —— 监测信号（线程间共享，由心跳线程统一上报到 server/Redis）——
    running_tasks = 0                 # 当前正在执行的任务数 -> work_status
    running_lock = threading.Lock()
    last_hb_sent = 0.0                # 最近一次心跳发送时刻（测 RTT 用）
    last_rtt_ms = 0.0                 # 最近一次服务器↔设备往返时延(ms)
    observed_links = {}               # 链路遥测(被动+主动): "src_ip:data_port" -> {mbps,last_ms,bytes,samples}
    links_lock = threading.Lock()
    peers = []                        # 在线对端清单(来自心跳ACK): [{device_no,ip_address,data_port}]
    PROBE_ID = '__probe__'            # 主动探测用的固定 artifact id
    PROBE_INTERVAL = int(os.environ.get('OPENSAI_PROBE_INTERVAL', '20'))  # 秒；主动探测间隔
    _nic_ips_cache = None             # P0-1c 多网卡：本机可用 NIC IPv4 列表（枚举一次后缓存）

    @classmethod
    def _local_nic_ips(cls):
        """P0-1c 多网卡聚合：本机可对外提供数据面的 NIC IPv4 列表（去回环/链路本地、去重）。
        可用 OPENSAI_NIC_IPS="ip1,ip2" 显式覆盖（单机验证：用 127.0.0.1+本机LAN IP 模拟两张网卡）。"""
        if cls._nic_ips_cache is not None:
            return cls._nic_ips_cache
        env = os.environ.get('OPENSAI_NIC_IPS', '').strip()
        ips = []
        if env:
            ips = [x.strip() for x in env.split(',') if x.strip()]
        else:
            try:
                import psutil
                for _name, addrs in psutil.net_if_addrs().items():
                    for a in addrs:
                        if a.family == socket.AF_INET and a.address \
                                and not a.address.startswith(('127.', '169.254.')):
                            ips.append(a.address)
            except Exception as e:
                logging.debug(f"[multinic] 枚举网卡失败，回退单 IP: {e}")
        seen, out = set(), []          # 去重保序
        for ip in ips:
            if ip not in seen:
                seen.add(ip); out.append(ip)
        cls._nic_ips_cache = out
        return out

    @classmethod
    # def execute_task(cls, algorithm_name, param_dict, data_dict):
    #     result = ActuatorImpl.run_actuator(algorithm_name, param_dict, data_dict)
    #     return result
    def execute_task(cls, algorithm_name, param_dict, data_dict):
        try:
            # 从 algorithm_name 中提取纯算法名
            if '.' in algorithm_name:
                pure_algorithm_name = algorithm_name.split('.')[-1]
            else:
                pure_algorithm_name = algorithm_name

            # 从 param_dict 读取容器资源配置（来自 DAG 图）
            container_config = param_dict.get('containerConfig', {})
            cpu_cores = container_config.get('cpu_cores', 2.0)  # 默认2核
            memory_mb = container_config.get('memory_mb', 2048)  # 默认2GB

            logging.info(f"使用容器执行任务: {pure_algorithm_name}, CPU={cpu_cores}核, 内存={memory_mb}MB")
            
            # 创建新容器执行任务（等待结果）
            result_dict = cls.container_controller.start_task(
                task_name=pure_algorithm_name,
                cpu_cores=cpu_cores,
                memory_mb=memory_mb,
                param_dict=param_dict,  
                data_dict=data_dict      
            )
            
            # 提取执行结果
            if result_dict.get('result', {}).get('status') == 1:
                # 任务执行成功
                task_result = result_dict['result'].get('taskResultInfo', '任务执行完成')
                logging.info(f"容器任务执行成功: {task_result}")
                result = task_result
            else:
                # 容器执行失败（如本机无 Docker / 镜像不可用）——按设计降级为本地直接执行。
                # 注意：start_task 会吞掉自身异常并返回 status!=1，因此这里不能只靠 except 兜底，
                # 必须在 status!=1 时显式降级，否则无 Docker 时每个任务都会返回失败串。
                error_msg = result_dict.get('result', {}).get('taskResultInfo', '任务执行失败')
                logging.warning(f"容器任务执行失败({error_msg})，降级使用本地直接执行")
                result = ActuatorImpl.run_actuator(algorithm_name, param_dict, data_dict)

            return result
            
        except Exception as e:
            # 容器执行异常，降级使用本地直接执行
            logging.error(f"容器执行任务异常: {e}")
            logging.warning("降级使用本地直接执行方式")
            result = ActuatorImpl.run_actuator(algorithm_name, param_dict, data_dict)
            return result

    @classmethod
    def process_task(cls, sock, data_decode):
        """
        在独立线程中处理单个任务
        """
        try:
            with cls.running_lock:
                cls.running_tasks += 1
            msg = data_decode['msg']
            task_id = msg['task_id']
            node_id = msg['sub_task_id']
            algorithm_name = "device.implements." + msg['algo_name']
            param_dict = dict(msg.get('param_dict') or {})

            # ---- 数据面：拉取前驱产物，注入 _artifact_in（pred_id -> 本地文件） ----
            artifact_in = {}
            remote = []   # 需经网络拉取的前驱: (pred_id, art, local)
            for art in (msg.get('input_artifacts') or []):
                pred_id = art.get('pred_id')
                # locality 优化：若该前驱产物就在本机（同机放置），直接本地读，跳过 QUIC 传输。
                here = cls.artifact_server.local_path(art['artifact_id']) if cls.artifact_server else None
                if here:
                    artifact_in[pred_id] = here
                    logging.info(f"[dataplane] 前驱产物 {art['artifact_id']} 在本机，直接本地读取（跳过传输）")
                    continue
                local = os.path.join(ARTIFACTS_DIR, f"{task_id}_{pred_id}.in.blob")
                remote.append((pred_id, art, local))

            # 多前驱并发拉取：不同源各走自身链路并行，同源复用长连接多 stream。
            # rate_mbps 由 server 在 input_artifacts 里给出（按任务分配带宽，T5.3）；None=不限。
            def _pull(item):
                _pid, _art, _local = item
                _srcs = _art.get('sources')   # 多链路源列表（dict: {host,port,origin_ip?,origin_port?}）；含 size 才启用
                if _srcs and _art.get('size'):
                    # 直接把 dict 源传给 fetch_striped（其 worker 会规范化 host/port/origin）；
                    # 切勿转成 (host,port) 元组——那样会丢掉中继的 origin（回源地址）。
                    _stats = fetch_striped(_srcs, _art['artifact_id'], _local, int(_art['size']),
                                           rate_mbps=_art.get('rate_mbps'))
                else:
                    _stats = fetch_blob(_art['src_ip'], int(_art['src_port']), _art['artifact_id'], _local,
                                        rate_mbps=_art.get('rate_mbps'))
                return _pid, _local, _stats

            if remote:
                with ThreadPoolExecutor(max_workers=min(8, len(remote))) as _ex:
                    _futs = {_ex.submit(_pull, it): it for it in remote}
                    for _f in as_completed(_futs):
                        _pid, _art, _local = _futs[_f]
                        try:
                            _pid, _local, _stats = _f.result()
                            artifact_in[_pid] = _local
                            cls._record_link(_art['src_ip'], _art['src_port'], _stats)
                            logging.info(f"[dataplane] task={task_id} sub={node_id} 拉取前驱{_pid}产物 "
                                         f"{_art['artifact_id']} 自 {_art['src_ip']}:{_art['src_port']} "
                                         f"({_stats['bytes']} bytes, {_stats['elapsed']:.3f}s)")
                        except Exception as e:
                            logging.error(f"[dataplane] task={task_id} sub={node_id} 拉取前驱{_pid}产物失败 {_art}: {e}")
            if artifact_in:
                param_dict['_artifact_in'] = artifact_in

            # ---- 数据面：本节点产出路径，注入 _artifact_out ----
            artifact_out = os.path.join(ARTIFACTS_DIR, f"{task_id}_{node_id}.blob")
            param_dict['_artifact_out'] = artifact_out

            # ---- T3.2-B 每任务资源档位：核数/内存 优先用大脑下发的 _alloc(server 已按声明/历史/可用算好)，
            #      否则回退到节点声明，再否则默认(总核/并行槽 ≈ 旧 worker 槽大小，保持默认行为不变) ----
            srv_alloc = param_dict.get('_alloc') or {}
            default_cores = max(1, (os.cpu_count() or 1) // max(1, cls.worker_slots))
            req_cores = int(srv_alloc.get('cores') or param_dict.get('cores') or default_cores)
            # B4：内存按「任务边际预算」(mem_budget_mb，声明/历史自动)；worker 端上限=基线+预算。None=不限。
            req_budget = srv_alloc.get('mem_budget_mb', param_dict.get('mem_mb'))
            # 记账用边际占用：预算 > 历史边际预估 > 0。用于 committed_mem 上报(T1.1)，不做硬限。
            acct_mem = int(req_budget or srv_alloc.get('est_mem_mb') or 0)
            leased_cores = cls.core_alloc.lease(req_cores) if cls.core_alloc else None
            if leased_cores:
                param_dict['_alloc'] = {'core_ids': leased_cores, 'mem_budget_mb': req_budget}
            with cls.running_lock:
                cls.committed_mem_mb += acct_mem

            # 执行算法：优先交给进程池的 worker（控并发+真并行+按档位绑核/限内存，T4.2/T3.2-B）；
            # 池不可用或执行异常则回退到主进程内执行（execute_task），保证仍能正常出结果。
            data_dict = msg.get('data_dict', {})
            metrics = {}
            proc_t0 = time.time()
            try:
                if cls.pool is not None:
                    try:
                        _out = cls.pool.submit(run_task, algorithm_name, param_dict, data_dict).result(timeout=300)
                        result_1, metrics = _out if isinstance(_out, tuple) else (_out, {})
                    except BrokenProcessPool as e:
                        # 进程池损坏(基础设施故障，罕见)：禁用进程池并降级为主进程内执行。
                        # 注意：降级后【不再施加每任务核/内存限额】——这里高声告警、并禁用池子(避免后续任务
                        # 反复撞坏池)，让“限额失效”可见而非静默。(运行时重建池在 Windows 上会与 IOCP 循环
                        # 并发 spawn 卡死，故不自动重建；需重启设备恢复限额。)
                        logging.warning(f"[pool] 进程池损坏，禁用并降级为主进程内执行"
                                        f"（本任务及后续不再受核/内存限额，需重启设备恢复）: {e}")
                        cls.pool = None
                        cls.worker_slots = 1
                        result_1 = cls.execute_task(algorithm_name, param_dict, data_dict)
                    except Exception as e:
                        # 任务自身异常（超内存 MemoryError、算法报错、超时）：按失败返回，不绕过资源限制。
                        logging.error(f"[pool] 任务执行异常: {type(e).__name__}: {e}")
                        result_1 = f"ERROR: {type(e).__name__}: {e}"
                else:
                    result_1 = cls.execute_task(algorithm_name, param_dict, data_dict)
            finally:
                # 归还核与内存档位
                if leased_cores and cls.core_alloc:
                    cls.core_alloc.release(leased_cores)
                with cls.running_lock:
                    cls.committed_mem_mb = max(0, cls.committed_mem_mb - acct_mem)
            metrics['proc_secs'] = round(time.time() - proc_t0, 2)   # B1 历史画像：本任务执行耗时

            # ---- 数据面：登记本节点产出，供后继 device 拉取 ----
            out_bytes = 0
            if cls.artifact_server is not None and os.path.exists(artifact_out):
                cls.artifact_server.register(f"{task_id}:{node_id}", artifact_out)
                try:
                    out_bytes = os.path.getsize(artifact_out)   # 上报产出大小，供 server 自适应放置(T3.2)
                except OSError:
                    out_bytes = 0
            if out_bytes:
                metrics['task_storage_mb'] = round(out_bytes / (1024 * 1024), 2)   # 产出存储 -> 历史画像(自动定档)

            _res_str = str(result_1)
            if _res_str.startswith('ERROR') or _res_str.startswith('MISSING'):
                logging.warning(f"[task] task={task_id} sub={node_id} algo={msg['algo_name']} "
                                f"dev={get_gl_device_no()} 失败: {_res_str[:300]}")
            else:
                logging.info(f"[task] task={task_id} sub={node_id} algo={msg['algo_name']} 完成: {_res_str[:200]}")

            # 构建响应消息
            msg_dict = {'task_id': data_decode['msg']['task_id'],
                        'sub_task_id': data_decode['msg']['sub_task_id'],
                        'device_no': data_decode['msg']['device_no'],
                        'finished_time': "",
                        'output_bytes': out_bytes,
                        'metrics': metrics,            # B1 历史画像：{peak_mem_mb, proc_secs}
                        'result': result_1}
            rsp_dict = {
                'dag_subtask_rsp': 'dag_subtask_rsp',
                'from_device_no': data_decode['to_device_no'],
                'from_device_ip': data_decode['to_device_ip'],
                'from_device_port': data_decode['to_device_port'],
                'to_device_no': data_decode['from_device_no'],
                'to_device_ip': data_decode['from_device_ip'],
                'to_device_port': data_decode['from_device_port'],
                'msg': msg_dict
            }

            # 发送响应
            # 注意: socket发送应该是线程安全的，但如果是高并发场景可能需要锁，
            # 这里简单起见直接发送。
            try:
                # 再次检查 socket 是否有效
                if sock.fileno() != -1:
                    NetworkUtils.send_msg(sock, rsp_dict)
                    logging.info(f"Sent task result back to server: {rsp_dict}")
            except (socket.error, OSError) as e:
                logging.warning(f"Socket error during task response sending: {e}")

        except Exception as e:
            logging.error(f"Error processing task: {e}")
        finally:
            with cls.running_lock:
                cls.running_tasks = max(0, cls.running_tasks - 1)

    @classmethod
    def _record_link(cls, src_ip, src_port, stats):
        """被动链路遥测：用一次真实产物拉取的 字节数/耗时 估算到源设备的有效带宽(MB/s)。

        小数据(如 256B 特征)的耗时主要是连接建立/时延，所以这里同时记录 last_ms 作为时延参考；
        EWMA 平滑多次采样。仅做记录，不影响任务执行。
        """
        try:
            elapsed = float(stats.get('elapsed') or 0.0)
            nbytes = int(stats.get('bytes') or 0)
            if elapsed <= 0:
                return
            mbps = (nbytes / elapsed) / (1024 * 1024)
            key = f"{src_ip}:{int(src_port)}"
            with cls.links_lock:
                prev = cls.observed_links.get(key, {})
                a = 0.5  # EWMA 权重
                mbps_s = mbps if 'mbps' not in prev else a * mbps + (1 - a) * prev['mbps']
                ms_s = elapsed * 1000 if 'last_ms' not in prev else a * elapsed * 1000 + (1 - a) * prev['last_ms']
                cls.observed_links[key] = {'mbps': round(mbps_s, 3), 'last_ms': round(ms_s, 2),
                                           'bytes': nbytes, 'samples': prev.get('samples', 0) + 1}
        except Exception as e:
            logging.debug(f"[link] 链路遥测打点失败(忽略): {e}")

    @classmethod
    def _probe_loop(cls):
        """T1.2 主动链路探测：周期性从每个在线对端拉取探测块，测真实带宽/时延，写入
        observed_links。这样即使两台设备之间还没发生过真实产物传输，放置决策也有「先验」
        链路信息可用（精准放置）。被动遥测与主动探测共用同一 observed_links（EWMA 融合）。
        OPENSAI_PROBE_INTERVAL<=0 时关闭主动探测（实验里需要干净测量时用）。"""
        if cls.PROBE_INTERVAL <= 0:
            logging.info("[probe] 主动探测已关闭 (OPENSAI_PROBE_INTERVAL<=0)")
            return
        recv_path = os.path.join(ARTIFACTS_DIR, '__probe_recv__.blob')
        time.sleep(min(8, cls.PROBE_INTERVAL))   # 先等首个心跳 ACK 把对端清单填好
        while True:
            for peer in list(cls.peers):
                ip, port = peer.get('ip_address'), peer.get('data_port')
                if not ip or port is None:
                    continue
                try:
                    stats = fetch_blob(ip, int(port), cls.PROBE_ID, recv_path, timeout=30)
                    cls._record_link(ip, port, stats)
                    logging.info(f"[probe] 主动探测 {ip}:{port} "
                                 f"({stats['bytes']} bytes, {stats['elapsed']:.3f}s)")
                except Exception as e:
                    logging.warning(f"[probe] 探测对端 {ip}:{port} 失败: {e}")
            time.sleep(cls.PROBE_INTERVAL)

    @classmethod
    def receive_msg(cls, sock, addr):
        '''
        接受来自server分发的任务，并调用相关函数进行处理
        '''
        # 发送设备初始化消息，格式化为JSON以便服务器识别
        init_msg = {'heart_beat': True, 'device_name': name, 'device_no': get_gl_device_no(),
                    'data_port': DATA_PORT}
        NetworkUtils.send_msg(sock, init_msg)
        logging.info(f"Sent device initialization message to {addr}")

        buf = bytearray()
        while True:
            try:
                data_decode = NetworkUtils.recv_msg(sock, buf)  # NDJSON 分帧读取一条消息
            except (socket.error, OSError) as e:
                # 捕获 socket 相关异常，如 socket 被关闭等
                logging.warning(f"[EXIT] The receive_msg thread is over, Socket error occurred: {e}")
                break
            except ValueError as e:
                logging.warning(f"跳过无法解析的控制帧: {e}")
                continue
            if data_decode is None:  # 连接已断开
                logging.warning("Connection closed by server")
                break

            # ---------------处理对应的dag任务-------------
            if data_decode is not None:
                if NetworkUtils.is_dict(data_decode) and 'udp_p2p_msg' in data_decode:
                    logging.info(f"Received P2P message: {data_decode}")
                    AlgorithmList = ActuatorImpl.read_algorithms()
                    if data_decode['msg']['algo_name'] in AlgorithmList:
                        # 开启新线程并行处理任务
                        task_thread = threading.Thread(
                            target=cls.process_task, 
                            args=(sock, data_decode),
                            daemon=True
                        )
                        task_thread.start()
                        logging.info(f"Started new thread to process task: {data_decode['msg']['algo_name']}")

                    else:
                        logging.error(f"[ERR] The receive_msg algorithm is not exist. {data_decode['msg']['algo_name']}")
                elif NetworkUtils.is_dict(data_decode) and 'artifact_cleanup' in data_decode:
                    # 7a：整图完成，server 通知删除该任务在本机的产物（输出 + 拉取的输入）
                    _tid = data_decode.get('task_id')
                    if _tid and cls.artifact_server is not None:
                        try:
                            n = cls.artifact_server.cleanup_task(_tid)
                            logging.info(f"[dataplane] 收到清理指令 task={_tid}: 删除 {n} 个产物")
                        except Exception as e:
                            logging.debug(f"[dataplane] 清理 task={_tid} 忽略: {e}")
                elif NetworkUtils.is_dict(data_decode) and 'SERVER RESPONSE IP' in data_decode:
                    # 服务器响应心跳：收到 ACK 的时刻 - 最近一次心跳发送时刻 = 真实 RTT
                    if cls.last_hb_sent:
                        cls.last_rtt_ms = (time.time() - cls.last_hb_sent) * 1000.0
                    # 记录在线对端清单，供主动探测线程使用
                    if isinstance(data_decode.get('peers'), list):
                        cls.peers = data_decode['peers']
                    logging.info(f"Received server heartbeat response: {data_decode}")
                else:
                    logging.info(f"Received other message: {data_decode}")

    @classmethod
    def send(cls, sock, addr):
        '''
          发送数据的方法
          参数：
            sock：定义一个实例化socket对象
            server：传递的服务器IP和端口
        '''
        while True:
            string = input('')
            message = name + ' : ' + string
            data = message.encode('utf-8')
            sock.send(data)  # TCP: sendto -> send
            logging.info(f"send msg {string} to {addr}")
            if string.lower() == 'EXIT'.lower():
                sock.close()
                break
        logging.info(f"[EXIT] The sending thread is over")

    @classmethod
    def send_heartbeat(cls, sock, addr):
        while True:
            # 每个心跳都重新采样物理信息，保证 CPU/内存是实时值（get_physical_device 内部
            # cpu_percent(interval=1) 会阻塞约 1s，相对 10s 心跳间隔可接受）。
            device_info = DeviceMonitor.get_physical_device()
            device_info['heart_beat'] = True
            device_info['device_no'] = get_gl_device_no()
            device_info['data_port'] = DATA_PORT  # 数据面端口，供 server 告知后继“去哪拉产物”
            device_info['nic_ips'] = cls._local_nic_ips()  # P0-1c 多网卡：本机其余可用 NIC，供大脑多链路并行
            device_info.update(cls.calculate_network_metrics(sock, addr))  # throughput/bandwidth/network_usage 仍为占位
            # —— 真实监测信号（覆盖占位值）——
            device_info['work_status'] = cls.running_tasks            # 正在执行的任务数
            device_info['worker_slots'] = cls.worker_slots            # 并行容量（capacity-aware 放置用）
            # T1.1 已承诺/可用算力：核(总/已承诺) + 已承诺内存（可用 = 总 − 已承诺，由读取方算）
            device_info['cores_total'] = os.cpu_count() or 1
            device_info['cores_committed'] = cls.core_alloc.committed() if cls.core_alloc else 0
            device_info['mem_committed_mb'] = cls.committed_mem_mb
            try:                                                     # 存储维度：上报产物目录所在盘 总量/空闲(MB)
                import shutil
                _du = shutil.disk_usage(ARTIFACTS_DIR)
                device_info['storage_total_mb'] = round(_du.total / (1024 * 1024))
                device_info['storage_free_mb'] = round(_du.free / (1024 * 1024))
            except Exception as e:
                logging.debug(f"[storage] 采集磁盘用量失败: {e}")
            device_info['delay'] = round(cls.last_rtt_ms, 2)          # 服务器↔设备 RTT(ms)
            with cls.links_lock:
                device_info['observed_links'] = dict(cls.observed_links)  # 被动链路遥测
            message = json.dumps(device_info)
            try:
                cls.last_hb_sent = time.time()
                NetworkUtils.send_msg(sock, device_info)
                logging.info(f"Sent heartbeat to server: {message}")
            except (socket.error, OSError) as e:
                # 捕获 socket 相关异常，如 socket 被关闭等
                logging.info(f"[EXIT] The heartbeat thread is over, Socket error occurred: {e}")
                break
            time.sleep(10)

    @classmethod
    def calculate_network_metrics(cls, sock, addr):
        # 发送和接收1MB数据包以计算网络吞吐量
        # data = os.urandom(1024 * 1024)
        # start_time = time.time()
        # sock.sendto(data, addr)
        # received_data = sock.recv(1024)
        # end_time = time.time()

        # 计算网络吞吐量（每秒传输的数据量）
        # throughput = len(data) / (end_time - start_time)
        throughput = 0

        # 计算带宽（每秒传输的比特数）
        # bandwidth = throughput * 8 * (10 ** 9)
        bandwidth = 0

        # 计算网络使用率（传输的数据量与可用带宽的比率）
        # network_usage = len(data) / (bandwidth * 0.01)
        network_usage = 0

        # 计算延迟（从发送数据包到接收到响应所需的时间）
        # delay = end_time - start_time
        delay = 0

        # 返回网络吞吐量、带宽、网络使用率和延迟
        return {'throughput': throughput, 'bandwidth': bandwidth, 'network_usage': network_usage, 'delay': delay}

    @classmethod
    def activate_client(cls):
        '''
          主函数执行方法，通过多线程来实现多个客户端之间的通信
        '''
        # 主线程预加载算法模块：避免子线程首次 import 重型 C 扩展(numpy/torch)在 Windows 上死锁。
        # （也是进程池不可用时、回退主进程内执行的安全垫。）
        ActuatorImpl.preload_algorithms()

        # T4.2：同步构建算法执行进程池（控并发+复用+真并行+绑核限算力）。
        # 必须在 asyncio 数据面循环/各线程启动【之前】，从干净的单线程状态 spawn worker——
        # 否则在 Windows 上与运行中的 IOCP 事件循环并发 spawn 会卡死。build_pool 内含预热超时自检，
        # 失败/超时则返回 None，任务回退主进程内执行，照常出结果。
        cls.pool, cls._pool_mgr, _slices = build_pool()
        # 并行容量：池可用时 = worker 数；否则 1（回退主进程内串行执行）。供放置 capacity-aware。
        cls.worker_slots = len(_slices) if (cls.pool and _slices) else 1
        # T3.2-B：CPU 核租借器（按本机总核数）。每任务运行时租 K 核、跑完归还。
        cls.core_alloc = CoreAllocator(os.cpu_count() or 1)

        # 启动数据面产物服务器（设备间大块数据直传 / QUIC）
        os.makedirs(ARTIFACTS_DIR, exist_ok=True)
        cls.artifact_server = ArtifactServer(port=DATA_PORT)
        cls.artifact_server.start()
        logging.info(f"数据面 ArtifactServer 已启动: 端口 {DATA_PORT}, 产物目录 {ARTIFACTS_DIR}")
        # 7b：产物 GC 兜底——删超 TTL 的陈旧产物 + 超水位按 LRU 回收（补 7a 在任务异常未收尾/历史遗留时的缺口）。
        cls.artifact_server.start_gc(
            ttl_s=float(os.environ.get('OPENSAI_ARTIFACT_TTL_S', '3600') or 0),      # 默认 1h
            max_mb=float(os.environ.get('OPENSAI_ARTIFACT_MAX_MB', '0') or 0),       # 默认 0=不限(opt-in 水位)
            interval_s=float(os.environ.get('OPENSAI_ARTIFACT_GC_INTERVAL_S', '600') or 600))  # 默认 10min

        # T1.2 主动探测：提供一个固定大小的探测块，供其它设备测量到本机的链路带宽/时延。
        probe_path = os.path.join(ARTIFACTS_DIR, '__probe__.blob')
        with open(probe_path, 'wb') as f:
            f.write(b'\0' * (4 * 1024 * 1024))   # 4MB，足够压过建连开销、测得有意义的带宽
        cls.artifact_server.register(cls.PROBE_ID, probe_path)
        threading.Thread(target=cls._probe_loop, daemon=True).start()

        # 创建TCP socket而不是使用CLIENT_SOCK
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # TCP
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)  # 保持与原代码一致的缓冲区设置
        server = (HOST, PORT)

        # TCP需要先连接到服务器
        try:
            sock.connect(server)
            logging.info(f"Connected to server at {server}")
        except Exception as e:
            logging.error(f"Failed to connect to server: {e}")
            return

        logging.info("-----欢迎使用OpenSAI Offloading System,退出连接请输入'EXIT(不分大小写)'-----")
        receive_msg_thread = threading.Thread(target=cls.receive_msg, args=(sock, server), daemon=True)
        receive_msg_thread.start()

        ts = threading.Thread(target=cls.send, args=(sock, server))
        ts.start()

        heartbeat_thread = threading.Thread(target=cls.send_heartbeat, args=(sock, server))
        heartbeat_thread.start()

        # 不阻塞主线程，让三个线程在后台运行
        # receive_msg_thread.join()
        ts.join()
        heartbeat_thread.join()
        sock.close()


if __name__ == '__main__':
    DeviceClient.activate_client()