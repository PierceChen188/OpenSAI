import json
import logging
import socket
import time
import queue
import datetime
import threading
import sys
import os
from collections import deque

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 将上一级目录添加到Python的模块搜索路径
from config.logging_config import LOG_FORMAT
from config.network_config import HOST, PORT
from config.sub_task_status import SubTaskStatus
from device.bean.devce_info import DeviceInfo
from network.utils.network_utils import NetworkUtils
from network.protocol.packet_protocol_conversion import PacketProtocolConversion
from predis.implements.device_service_impl import DeviceServiceImpl
from device.implements.idle_device_impl import IdleDeviceImpl
from device.implements.actuator_impl import ActuatorImpl
from task.bean.sub_task_info import SubTaskInfo
from offloading_brain.offloading_brain import OffloadingImpl
from predis.utils.redis_utils import RedisUtils


class UserServer:
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    all_task_dict = {
        # 'task1': {"time": timestamp, "queue": []},
        # 可以根据需要添加更多任务队列
    }

    all_task_decoded = {
        # 'task1': decoded_info
    }

    ready_subtask_queue = queue.Queue()
    data_lock = threading.Lock()

    # TCP客户端连接列表 - 用于存储TCP连接
    client_sockets = {}

    # T2.1 预留账本：device_no -> 已放置但尚未完成的子任务数。放置即 +1、完成即 -1。
    # 让放置决策按「真实负载 + 已承诺」选设备，避免把任务都堆到同一台“看起来空闲”的机器（防超卖）。
    # 由 data_lock 保护（offloading_strategy 与 dag_subtask_rsp 处理均在该锁下）。
    reservations = {}
    # T3.2 自适应放置用：(task_id, node_id) -> 该子任务实测产出字节数（设备在 rsp 中上报）。
    # 放置下游时按前驱真实数据量估传输代价，从而按数据量自动权衡就近↔分散。
    output_sizes = {}
    # 前瞻放置(整图反向就近规划)用：task_id -> {node_id: device_no}，提交后首次放置时整图规划一次并缓存。
    placement_plan = {}
    # T3.2 资源档位(阶段A·带宽)：committed_bw[device_no] = 该设备当前已承诺的出口带宽(MB/s)；
    # 分配一条传输即在源设备上扣减预算、完成时归还 -> "可用 = 预算 - 已承诺"（补 T1.1 的带宽侧）。
    committed_bw = {}
    bw_reservations = {}   # (task_id, consumer_node_id) -> [(src_device_no, rate_mbps), ...] 供完成时归还
    # 资源档位(存储维度)：committed_storage_mb[device_no] = 该设备当前已承诺的产物存储(MB)；
    # 派发任务时在【执行设备】上扣减、完成/超时时归还 -> "可用 = 设备空闲磁盘 - 已承诺"，避免把多个大产出
    # 任务堆到磁盘将满的设备(防超卖)。硬实施由设备侧 7b GC 水位兜底；本维度做"档位决策 + 容量收口 + 记账"。
    committed_storage_mb = {}
    storage_reservations = {}   # (task_id, node_id) -> (device_no, storage_mb) 供完成时归还
    # C1 预留泄漏兜底：(task_id, node_id) -> (device_no, 放置时间戳)。完成时移除；超时(任务似已死)由
    # 卸载线程定期回收其放置预留 + 带宽预留，防止设备崩溃后账本永久虚高。
    inflight = {}
    # B1 历史画像：algo_name -> {peak_mem_mb, task_mem_mb, proc_secs, n}（设备 rsp 上报 metrics 后 EWMA）。
    # task_mem_mb=扣掉 worker 基线的「任务边际内存」；供 B4 在开 OPENSAI_AUTO_MEM 时自动定内存档(基线+边际×系数)。
    algo_history = {}
    # 决策事件流(§14.2)：放置/资源/传输三类结构化事件的环形缓冲，供前端轮询展示"决策过程"。
    # 只在内存里、有界(防膨胀)；读写用 events_lock 保护(快照与追加可能跨线程)。
    decision_events = deque(maxlen=400)
    _event_seq = 0
    events_lock = threading.Lock()

    @classmethod
    def update_decoded_by_dict(cls, task_id: str):
        task_dict = cls.all_task_dict[task_id]
        task_encoded = cls.all_task_decoded[task_id]
        for node in task_encoded['dagData']['nodes']:
            node['result']['status'] = task_dict["queue"][node['id'] - 1].get_subtask_status()
            node['result']['delay'] = task_dict["queue"][node['id'] - 1].get_subtask_delay()
            node['result']['deviceId'] = task_dict["queue"][node['id'] - 1].get_subtask_device()

    @classmethod
    def _healthy_devices(cls, device_info_list):
        """6a 资源健康预筛：过滤掉过载设备(CPU/内存/队列超阈值，复用 IdleDeviceImpl 的阈值)，
        避免把任务派给濒临 OOM/打满的机器（代价模型的负载项只看"已预留数"，抓不住设备自身已很忙）。
        受 OPENSAI_HEALTH_FILTER 控制(默认开)。安全兜底：过滤后为空(全部过载)→退回全量，绝不因预筛阻塞放置。"""
        if not device_info_list:
            return device_info_list
        if os.environ.get('OPENSAI_HEALTH_FILTER', '1') in ('0', '', 'false', 'False'):
            return device_info_list
        healthy = {}
        for dno, d in device_info_list.items():
            try:
                if IdleDeviceImpl.idle_devices(d):
                    healthy[dno] = d
            except Exception:
                healthy[dno] = d        # 心跳字段不全 -> 不误杀，保留候选
        if not healthy:
            logging.warning('[health] 所有设备均过载，资源预筛退回全量(不阻塞放置)')
            return device_info_list
        if len(healthy) < len(device_info_list):
            logging.info(f'[health] 资源预筛: {len(healthy)}/{len(device_info_list)} 台健康设备进入放置')
        return healthy

    @classmethod
    def offloading_strategy(cls, subtask):
        '''
            抽象出一层内容,将具体的卸载决策放在./offloading_brain里
        '''
        logging.info('making offloading strategy for subtask')
        device_info_list = DeviceServiceImpl.read_device_info()
        # 6a 资源健康预筛：自动放置只在"未过载"设备中挑（手动指定 direct 不筛，尊重用户钉选）。
        healthy_list = cls._healthy_devices(device_info_list)
        # 默认 plan（前瞻整图规划）；设备掉线会自动回退贪心。smart=纯贪心自适应；random=对照。
        mode = os.environ.get('OPENSAI_PLACEMENT', 'plan').lower()
        try:                                  # 容错：directAllocationId 可能以字符串传入
            _dai = int(subtask.get_sub_task_direct_allocate_id())
        except (TypeError, ValueError):
            _dai = -1
        if _dai != -1:
            # 手动指定（directAllocationId>0）：保持原行为不变，用全量列表（不过滤）。
            device_dict = OffloadingImpl.direct_strategy(device_info_list, subtask)
        elif mode == 'random':
            # 对照实验用：强制随机放置（A/B 对比智能放置的收益）。
            device_dict = OffloadingImpl.random_strategy(healthy_list, subtask)
        elif mode == 'plan':
            # 前瞻放置：整图反向就近规划（提交后首次放置时规划一次并缓存）。
            task_id = subtask.get_task_id()
            plan = cls.placement_plan.get(task_id)
            if plan is None:
                decoded = cls.all_task_decoded.get(task_id)
                plan = OffloadingImpl.plan_dag(decoded, healthy_list, cls.reservations) if decoded else {}
                cls.placement_plan[task_id] = plan
            device_dict = healthy_list.get(plan.get(subtask.get_sub_task_id()))
            if not device_dict:
                # 规划的设备已下线/未知/被预筛 -> 退化为贪心自适应，保证仍能放下去。
                device_dict = OffloadingImpl.locality_strategy(
                    healthy_list, subtask, cls._predecessor_info(subtask), cls.reservations)
        else:
            # T3.1/T3.2 自适应放置：传入前驱(设备,实测产出大小) + 预留账本。
            pred_info = cls._predecessor_info(subtask)
            device_dict = OffloadingImpl.locality_strategy(
                healthy_list, subtask, pred_info, cls.reservations)
        if not device_dict:
            # 当前无在线设备：不分配，保持子任务 READY 等待下一轮重试。
            # （旧代码会直接 set_device_from_dict(None) 抛 TypeError，进而杀死整个卸载线程。）
            logging.warning('无可用在线设备，子任务暂缓卸载，等待下一轮重试...')
            return False
        device_info = DeviceInfo()
        device_info.set_device_from_dict(device_dict)
        subtask.set_device_info(device_info)
        # T2.1 占用即记账（放置成功 -> 该设备预留 +1），完成时在 dag_subtask_rsp 释放。
        dno = device_info.get_device_no()
        if dno:
            cls.reservations[dno] = cls.reservations.get(dno, 0) + 1
        # C1 记录在飞任务(设备+时间戳)，供超时回收。
        cls.inflight[(subtask.get_task_id(), subtask.get_sub_task_id())] = (dno, time.time())
        # §14.2 放置决策事件：候选集合 / 健康预筛淘汰 / 代价打分 / 选中 / 就近命中。
        try:
            pred_info = cls._predecessor_info(subtask)
            scored = (OffloadingImpl.score_candidates(healthy_list, subtask, pred_info, cls.reservations)
                      if mode in ('plan', 'smart') and _dai == -1 else [])
            _nm = lambda d: cls._dev_name(device_info_list, d)   # device_no -> 设备名(与前端设备 id 一致)
            filtered = [{'device': _nm(d), 'reason': '过载(健康预筛)'}
                        for d in device_info_list if d not in (healthy_list or {})]
            scored_named = [{'device': _nm(s['device']), 'xfer_s': s['xfer_s'],
                             'load': s['load'], 'cost': s['cost']} for s in scored]
            cls._emit_event('placement', {
                'task_id': subtask.get_task_id(), 'node_id': subtask.get_sub_task_id(),
                'algorithm': subtask.get_task_algorithm(), 'mode': mode,
                'chosen': _nm(dno),
                'candidates': [_nm(d) for d in sorted((healthy_list or {}).keys())],
                'filtered': filtered,
                'scores': scored_named,
                'locality_hit': any(src == dno for (src, _b) in pred_info),
            })
        except Exception as _e:
            logging.debug(f"[event] placement 忽略: {_e}")
        return True

    @classmethod
    def _reclaim_stale_reservations(cls):
        """C1：回收超时(疑似设备崩溃、永不上报完成)的在飞任务所占的放置预留与带宽预留。
        TTL 由 OPENSAI_RESERVATION_TTL 指定(默认 600s)。需在 data_lock 下调用。"""
        ttl = float(os.environ.get('OPENSAI_RESERVATION_TTL', '600') or 600)
        now = time.time()
        for key, (dev, ts) in list(cls.inflight.items()):
            if now - ts <= ttl:
                continue
            if dev and cls.reservations.get(dev):
                cls.reservations[dev] = max(0, cls.reservations[dev] - 1)
            for s, rt in cls.bw_reservations.pop(key, []):
                cls.committed_bw[s] = max(0.0, cls.committed_bw.get(s, 0.0) - rt)
            _sres = cls.storage_reservations.pop(key, None)
            if _sres:
                _sd, _smb = _sres
                cls.committed_storage_mb[_sd] = max(0.0, cls.committed_storage_mb.get(_sd, 0.0) - _smb)
            cls.inflight.pop(key, None)
            logging.warning(f"[reclaim] 子任务 {key} 超过 {ttl}s 未完成，回收其设备 {dev} 的预留")

    @classmethod
    def aggregate_monitor_view(cls):
        """T1.3 server 侧聚合监测视图：把【设备心跳(Redis)】与【大脑自有账本(本 server 进程内存)】
        合并成一份权威快照。关键：放置预留、已承诺出口带宽、在飞任务、算法历史画像都只在 server
        内存里(心跳/Redis 里没有)，所以只有 server 能给出"看资源 + 看决策账本"的完整视图。
        需在 data_lock 下调用(读 reservations/committed_bw/inflight/algo_history)。"""
        devices = DeviceServiceImpl.read_device_info() or {}
        view_devs = {}
        for dno, d in devices.items():
            ct = int(d.get('cores_total') or 0)
            cc = int(d.get('cores_committed') or 0)
            view_devs[dno] = {
                'used_cpu': d.get('used_cpu'),
                'idle_memory': d.get('idle_memory'), 'total_memory': d.get('total_memory'),
                'work_status': d.get('work_status', 0), 'worker_slots': d.get('worker_slots', 1),
                'cores_total': ct, 'cores_committed': cc, 'cores_available': max(0, ct - cc),
                'mem_committed_mb': d.get('mem_committed_mb', 0), 'delay': d.get('delay', 0),
                'data_port': d.get('data_port'), 'ip_address': d.get('ip_address'),
                'observed_links': d.get('observed_links') or {},
                'storage_total_mb': int(d.get('storage_total_mb') or 0),               # 设备磁盘(心跳)
                'storage_free_mb': int(d.get('storage_free_mb') or 0),
                # —— 大脑账本(只在 server 内存、Redis 心跳里没有) ——
                'placement_reserved': int(cls.reservations.get(dno, 0)),               # T2.1 放置预留
                'bw_committed_mbps': round(float(cls.committed_bw.get(dno, 0.0)), 2),   # T3.2-A 已承诺出口带宽
                'storage_committed_mb': round(float(cls.committed_storage_mb.get(dno, 0.0)), 0),  # 已承诺存储档位
            }
        return {
            'devices': view_devs,
            'inflight_tasks': len(cls.inflight),                # C1 在飞(已派发未完成)子任务数
            'algo_history': {a: {k: (round(v, 1) if isinstance(v, float) else v)
                                 for k, v in h.items()} for a, h in cls.algo_history.items()},  # B1 画像
            'ts': time.time(),
        }

    @classmethod
    def _artifact_rate_mbps(cls, sub_task_info, src_device_no, pred_node_id=None):
        """T3.2-A 带宽分配：给「从源设备 src_device_no 拉到本消费者」这条传输分配限速(MB/s)，None=不限。

        策略 = min(需求, 源设备出口预算 - 已承诺)，地板值 OPENSAI_MIN_BW_MBPS。
          - 需求：① 消费者节点 taskInputInfo 里声明的 `bw_mbps`；② 未声明且设了目标传输时延
            OPENSAI_TARGET_XFER_S 时，按【前驱实测产出字节数 / 目标时延】自动估算需求(A2，免手填)；
            ③ 都没有则不设需求上限。
          - 预算：OPENSAI_DEVICE_BW_MBPS（每设备出口，0=不限）。
        分配即在源设备上记账(committed_bw)，完成时在 dag_subtask_rsp 归还。同机传输(本地直读、不走网络)不分配。
        OPENSAI_FORCE_RATE_MBPS 若设置则强制该值（实验用）。"""
        forced = os.environ.get('OPENSAI_FORCE_RATE_MBPS')
        if forced:
            try:
                return float(forced)
            except ValueError:
                pass
        # 同机传输不经网络（设备会本地直读），不分配带宽
        try:
            consumer_dev = sub_task_info.get_device_info().get_device_no()
        except Exception:
            consumer_dev = None
        if not src_device_no or src_device_no == consumer_dev:
            return None
        # 任务声明的带宽需求（上限）
        demand = None
        try:
            d = (sub_task_info.get_sub_task_input_param() or {}).get('bw_mbps')
            demand = float(d) if d else None
        except Exception:
            demand = None
        # A2：未显式声明时，按前驱实测产出大小 / 目标传输时延 自动估算需求(MB/s)；免手填 bw_mbps。
        if demand is None and pred_node_id is not None:
            target_s = float(os.environ.get('OPENSAI_TARGET_XFER_S', '0') or 0)
            nbytes = cls.output_sizes.get((sub_task_info.get_task_id(), pred_node_id), 0)
            if target_s > 0 and nbytes > 0:
                demand = (nbytes / (1024 * 1024)) / target_s
                logging.info(f"[alloc] 带宽(A2自动): 前驱{pred_node_id} {nbytes}B / 目标{target_s}s "
                             f"-> 需求 {demand:.2f} MB/s")
        budget = float(os.environ.get('OPENSAI_DEVICE_BW_MBPS', '0') or 0)
        if budget <= 0:
            rate = demand                      # 无预算：仅受声明需求约束（没声明则不限）
        else:
            floor = float(os.environ.get('OPENSAI_MIN_BW_MBPS', '1') or 1)
            avail = max(floor, budget - cls.committed_bw.get(src_device_no, 0.0))
            rate = min(demand, avail) if demand else avail
        if rate and rate > 0:
            cls.committed_bw[src_device_no] = cls.committed_bw.get(src_device_no, 0.0) + rate
            key = (sub_task_info.get_task_id(), sub_task_info.get_sub_task_id())
            cls.bw_reservations.setdefault(key, []).append((src_device_no, rate))
            logging.info(f"[alloc] 带宽: 源 {src_device_no} 分配 {rate:.2f} MB/s "
                         f"(已承诺 {cls.committed_bw[src_device_no]:.2f}/{budget or '∞'} MB/s)")
        return rate

    @classmethod
    def _alloc_compute(cls, sub_task_info, dev_dict):
        """B1+B2 算力/内存档位决策：返回 {cores, mem_mb, est_mem_mb}。
          - cores：节点声明 `cores`，否则 OPENSAI_DEFAULT_CORES(>0 时)，否则 None=设备默认槽；按可用核收口(B2)。
          - mem_mb（硬上限）：仅当节点**显式声明** `mem_mb` 才设；按可用内存收口。
            注意：内存上限作用于整个 worker 进程(含已加载的 torch ~数百MB)，**自动从历史硬限会贴近基线、
            轻微波动即误杀**，故 v1 不自动硬限；历史画像(algo_history)改为提供 `est_mem_mb`(预估占用)，
            仅供账本/放置参考，不强制(B1 安全用法)。"""
        param = sub_task_info.get_sub_task_input_param() or {}
        algo = sub_task_info.get_task_algorithm()
        cores = param.get('cores')
        if cores is None:
            d = int(os.environ.get('OPENSAI_DEFAULT_CORES', '0') or 0)
            cores = d if d > 0 else None
        hist = cls.algo_history.get(algo) or {}
        marginal = hist.get('task_mem_mb')                  # 历史「任务边际内存」(扣掉 worker 基线)
        est_mem_mb = int(marginal) if marginal else None    # 记账预估(边际)
        # B4 内存预算(基线之上的“任务预算”，worker 端上限=基线+预算)：
        #   ① 节点显式声明 mem_mb(此处当作边际预算)；
        #   ② 否则开了 OPENSAI_AUTO_MEM 且有历史 -> 边际×2、地板 256MB 自动定档(安全:上限恒>基线，不误杀)。
        budget = param.get('mem_mb')
        if budget is None and os.environ.get('OPENSAI_AUTO_MEM') and marginal:
            budget = max(marginal * 2.0, 256.0)
        if dev_dict:                                        # B2：按设备可用收口
            avail_cores = max(1, int(dev_dict.get('cores_total') or os.cpu_count() or 1)
                              - int(dev_dict.get('cores_committed') or 0))
            if cores:
                cores = min(int(cores), avail_cores)
            idle_mb = float(dev_dict.get('idle_memory') or 0) * 1024
            if budget and idle_mb > 0:
                budget = min(float(budget), idle_mb * 0.9)
        # 存储档位：① 节点声明 `storage_mb`；② 否则开 OPENSAI_AUTO_STORAGE 且有历史 -> 历史产出×2、地板256MB。
        #   按【所选设备空闲磁盘 - 已承诺】收口，并在该设备上记账(完成/超时归还)。
        storage_budget = param.get('storage_mb')
        hist_storage = hist.get('task_storage_mb')
        if storage_budget is None and os.environ.get('OPENSAI_AUTO_STORAGE') and hist_storage:
            storage_budget = max(float(hist_storage) * 2.0, 256.0)
        dno_s = dev_dict.get('device_no') if dev_dict else None
        if dev_dict and storage_budget:
            free_mb = float(dev_dict.get('storage_free_mb') or 0)
            if free_mb > 0:
                avail = max(0.0, free_mb - cls.committed_storage_mb.get(dno_s, 0.0))
                storage_budget = min(float(storage_budget), avail * 0.9)
        if storage_budget and storage_budget > 0 and dno_s:
            cls.committed_storage_mb[dno_s] = cls.committed_storage_mb.get(dno_s, 0.0) + storage_budget
            cls.storage_reservations[(sub_task_info.get_task_id(), sub_task_info.get_sub_task_id())] = \
                (dno_s, storage_budget)
            logging.info(f"[alloc] 存储: 设备 {dno_s} 分配 {storage_budget:.0f} MB "
                         f"(已承诺 {cls.committed_storage_mb[dno_s]:.0f} MB)")
        _alloc = {'cores': cores, 'mem_budget_mb': (int(budget) if budget else None),
                  'est_mem_mb': est_mem_mb,
                  'storage_budget_mb': (int(storage_budget) if storage_budget else None)}
        # §14.2 资源决策事件：四维档位「声明 / 自动 / 最终」+ 设备已承诺存储。
        try:
            cls._emit_event('resource', {
                'task_id': sub_task_info.get_task_id(), 'node_id': sub_task_info.get_sub_task_id(),
                'algorithm': algo, 'device': ((dev_dict or {}).get('device_name') or dno_s),
                'cores': {'declared': param.get('cores'), 'final': cores},
                'mem_mb': {'declared': param.get('mem_mb'), 'final': _alloc['mem_budget_mb'],
                           'auto': bool(param.get('mem_mb') is None and os.environ.get('OPENSAI_AUTO_MEM') and marginal)},
                'storage_mb': {'declared': param.get('storage_mb'), 'final': _alloc['storage_budget_mb'],
                               'auto': bool(param.get('storage_mb') is None and os.environ.get('OPENSAI_AUTO_STORAGE') and hist_storage)},
                'committed_storage_mb': (round(cls.committed_storage_mb.get(dno_s, 0.0), 0) if dno_s else None),
            })
        except Exception as _e:
            logging.debug(f"[event] resource 忽略: {_e}")
        return _alloc

    @classmethod
    def _update_algo_history(cls, algo, metrics):
        """B1：用设备上报的 metrics(EWMA) 更新算法历史画像。"""
        if not algo or not isinstance(metrics, dict):
            return
        h = cls.algo_history.setdefault(algo, {'peak_mem_mb': 0.0, 'task_mem_mb': 0.0,
                                               'proc_secs': 0.0, 'task_storage_mb': 0.0, 'n': 0})
        a = 0.5
        for k in ('peak_mem_mb', 'task_mem_mb', 'proc_secs', 'task_storage_mb'):   # 增记产出存储(供自动定档)
            v = metrics.get(k)
            if v:
                h[k] = float(v) if h['n'] == 0 else a * float(v) + (1 - a) * h[k]
        h['n'] += 1

    @classmethod
    def _emit_event(cls, kind, payload):
        """追加一条决策事件(放置/资源/传输)到环形缓冲，供前端展示决策过程。容错：异常不影响主流程。"""
        try:
            with cls.events_lock:
                cls._event_seq += 1
                ev = {'seq': cls._event_seq, 'ts': round(time.time(), 3), 'kind': kind}
                ev.update(payload)
                cls.decision_events.append(ev)
        except Exception as e:
            logging.debug(f"[event] emit 忽略: {e}")

    @classmethod
    def _events_snapshot(cls):
        """返回决策事件快照(列表)，供桥接经 eventsInfo 回传前端。"""
        with cls.events_lock:
            return list(cls.decision_events)

    @classmethod
    def _dev_name(cls, device_info_list, dno):
        """device_no(UUID) -> 设备显示名(device_name)。前端设备列表用 device_name 作 id，
        故决策事件里的设备引用也要用 device_name，才能在三维窗口/决策页正确映射到设备。"""
        if not dno:
            return dno
        info = (device_info_list or {}).get(dno) or {}
        return info.get('device_name') or dno

    @classmethod
    def _predecessor_info(cls, sub_task_info):
        """返回该子任务各前驱的 (device_no, 实测产出字节数)。

        前驱用原始 DAG 的 beforeNodes；前驱产出在哪台机 = 该前驱子任务被放置到的设备
        （T2.2 数据位置追踪）；产出大小来自 output_sizes（设备在 rsp 中上报，T3.2）。
        前驱必已完成（下游依赖它才就绪），故大小可用；未知则记 0（不计传输代价）。"""
        task_id = sub_task_info.get_task_id()
        node_id = sub_task_info.get_sub_task_id()
        decoded = cls.all_task_decoded.get(task_id)
        info = []
        if not decoded:
            return info
        preds = []
        for node in decoded['dagData']['nodes']:
            if node['id'] == node_id:
                preds = node['form'].get('beforeNodes', []) or []
                break
        for p in preds:
            try:
                pred_dev = cls.all_task_dict[task_id]["queue"][p - 1].get_device_info()
                dno = pred_dev.get_device_no() if pred_dev else None
            except Exception:
                dno = None
            info.append((dno, cls.output_sizes.get((task_id, p), 0)))
        return info

    @classmethod
    #保存历史记录到redis中
    def save_history(cls):
        try:
            connected_device_info = DeviceServiceImpl.read_device_info()
            devices_data = []
            for device_id, device_info in connected_device_info.items():
                device = {
                    "deviceNo": device_info['device_name'],  # device_info['device_no'],
                    "usedCpu": device_info['used_cpu'],
                    "coreNum": device_info['core_num'],
                    "idleMemory": device_info['idle_memory'],
                    "usedMemory": device_info['used_memory'],
                    "totalMemory": device_info['total_memory'],
                    "workStatus": device_info['work_status'],
                    "lng": device_info['lng'],
                    "lat": device_info['lat'],
                    "orderValue": device_info['order_value'],
                    "ipAddress": device_info['ip_address'],
                    "throughput": device_info['throughput'],
                    "bandwidth": device_info['bandwidth'],
                    "networkUsage": device_info['network_usage'],
                    "delay": device_info['delay'],
                    "port": device_info['port']
                }
                devices_data.append(device)
            record = {
                "timestamp": time.time(),
                "algorithms": ActuatorImpl.read_algorithms(),
                "policies": ["default"],
                "devices": devices_data
            }
            RedisUtils().lset_multiple(
                key="History",
                values=[json.dumps(record)]  # 序列化为JSON字符串
            )
            time.sleep(5)
        except Exception as e:
            print(f"保存历史记录失败: {str(e)}")

    @classmethod
    def get_history(cls):
    #从redis中读取历史记录
        try:
            # 获取全部记录（0到-1表示所有元素）
            raw_data = RedisUtils().lget("History", 0, -1)

            # 反序列化处理
            records = []
            for item in raw_data:
                try:
                    records.append(json.loads(item.decode('utf-8')))
                except Exception as e:
                    print(f"记录解析失败: {str(e)}")

            # 按时间倒序排列（最后插入的在前）
            print(records)
            return sorted(records,
                          key=lambda x: x["timestamp"],
                          reverse=True)
        except Exception as e:
            print(f"读取历史记录失败: {str(e)}")

    @classmethod
    # 子线程1：遍历 all_task_dict 中的所有子任务队列，将就绪子任务放入 ready_subtask_queue
    def offloading_making_process(cls):
        logging.info('The thread for offloading making process start...')
        _last_reclaim = [time.time()]   # C1：上次回收时间（列表以便闭包内可改）
        _last_view = [0.0]              # T1.3：上次发布聚合监测视图的时间
        while True:

            try:
                # C1：每 ~30s 回收一次超时未完成任务的预留（防设备崩溃后账本永久虚高）。
                if time.time() - _last_reclaim[0] > 30:
                    with cls.data_lock:
                        cls._reclaim_stale_reservations()
                    _last_reclaim[0] = time.time()
                # T1.3：每 ~5s 发布一次 server 侧聚合监测视图(设备心跳 + 大脑账本)到 Redis，
                # 供 show_monitor 展示完整画面(含预留/已承诺带宽/在飞/画像)；TTL 30s，server 挂了自动过期。
                if time.time() - _last_view[0] > 5:
                    try:
                        with cls.data_lock:
                            _view = cls.aggregate_monitor_view()
                        RedisUtils().set_key_value_with_ttl('OPENSAI_MONITOR_VIEW', json.dumps(_view), 30)
                    except Exception as _e:
                        logging.debug(f"[monitor] 发布聚合视图失败: {_e}")
                    _last_view[0] = time.time()
                for task_name, task_info in cls.all_task_dict.items():
                    # print(task_name, task_info)
                    for subtask in task_info["queue"]:
                        if subtask.get_sub_task_status() == SubTaskStatus.READY.value:
                            with cls.data_lock:
                                # 对subtask做卸载决策(目前启用随机卸载决策）
                                # 仅在成功分配到设备后才入队并置为 WORK；否则保持 READY 下轮重试
                                if cls.offloading_strategy(subtask):
                                    cls.ready_subtask_queue.put(subtask)
                                    subtask.set_sub_task_status(SubTaskStatus.WORK.value)
            except Exception as e:
                # 防御性兜底：单个子任务/单轮的异常不应杀死整个卸载线程
                logging.error(f"卸载决策线程异常（已忽略并继续）: {e}")
            time.sleep(0.01)  # 指定间隔检查一次

    @classmethod
    def _multinic_sources(cls, src_ip, src_port, dev, size_bytes):
        """P0-1c 多网卡聚合：源设备若上报了多张可用 NIC（nic_ips）且产物够大，返回该设备的多个
        (host,port) 直连源（主 IP + 其余 NIC，去重保序）—— 消费者会对不同目的 IP 建独立连接，
        由路由表把各连接从匹配网卡发出，带宽叠加。否则返回 [主直连]。受 OPENSAI_MULTINIC 控制。
        默认关闭 → 单网卡/同构环境只回 1 个源，不影响现有直连路径与回归。"""
        primary = {'host': src_ip, 'port': src_port}
        if os.environ.get('OPENSAI_MULTINIC', '0') in ('0', '', 'false', 'False'):
            return [primary]
        min_mb = float(os.environ.get('OPENSAI_MULTINIC_MIN_MB', '8') or 8)
        if not size_bytes or size_bytes < min_mb * 1024 * 1024:
            return [primary]
        hosts, seen = [src_ip], {src_ip}
        for ip in (dev.get('nic_ips') or []):
            if ip and ip not in seen:
                seen.add(ip); hosts.append(ip)
        return [{'host': h, 'port': src_port} for h in hosts]

    @classmethod
    def _pick_relay(cls, consumer_dno, src_dno, src_ip, src_port, size_bytes, device_info_list):
        """P0-1b 多链路中继选择：仅当 OPENSAI_MULTIPATH 开启、产物够大、且存在某中继 R 使
        min(src→R, R→consumer) 明显快于直连 consumer→src 时，返回 (relay_ip, relay_port)，否则 None。
        带宽取自各设备上报的 observed_links（key='ip:dataport'，单位 MB/s），未知用默认兜底。
        默认关闭 + 阈值保守 → 同构/局域网下不会误触发，不影响现有直连路径与回归。"""
        if os.environ.get('OPENSAI_MULTIPATH', '0') in ('0', '', 'false', 'False'):
            return None
        min_mb = float(os.environ.get('OPENSAI_MULTIPATH_MIN_MB', '8') or 8)
        if not size_bytes or size_bytes < min_mb * 1024 * 1024:
            return None
        gain = float(os.environ.get('OPENSAI_MULTIPATH_GAIN', '1.3') or 1.3)
        default_bw = float(os.environ.get('OPENSAI_DEFAULT_BW_MBPS', '10') or 10)

        def bw(dev, peer_ip, peer_port):
            rec = ((dev or {}).get('observed_links') or {}).get(f"{peer_ip}:{int(peer_port)}")
            return float(rec['mbps']) if (rec and rec.get('mbps')) else default_bw

        consumer_dev = device_info_list.get(consumer_dno) or {}
        threshold = bw(consumer_dev, src_ip, src_port) * gain   # 绕行须超过直连×gain 才值得
        best, best_bw = None, threshold
        for r_dno, r in device_info_list.items():
            if r_dno in (consumer_dno, src_dno) or not r.get('data_port'):
                continue
            r_ip, r_port = r.get('ip_address'), r.get('data_port')
            path_bw = min(bw(r, src_ip, src_port),            # src→R（用 R 实测从 src 拉的带宽近似）
                          bw(consumer_dev, r_ip, r_port))     # R→consumer（用 consumer 实测从 R 拉近似）
            if path_bw > best_bw:
                best, best_bw = (r_ip, r_port), path_bw
        return best

    @classmethod
    def _build_input_artifacts(cls, sub_task_info):
        """数据面：为待派发的子任务，构造“需要从前驱设备拉取的产物”清单。

        每项: {artifact_id, src_ip, src_port, pred_id}。
        前驱用原始 DAG 的 beforeNodes（运行期 dependence_list 会被清空，不能用它）。
        """
        task_id = sub_task_info.get_task_id()
        node_id = sub_task_info.get_sub_task_id()
        artifacts = []
        decoded = cls.all_task_decoded.get(task_id)
        if not decoded:
            return artifacts
        predecessors = []
        for node in decoded['dagData']['nodes']:
            if node['id'] == node_id:
                predecessors = node['form'].get('beforeNodes', []) or []
                break
        if not predecessors:
            return artifacts
        device_info_list = DeviceServiceImpl.read_device_info() or {}
        _consumer = sub_task_info.get_device_info()
        consumer_dno = _consumer.get_device_no() if _consumer else None
        for p in predecessors:
            try:
                pred_subtask = cls.all_task_dict[task_id]["queue"][p - 1]
                pred_dev = pred_subtask.get_device_info()
                if pred_dev is None:
                    continue
                dev = device_info_list.get(pred_dev.get_device_no())
                if not dev or not dev.get('data_port'):
                    logging.warning(f"前驱节点{p}的设备数据端点未知，暂无法构造产物拉取项")
                    continue
                entry = {
                    'artifact_id': f"{task_id}:{p}",
                    'src_ip': dev.get('ip_address'),
                    'src_port': dev.get('data_port'),
                    'pred_id': p,
                    # T3.2-A：按需求(声明/A2自动按数据量) + 源设备带宽预算，分配这条传输的限速(MB/s)；None=不限。
                    'rate_mbps': cls._artifact_rate_mbps(sub_task_info, pred_dev.get_device_no(), pred_node_id=p),
                }
                # 多链路：组合 P0-1c 多网卡直连源 + P0-1b 中继源；≥2 个源才附 sources+size 激活分条带聚合。
                size_bytes = cls.output_sizes.get((task_id, p), 0)
                src_ip, src_port = dev.get('ip_address'), dev.get('data_port')
                sources = cls._multinic_sources(src_ip, src_port, dev, size_bytes)   # 直连（可能多网卡）
                relay = cls._pick_relay(consumer_dno, pred_dev.get_device_no(),
                                        src_ip, src_port, size_bytes, device_info_list)
                if relay:
                    sources = list(sources)
                    sources.append({'host': relay[0], 'port': relay[1],
                                    'origin_ip': src_ip, 'origin_port': src_port})
                if len(sources) >= 2:
                    entry['size'] = size_bytes
                    entry['sources'] = sources
                    _nics = sum(1 for s in sources if not s.get('origin_ip'))
                    logging.info(f"[multipath] task={task_id} sub={node_id} 前驱{p} 多链路源={len(sources)} "
                                 f"(多网卡直连×{_nics}{'+中继' if relay else ''}, size={size_bytes})")
                # §14.2 传输计划事件：本地直读 / 单链路 / 多链路(中继·多网卡)，含每条物理路径(带 device_no 供三维窗映射)。
                try:
                    _src_dno = pred_dev.get_device_no()
                    # endpoint(ip:port) -> device_no，用于把中继路径映射回设备节点。
                    _ep2dno = {}
                    for _dn, _di in device_info_list.items():
                        if _di.get('ip_address') is not None and _di.get('data_port') is not None:
                            _ep2dno[f"{_di.get('ip_address')}:{int(_di.get('data_port'))}"] = _dn
                    _srcs = entry.get('sources') or [{'host': src_ip, 'port': src_port}]
                    _paths = []
                    for s in _srcs:
                        if s.get('origin_ip'):   # 中继路径：该 host 是中继设备
                            _dno = _ep2dno.get(f"{s.get('host')}:{int(s.get('port'))}")
                            _paths.append({'type': 'relay', 'host': s.get('host'), 'port': s.get('port'),
                                           'device': cls._dev_name(device_info_list, _dno)})
                        else:                    # 直连/多网卡：源设备
                            _paths.append({'type': 'direct', 'host': s.get('host'), 'port': s.get('port'),
                                           'device': cls._dev_name(device_info_list, _src_dno)})
                    cls._emit_event('transfer', {
                        'task_id': task_id, 'consumer_node': node_id, 'pred_id': p,
                        'src_device': cls._dev_name(device_info_list, _src_dno),
                        'consumer_device': cls._dev_name(device_info_list, consumer_dno),
                        'size': size_bytes, 'rate_mbps': entry.get('rate_mbps'),
                        'local_read': (_src_dno == consumer_dno),
                        'multipath': bool(entry.get('sources')),
                        'paths': _paths,
                    })
                except Exception as _e:
                    logging.debug(f"[event] transfer 忽略: {_e}")
                artifacts.append(entry)
            except Exception as e:
                logging.error(f"构造前驱{p}产物拉取项失败: {e}")
        return artifacts

    @classmethod
    def _cleanup_task_artifacts(cls, task_id):
        """7a：整图完成后，通知所有承担过该任务子任务的设备删除其落盘产物。
        此时 DAG 已全部 FINISH，产物必然无下游再需要 → 删除是安全的。
        通知尽力而为：某设备掉线/发送失败不影响其它（设备侧另有 TTL 兜底，见 7b）。"""
        try:
            devs = set()
            for st in (cls.all_task_dict.get(task_id, {}) or {}).get("queue", []):
                di = st.get_device_info()
                dno = di.get_device_no() if di else None
                if dno:
                    devs.add(dno)
            msg = {'artifact_cleanup': True, 'task_id': task_id}
            sent = 0
            for dno in devs:
                sock = cls.client_sockets.get(dno)
                if not sock:
                    continue
                try:
                    NetworkUtils.send_msg(sock, msg)
                    sent += 1
                except Exception as e:
                    logging.debug(f"[cleanup] 通知设备 {dno} 失败: {e}")
            logging.info(f"[cleanup] 任务 {task_id} 完成，已通知 {sent}/{len(devs)} 台设备清理产物")
        except Exception as e:
            logging.error(f"[cleanup] 任务 {task_id} 清理通知异常(已忽略): {e}")

    @classmethod
    # 子线程2：处理 ready_subtask_queue 中的就绪子任务
    def handle_ready_subtasks(cls):
        logging.info('The thread for handling ready subtasks start...')
        while True:

            if not cls.ready_subtask_queue.empty():
                with cls.data_lock:

                    sub_task_info = cls.ready_subtask_queue.get()
                    device_info = sub_task_info.get_device_info()
                    # 数据面：构造该子任务需要从前驱设备拉取的产物清单
                    input_artifacts = cls._build_input_artifacts(sub_task_info)
                    # B1+B2 算力/内存档位：按 声明/历史/所选设备可用 算好 _alloc，注入下发的 param_dict。
                    _param = sub_task_info.get_sub_task_input_param()
                    if isinstance(_param, dict):
                        _devd = (DeviceServiceImpl.read_device_info() or {}).get(device_info.get_device_no())
                        _param['_alloc'] = cls._alloc_compute(sub_task_info, _devd)

                    if sub_task_info.get_sub_task_data_list() is not None:
                        sub_task_data_list = sub_task_info.get_sub_task_data_list()
                        data_name_dict = {}
                        data_location_dict = {}
                        data_type_dict = {}
                        num = 0
                        for data_info in sub_task_data_list:
                            data_name_dict[str(num)] = data_info.get_data_name()
                            data_location_dict[str(num)] = data_info.get_data_location()
                            data_type_dict[str(num)] = data_info.get_data_type()
                            num += 1

                        data_dict = {"data_name": data_name_dict, "data_location": data_location_dict,
                                     "file_suffix": data_type_dict}

                        # 通过TCP发送任务到设备
                        device_no = device_info.get_device_no()
                        if device_no in cls.client_sockets:
                            # 构建消息（与原AlgorithmTransmission.send_algo_with_data类似）
                            msg_dict = {
                                'task_id': sub_task_info.get_task_id(),
                                'sub_task_id': sub_task_info.get_sub_task_id(),
                                'device_no': device_info.get_device_no(),
                                'algo_name': sub_task_info.get_task_algorithm(),
                                'algo_version': sub_task_info.get_sub_task_version(),
                                'param_dict': sub_task_info.get_sub_task_input_param(),
                                'input_artifacts': input_artifacts,
                                'data_dict': data_dict
                            }

                            # 构建P2P消息
                            p2p_msg = {
                                'udp_p2p_msg': 'udp_p2p_msg',  # 保持原有消息键名
                                'from_device_no': 'server',
                                'from_device_ip': HOST,
                                'from_device_port': PORT,
                                'to_device_no': device_no,
                                'to_device_ip': device_info.get_ip_address(),
                                'to_device_port': device_info.get_port(),
                                'msg': msg_dict
                            }

                            # 通过设备的TCP连接发送
                            try:
                                NetworkUtils.send_msg(cls.client_sockets[device_no], p2p_msg)
                                logging.info(f"通过TCP发送任务到设备: {device_no}")
                            except Exception as e:
                                logging.error(f"发送任务到设备失败: {e}")
                        else:
                            logging.error(f"找不到设备的连接: {device_no}")
                    else:
                        # 无数据的情况
                        device_no = device_info.get_device_no()
                        if device_no in cls.client_sockets:
                            # 构建消息（与原AlgorithmTransmission.send_algo_no_data类似）
                            msg_dict = {
                                'task_id': sub_task_info.get_task_id(),
                                'sub_task_id': sub_task_info.get_sub_task_id(),
                                'device_no': device_info.get_device_no(),
                                'algo_name': sub_task_info.get_task_algorithm(),
                                'algo_version': sub_task_info.get_sub_task_version(),
                                'param_dict': sub_task_info.get_sub_task_input_param(),
                                'input_artifacts': input_artifacts
                            }

                            # 构建P2P消息
                            p2p_msg = {
                                'udp_p2p_msg': 'udp_p2p_msg',  # 保持原有消息键名
                                'from_device_no': 'server',
                                'from_device_ip': HOST,
                                'from_device_port': PORT,
                                'to_device_no': device_no,
                                'to_device_ip': device_info.get_ip_address(),
                                'to_device_port': device_info.get_port(),
                                'msg': msg_dict
                            }

                            # 通过设备的TCP连接发送
                            try:
                                NetworkUtils.send_msg(cls.client_sockets[device_no], p2p_msg)
                                logging.info(f"通过TCP发送任务到设备: {device_no}")
                            except Exception as e:
                                logging.error(f"发送任务到设备失败: {e}")
                        else:
                            logging.error(f"找不到设备的连接: {device_no}")
            else:
                time.sleep(0.01)  # 每0.01秒检查一次

    # 推送所有连接上的设备的device_info和algorithm_info到前端页面, 5s扫描一次
    @classmethod
    def push_connected_device_info(cls, key_value: str):
        global user_dict
        connected_device_info = DeviceServiceImpl.read_device_info()
        devices_data = []
        for device_id, device_info in connected_device_info.items():
            device = {
                "deviceNo": device_info['device_name'],  # device_info['device_no'],
                "usedCpu": device_info['used_cpu'],
                "coreNum": device_info['core_num'],
                "idleMemory": device_info['idle_memory'],
                "usedMemory": device_info['used_memory'],
                "totalMemory": device_info['total_memory'],
                "workStatus": device_info['work_status'],
                "lng": device_info['lng'],
                "lat": device_info['lat'],
                "orderValue": device_info['order_value'],
                "ipAddress": device_info['ip_address'],
                "throughput": device_info['throughput'],
                "bandwidth": device_info['bandwidth'],
                "networkUsage": device_info['network_usage'],
                "delay": device_info['delay'],
                "port": device_info['port']
            }
            devices_data.append(device)
        cricle_info = {}
        cricle_info["algorithms"] = ActuatorImpl.read_algorithms()
        cricle_info["policies"] = ["default"]
        cricle_info["devices"] = devices_data
        base_info = {key_value: cricle_info}
        # threading.Thread(target=cls.save_history, daemon=True).start()
        # print(base_info)

        # 向用户客户端发送数据
        for username, addr in user_dict.items():
            if username in cls.client_sockets:
                try:
                    NetworkUtils.send_msg(cls.client_sockets[username], base_info)
                except Exception as e:
                    logging.error(f"Error sending to {username}: {e}")

    @classmethod
    def update_ready_task(cls, task_queue):
        # 遍历任务列表，更新内部的任务状态
        for subtask in task_queue:
            # READY 条件：未开始 且 无剩余前驱依赖（dependence_list 为空）
            if subtask.get_sub_task_status() == SubTaskStatus.UN_WORK.value and (not subtask.get_dependence_list()):
                # 更新store任务队列，并标记就绪任务的状态
                subtask.set_sub_task_delay(time.time())   # 将就绪时间戳暂存在ready subtask的delay字段里
                subtask.set_sub_task_status(SubTaskStatus.READY.value)

    @classmethod
    def handle_tcp_connections(cls):
        """处理所有的TCP连接"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
        # 注意：不要在 Windows 上设 SO_REUSEADDR。Windows 的 SO_REUSEADDR 允许多个进程
        # 同时绑定同一端口，导致客户端连接被不确定地分发到不同的 server 实例（曾导致注入器
        # 连到“没有设备”的旧实例、任务不被处理）。去掉后，重复启动会直接报“端口被占用”，
        # 问题立刻可见而不是静默路由错乱。
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)
        logging.info(f'TCP Server on {HOST}:{PORT}...')

        while True:
            client_sock, client_addr = server_socket.accept()
            logging.info(f"接受新连接: {client_addr}")

            # 启动一个新线程来处理这个客户端
            client_thread = threading.Thread(target=cls.handle_client_messages,
                                             args=(client_sock, client_addr),
                                             daemon=True)
            client_thread.start()

    @classmethod
    def handle_client_messages(cls, client_sock, client_addr):
        """处理来自单个客户端的消息"""
        global user_dict, task_dict, data_lock

        client_id = None  # 可能是用户名或设备ID
        is_device = False  # 该连接是否为边缘设备（用于断开时从Redis在线列表中移除）

        try:
            buf = bytearray()
            while True:
                data_decode = NetworkUtils.recv_msg(client_sock, buf)
                if data_decode is None:
                    logging.info(f"客户端断开连接: {client_addr}")
                    break

                # 处理接收到的数据（NDJSON 分帧：recv_msg 已返回解析后的对象）
                if data_decode is not None:
                    if NetworkUtils.is_dict(data_decode):
                        # 处理心跳消息(设备)
                        if 'heart_beat' in data_decode:
                            data_decode['ip_address'] = client_addr[0]
                            data_decode.update({'port': client_addr[1]})
                            DeviceServiceImpl.write_device_to_cache(data_decode)
                            # Reduce log noise
                            # logging.info(f"设备信息: {data_decode}")

                            # 保存设备连接
                            device_no = data_decode.get('device_no')
                            if device_no:
                                client_id = device_no
                                is_device = True
                                cls.client_sockets[device_no] = client_sock
                                logging.info(f"已记录设备 {device_no} 的连接")

                            # 发送响应（附带在线对端清单，供设备做 T1.2 主动链路探测）
                            peers = []
                            try:
                                for _dno, _info in (DeviceServiceImpl.read_device_info() or {}).items():
                                    if _dno != device_no and _info.get('data_port'):
                                        peers.append({'device_no': _dno,
                                                      'ip_address': _info.get('ip_address'),
                                                      'data_port': _info.get('data_port')})
                            except Exception:
                                pass
                            NetworkUtils.send_msg(client_sock,
                                {"SERVER RESPONSE IP": HOST, "SERVER RESPONSE PORT": PORT,
                                 "peers": peers})

                        # 处理用户消息
                        elif 'user_beat' in data_decode:
                            with data_lock:
                                logging.info(f"A new user is added: {data_decode}")
                                user_name = data_decode['user_beat']['name']
                                user_dict[user_name] = client_addr
                                client_id = user_name
                                cls.client_sockets[user_name] = client_sock

                        # 处理DAG任务
                        elif 'dagData' in data_decode:
                            print(ActuatorImpl.read_algorithms())
                            logging.info(f"DAG请求报文: {data_decode}")

                            # Fix: Handle both string and float timestamps
                            task_create_time = data_decode['dagData']["taskCreateTime"]

                            if isinstance(task_create_time, str):
                                # If it's a string, parse it
                                dt = datetime.datetime.strptime(task_create_time, "%Y-%m-%d %H:%M:%S")
                                dt_utc = dt.replace(tzinfo=datetime.timezone.utc)
                                timestamp = dt_utc.timestamp()
                            elif isinstance(task_create_time, (int, float)):
                                # If it's already a timestamp, use it directly
                                timestamp = task_create_time
                                dt_utc = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
                            else:
                                raise ValueError(f"Unexpected type for taskCreateTime: {type(task_create_time)}")

                            # 解析dag的整体结构
                            alltasks = data_decode['dagData']['nodes']
                            task_queue = []
                            task_id = data_decode['dagData']["taskName"] + str(timestamp)
                            with data_lock:
                                task_dict[task_id] = client_addr

                            for subtask in alltasks:
                                sub_task_info = SubTaskInfo()
                                sub_task_info.set_sub_task_info_by_node(task_id=task_id, subtask=subtask)
                                task_queue.append(sub_task_info)

                            with cls.data_lock:
                                cls.all_task_dict[task_id] = {}
                                cls.all_task_dict[task_id]["time"] = timestamp# time.time()  # data_decode['dagData']["taskCreateTime"]
                                cls.all_task_dict[task_id]["queue"] = task_queue
                                cls.all_task_decoded[task_id] = data_decode
                                logging.info(f"DAG任务加入总任务处理队列")

                            cls.update_ready_task(task_queue)
                            rsp_dict = {"dagRspStatus": True}
                            NetworkUtils.send_msg(client_sock, rsp_dict)

                        # 处理子任务响应
                        elif 'dag_subtask_rsp' in data_decode:
                            task_id = data_decode['msg']['task_id']
                            subtask_id = data_decode['msg']['sub_task_id']
                            subtask_res = data_decode['msg']['result']

                            logging.info(f"DAG任务{task_id}的子任务{subtask_id}完成,结果报文：{data_decode}")
                            with cls.data_lock:
                                if cls.all_task_dict[task_id] and cls.all_task_dict[task_id]["queue"][
                                    subtask_id - 1] != None:
                                    # 完成的子任务是否存在于指定store队列中
                                    fin_subtask = cls.all_task_dict[task_id]["queue"][subtask_id - 1]
                                    fin_subtask = cls.mark_sub_task_status(fin_subtask)
                                    cls.all_task_dict[task_id]["queue"][subtask_id - 1] = fin_subtask

                                    # T2.1 子任务完成 -> 释放它在所属设备上的预留。
                                    _fdev = fin_subtask.get_device_info()
                                    _fno = _fdev.get_device_no() if _fdev else None
                                    if _fno and cls.reservations.get(_fno):
                                        cls.reservations[_fno] = max(0, cls.reservations[_fno] - 1)
                                    # T3.2 记录该子任务实测产出大小，供下游放置按真实数据量估传输代价。
                                    cls.output_sizes[(task_id, subtask_id)] = data_decode['msg'].get('output_bytes', 0)
                                    # T3.2-A 归还该子任务此前占用的源设备出口带宽。
                                    for _s, _rt in cls.bw_reservations.pop((task_id, subtask_id), []):
                                        cls.committed_bw[_s] = max(0.0, cls.committed_bw.get(_s, 0.0) - _rt)
                                    # 归还该子任务此前在执行设备上占用的存储档位。
                                    _sres = cls.storage_reservations.pop((task_id, subtask_id), None)
                                    if _sres:
                                        _sd, _smb = _sres
                                        cls.committed_storage_mb[_sd] = max(0.0, cls.committed_storage_mb.get(_sd, 0.0) - _smb)
                                    cls.inflight.pop((task_id, subtask_id), None)   # C1 在飞登记移除
                                    # B1 历史画像：用设备上报的 metrics 更新该算法的峰值内存/耗时。
                                    cls._update_algo_history(fin_subtask.get_task_algorithm(),
                                                             data_decode['msg'].get('metrics'))

                                    if fin_subtask.get_sub_task_status() == SubTaskStatus.EARLY_EXIT.value:
                                        # subtask 早退，应用任务提早完成
                                        logging.info(f"DAG任务{task_id}提前完成, 将发送结果报文至client")
                                        result_dict = cls.summarize_task_result(task_id)
                                        # 发送给对应的用户客户端
                                        for username, addr in user_dict.items():
                                            if task_dict[task_id] == addr and username in cls.client_sockets:
                                                NetworkUtils.send_msg(cls.client_sockets[username], result_dict)
                                                break

                                    elif fin_subtask.get_sub_task_status() == SubTaskStatus.DROPPED.value:
                                        # subtask 被丢弃
                                        logging.info(f"DAG任务{task_id}被丢弃,整体卸载失败，将发送结果报文至client")
                                        result_dict = cls.summarize_task_result(task_id)
                                        # 发送给对应的用户客户端
                                        for username, addr in user_dict.items():
                                            if task_dict[task_id] == addr and username in cls.client_sockets:
                                                NetworkUtils.send_msg(cls.client_sockets[username], result_dict)
                                                break

                                    else:
                                        # subtask 正常完成，触发后继节点更新
                                        fin_subtask.set_sub_task_delay(time.time() - fin_subtask.get_sub_task_delay())
                                        fin_subtask.set_sub_task_result_info(subtask_res)
                                        for subtask in cls.all_task_dict[task_id]["queue"]:
                                            # 删除其它节点对当前完成节点的依赖（从其 dependence_list 中移除）
                                            deps = subtask.get_dependence_list()
                                            if deps and subtask_id in deps:
                                                deps.remove(subtask_id)
                                                subtask.set_dependence_list(deps)

                                        # 检查是否整个DAG任务都已完成（所有节点状态都是FINISH）
                                        all_finished = all(
                                            subtask.get_sub_task_status() == SubTaskStatus.FINISH.value 
                                            for subtask in cls.all_task_dict[task_id]["queue"]
                                        )
                                        
                                        if all_finished:
                                            # 整个DAG任务已完成
                                            logging.info(f"DAG任务{task_id}总体已完成, 将发送结果报文至client")
                                            cls.placement_plan.pop(task_id, None)   # 释放前瞻规划缓存
                                            cls._cleanup_task_artifacts(task_id)    # 7a 通知涉及设备删除该任务落盘产物
                                            result_dict = cls.summarize_task_result(task_id)
                                            # 发送给对应的用户客户端
                                            for username, addr in user_dict.items():
                                                if task_dict[task_id] == addr and username in cls.client_sockets:
                                                    NetworkUtils.send_msg(cls.client_sockets[username], result_dict)
                                                    break

                                        cls.update_ready_task(cls.all_task_dict[task_id]["queue"])
                                        logging.info(f"DAG子任务完成整体更新！")

                        # 处理DAG更新
                        elif 'dagUpdate' in data_decode:
                            task_id = data_decode['dagUpData']["taskName"]
                            subtask_feedbacks = []
                            with cls.data_lock:  # 添加同步锁
                                if task_id not in cls.all_task_dict:
                                    logging.error(f"Invalid task ID: {task_id}")
                                    continue
                                for subtask in cls.all_task_dict[task_id]["queue"]:
                                    subtask_result = subtask.get_sub_task_result_by_node()
                                    # 动态转换设备ID为可读名称
                                    if subtask_result["result"]["deviceId"] != "Unknown":
                                        device_id = subtask_result["result"]["deviceId"]
                                        subtask_result["result"]["deviceId"] = DeviceServiceImpl.device_map.get(
                                            device_id, "Unknown")
                                    subtask_feedbacks.append(subtask_result)

                            logging.info(f"接收前端对任务{task_id}的反馈请求, 将更新结果报文至client")
                            result_dict = {"dagUpdateFeedback": {"taskName": task_id, "nodes": subtask_feedbacks}}
                            # 发送给对应的用户客户端
                            for username, addr in user_dict.items():
                                if task_dict[task_id] == addr and username in cls.client_sockets:
                                    NetworkUtils.send_msg(cls.client_sockets[username], result_dict)
                                    break

                        # 处理设备/算法/策略信息请求
                        elif 'devicesInfo' in data_decode or 'algorithmsInfo' in data_decode or 'policiesInfo' in data_decode:
                            key = next(iter(data_decode))
                            cls.push_connected_device_info(key)
                            # logging.info(f"接收前端 client 对 base_info 的反馈请求, 将更新结果报文至client")

                        # §14.2 决策事件流：直接在请求方连接上回传事件快照（放置/资源/传输）。
                        elif 'eventsInfo' in data_decode:
                            try:
                                NetworkUtils.send_msg(client_sock, {"eventsInfo": {"events": cls._events_snapshot()}})
                            except Exception as _e:
                                logging.debug(f"[event] 回传事件失败: {_e}")
                        
                        # 处理P2P消息
                        elif 'udp_p2p_msg' in data_decode:
                            logging.info(f"TCP-P2P报文: {data_decode}")
                            to_device_no = data_decode['to_device_no']

                            # 查找设备连接
                            if to_device_no in cls.client_sockets:
                                try:
                                    NetworkUtils.send_msg(cls.client_sockets[to_device_no], data_decode)  # 原样转发（分帧）
                                    logging.info(f"转发消息到设备 {to_device_no}")
                                except Exception as e:
                                    logging.error(f"转发消息失败: {e}")
                                    del cls.client_sockets[to_device_no]
                            else:
                                logging.error(f"未找到设备 {to_device_no} 的连接")

                        # 处理广播消息
                        elif 'udp_bc_msg' in data_decode:
                            logging.info(f"TCP-BC报文: {data_decode}")
                            for num in range(len(data_decode['to_device_no_list'])):
                                p2p_msg_dict, _, _ = PacketProtocolConversion.change_bc_p2p(
                                    data_dict=data_decode, index=num)

                                # 获取目标设备号并转发消息
                                to_device_no = p2p_msg_dict.get('to_device_no')
                                if to_device_no and to_device_no in cls.client_sockets:
                                    try:
                                        NetworkUtils.send_msg(cls.client_sockets[to_device_no], p2p_msg_dict)
                                        logging.info(f"广播消息到设备 {to_device_no}")
                                    except Exception as e:
                                        logging.error(f"广播消息失败: {e}")
                                        del cls.client_sockets[to_device_no]
                                else:
                                    logging.error(f"未找到设备 {to_device_no} 的连接")
        except Exception as e:
            logging.error(f"处理客户端消息时发生错误: {e}")
        finally:
            # 清理连接资源
            if client_id and client_id in cls.client_sockets:
                del cls.client_sockets[client_id]
                logging.info(f"已移除客户端连接: {client_id}")

            # 设备断开：从Redis在线列表中移除，避免随机卸载决策选中已下线设备
            # （否则离线设备会在 DEVICE_ONLINE_SET 中残留 TTL 24h，导致任务静默丢失）
            if is_device and client_id:
                DeviceServiceImpl.remove_device(client_id)

            # 如果是用户端，也从user_dict中删除
            for username, addr in list(user_dict.items()):
                if addr == client_addr:
                    del user_dict[username]
                    break
            client_sock.close()

    @classmethod
    def mark_sub_task_status(cls, sub_task: SubTaskInfo):
        choice = 1
        if choice == 1:
            sub_task.set_sub_task_status(SubTaskStatus.FINISH.value)
        elif choice == 2:
            sub_task.set_sub_task_status(SubTaskStatus.EARLY_EXIT.value)
        else:
            sub_task.set_sub_task_status(SubTaskStatus.DROPPED.value)
        return sub_task

    @classmethod
    def summarize_task_result(cls, task_id: str):
        subtask_results = []
        for subtask in cls.all_task_dict[task_id]["queue"]:
            subtask_result = subtask.get_sub_task_result_by_node()
            device_id = subtask_result["result"]["deviceId"]
            if device_id == "Unknown":
                device_name = "None"
            else:
                device_name = DeviceServiceImpl.device_map[device_id] if "deviceId" in subtask_result["result"] else "Unknown"
            subtask_result["result"]["deviceId"] = device_name
            subtask_results.append(subtask_result)
        # 使用入队时已保存的创建时间戳，而不是用硬编码的 task_id[12:] 切片解析
        # （旧写法假设任务名固定为12字符，任务名长度不同时会算出错误的 finishedTime）。
        start_time = cls.all_task_dict[task_id]["time"]
        result_dict = {
            "dagResult": {"taskName": task_id, "finishedTime": time.time() - start_time, "nodes": subtask_results}}
        return result_dict

    @classmethod
    def activate_server(cls, host, port):
        """启动TCP服务器"""
        global task_dict, data_lock, user_dict

        # 启动子线程1和子线程2
        thread1 = threading.Thread(target=cls.offloading_making_process, daemon=True)
        thread2 = threading.Thread(target=cls.handle_ready_subtasks, daemon=True)
        thread1.start()
        thread2.start()

        # 清空device list的缓存内容
        DeviceServiceImpl.clear_cache()

        # 启动TCP连接处理线程
        cls.handle_tcp_connections()


if __name__ == '__main__':
    # 全局共享变量
    user_dict = {
        # "username":(ip, port)  # 用户名与用户端口的映射
    }
    task_dict = {
        # 'task1': (1, 2),  # 任务名与用户端口的映射
    }

    data_lock = threading.Lock()

    UserServer.activate_server(HOST, PORT)