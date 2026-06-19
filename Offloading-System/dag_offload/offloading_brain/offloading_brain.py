import os
import random
import logging
from collections import deque
from task.bean.sub_task_info import SubTaskInfo
class OffloadingImpl():

    @classmethod
    def random_strategy(cls, device_info_list: dict, subtask_info:SubTaskInfo):
        device_size = len(device_info_list)

        if device_size > 0:
            keys_list = list(device_info_list.keys())
            select_id = random.randrange(0, device_size)
            return device_info_list[keys_list[select_id]]
        else:
            logging.info("无在线设备...")
            return None

    @classmethod
    def direct_strategy(cls, device_info_list: dict, subtask_info: SubTaskInfo):
        idx = subtask_info.get_sub_task_direct_allocate_id()
        device_size = len(device_info_list)
        # 1-based 索引到“按 device_no 稳定排序”的设备列表。
        # （旧实现是 0-based 且要求 idx>0，导致 idx=0 永远失效，且 dict 顺序不稳定、不可复现。）
        if 1 <= idx <= device_size:
            keys_list = sorted(device_info_list.keys())
            return device_info_list[keys_list[idx - 1]]
        else:
            logging.info("directAllocationId 越界或<=0,为该任务随机分配一个设备...")
            return cls.random_strategy(device_info_list, subtask_info)


    # —— T3.1/T3.2 自适应位置/资源感知放置 ——
    # 把「传输代价」按真实数据量估成秒数(时延 + 字节/带宽)，与「负载代价」(W_LOAD 秒/已预留任务)
    # 一起折算成"预计延迟秒数"，选总代价最低的设备。数据大 -> 传输项主导 -> 自动就近(聚合)；
    # 数据小 -> 负载项主导 -> 自动铺开(分散)。拐点随数据量自动移动，无需手调权重（这就是 T3.2）。
    # W_LOAD=0 退化为「纯最小化传输」(聚合)，W_LOAD 很大则强制分散——实验对照用。
    W_XFER = float(os.environ.get('OPENSAI_W_XFER', '1.0'))            # 传输秒数的权重
    W_LOAD = float(os.environ.get('OPENSAI_W_LOAD', '0.5'))           # 每个已预留任务折算的延迟秒数
    DEFAULT_BW_MBPS = float(os.environ.get('OPENSAI_DEFAULT_BW_MBPS', '10.0'))  # 未知链路默认带宽(MB/s)
    DEFAULT_LAT_MS = 50.0                                             # 未知链路默认时延(ms)

    @classmethod
    def _endpoint(cls, dev: dict):
        ip, dp = dev.get('ip_address'), dev.get('data_port')
        if ip and dp is not None:
            return f"{ip}:{int(dp)}"
        return None

    @classmethod
    def _transfer_seconds(cls, cand_info: dict, src_dno: str, nbytes, device_info_list: dict):
        """候选设备从源设备 src_dno 拉取 nbytes 字节的预计耗时(秒)。
        用候选设备的链路遥测 observed_links[src_endpoint] 的带宽/时延；未知则用默认值。
        nbytes<=0（前驱无产出/未知）-> 0。同机前驱由调用方跳过（代价=0=就近收益）。"""
        if not nbytes or nbytes <= 0:
            return 0.0
        src = device_info_list.get(src_dno)
        ep = cls._endpoint(src) if src else None
        link = (cand_info.get('observed_links') or {}).get(ep) if ep else None
        if link and link.get('mbps'):
            bw = float(link['mbps']) * 1e6
            lat = float(link.get('last_ms', cls.DEFAULT_LAT_MS)) / 1000.0
        else:
            bw = cls.DEFAULT_BW_MBPS * 1e6
            lat = cls.DEFAULT_LAT_MS / 1000.0
        return lat + nbytes / max(bw, 1.0)

    @classmethod
    def score_candidates(cls, device_info_list: dict, subtask_info: SubTaskInfo,
                         pred_info=None, reservations=None):
        """对每个候选设备按同一代价模型打分，返回按代价升序的明细列表（只读、无副作用）。
        供 locality_strategy 选最优，也供放置决策事件展示「候选 × 代价分项」。
        每项: {device, xfer_s, load, cost}。"""
        reservations = reservations or {}
        pred_info = pred_info or []
        out = []
        for dno in sorted(device_info_list.keys()):
            info = device_info_list[dno]
            xfer = sum(cls._transfer_seconds(info, src, b, device_info_list)
                       for (src, b) in pred_info if src and src != dno)
            # capacity-aware：负载按「已预留/并行容量」折算——设备有 N 个 worker 可并行，
            # 故同机塞到 N 个任务几乎不罚分（它们并行跑），超过容量才显著变贵。
            slots = max(1, int(info.get('worker_slots', 1) or 1))
            load = reservations.get(dno, 0) / slots
            cost = cls.W_XFER * xfer + cls.W_LOAD * load
            out.append({'device': dno, 'xfer_s': round(xfer, 3),
                        'load': round(load, 3), 'cost': round(cost, 3)})
        out.sort(key=lambda x: x['cost'])
        return out

    @classmethod
    def locality_strategy(cls, device_info_list: dict, subtask_info: SubTaskInfo,
                          pred_info=None, reservations=None):
        """T3.2 自适应放置：选「预计传输耗时 + 负载延迟」总代价最低的设备。

        pred_info: [(前驱device_no, 前驱实测产出字节数), ...]（同机前驱传输代价=0=就近收益）。
        reservations: 预留账本 {device_no: 已预留任务数}（T2.1，实时负载信号）。
        cost(d) = W_XFER*Σ传输秒数(非本机前驱) + W_LOAD*已预留任务数；取最小，按 device_no 稳定 tie-break。"""
        if not device_info_list:
            logging.info("无在线设备...")
            return None
        scored = cls.score_candidates(device_info_list, subtask_info, pred_info, reservations)
        best_dno = scored[0]['device']
        logging.info(f"[placement] 子任务 {subtask_info.get_sub_task_id()} -> "
                     f"{best_dno} (cost={scored[0]['cost']:.3f}s, preds={pred_info or []})")
        return device_info_list[best_dno]

    @classmethod
    def plan_dag(cls, decoded: dict, device_info_list: dict, reservations=None):
        """前瞻放置：整图「反向就近」规划，一次性给整张 DAG 的每个节点选设备，返回 {node_id: device_no}。

        贪心放置(locality_strategy)只看前驱，所以独立的源节点(传感器)只能按负载铺开，无法为了
        下游汇聚而预先聚到一起。这里按**倒拓扑序**(消费者先放)处理：放每个节点时，让它偏向其
        (已放置的)后继所在设备——产出就地被消费=0 传输。这样连源节点都会被"拉"向其消费者，
        从而吃到贪心够不到的聚合收益。沿用同一成本模型：
            cost(d) = W_XFER*Σ传输秒数(产出送给非本机后继) + W_LOAD*(已预留 + 本规划已分配)
        数据大 -> 传输项主导 -> 整条子树聚到消费者处；数据小 -> 负载项主导 -> 铺开抢并行。
        产出大小用节点声明的 size_mb 估(提交时即可知)；directAllocationId 仍被尊重(固定)。"""
        reservations = reservations or {}
        devs = sorted(device_info_list.keys())
        if not devs:
            return {}
        nodes = decoded.get('dagData', {}).get('nodes', [])
        before, size, direct = {}, {}, {}
        for nd in nodes:
            nid = nd['id']
            form = nd.get('form', {})
            before[nid] = list(form.get('beforeNodes', []) or [])
            size[nid] = int(form.get('taskInputInfo', {}).get('size_mb', 0) or 0) * 1024 * 1024
            direct[nid] = form.get('directAllocationId', -1)
        succ = {nid: [] for nid in before}
        for c, preds in before.items():
            for p in preds:
                if p in succ:
                    succ[p].append(c)
        # 前向拓扑(Kahn) 再反转 = 倒拓扑(后继先于前驱)
        indeg = {nid: len(before[nid]) for nid in before}
        q = deque(sorted(n for n in before if indeg[n] == 0))
        topo = []
        while q:
            n = q.popleft()
            topo.append(n)
            for c in sorted(succ.get(n, [])):
                indeg[c] -= 1
                if indeg[c] == 0:
                    q.append(c)
        if len(topo) != len(before):     # 异常(有环)则退化为任意稳定序
            topo = sorted(before)
        # 祖先/后代集合：用于负载项只统计「独立(可并发)」任务——依赖链上的任务是顺序执行、
        # 不并发，所以把一条链同机放置不应计并发惩罚（否则会把廉价传输的依赖链错误地拆开）。
        desc = {n: set() for n in before}
        for n in reversed(topo):         # sinks first：后继的 desc 已就绪
            s = set()
            for c in succ.get(n, []):
                s.add(c); s |= desc[c]
            desc[n] = s
        anc = {n: set() for n in before}
        for n in topo:                   # sources first：前驱的 anc 已就绪
            a = set()
            for p in before.get(n, []):
                a.add(p); a |= anc[p]
            anc[n] = a
        related = {n: desc[n] | anc[n] | {n} for n in before}
        slots_of = {d: max(1, int(device_info_list[d].get('worker_slots', 1) or 1)) for d in devs}
        bw = cls.DEFAULT_BW_MBPS * 1e6
        lat = cls.DEFAULT_LAT_MS / 1000.0
        dev, placed_on = {}, {d: [] for d in devs}
        for n in reversed(topo):         # 倒拓扑：消费者已放，再放它
            pin = direct.get(n, -1)
            if isinstance(pin, int) and 1 <= pin <= len(devs):
                chosen = devs[pin - 1]
            else:
                nb = size.get(n, 0)
                best, best_cost = None, None
                for d in devs:
                    xfer = sum(lat + nb / bw
                               for c in succ.get(n, [])
                               if nb > 0 and dev.get(c) is not None and dev[c] != d)
                    # 并发负载 = 该设备上「与 n 独立」的已放置节点数 + 其它任务的预留，
                    # 再按并行容量(worker 数)折算——容量内并行不罚分，超容量才变贵(capacity-aware)。
                    indep = sum(1 for m in placed_on[d] if m not in related[n])
                    cost = cls.W_XFER * xfer + cls.W_LOAD * ((reservations.get(d, 0) + indep) / slots_of[d])
                    if best_cost is None or cost < best_cost:
                        best_cost, best = cost, d
                chosen = best
            dev[n] = chosen
            placed_on[chosen].append(n)
        logging.info(f"[plan] 整图反向就近规划完成: {dev}")
        return dev

    def rl_based_strategy(cls, device_info_list: dict, subtask_info: SubTaskInfo):
        pass
        # device_size = len(device_info_list)
        # cpu_consume = [1 for i in range(device_size)]
        # band_width = [0 for i in range(device_size)]
        # base info : data size, remain_time/total_time, task_id/total_id, is_exit, device cpu,
        # test
        # in_data = subtask_info.get_sub_task_input_param()
        # data_size = in_data["DataSize"]
        # is_exit = in_data["isExit"]
        # task_id = subtask_info.get_sub_task_id()
        # keys_list = list(device_info_list.keys())
        # for i in range(device_size):
        #     device_dict = device_info_list[keys_list[i]]
        #     cpu_consume[i] = float(device_dict['used_cpu'].replace('%', ''))
        #     band_width[i] = device_dict['bandwidth']
        # print(is_exit, data_size, task_id, cpu_consume, band_width)





