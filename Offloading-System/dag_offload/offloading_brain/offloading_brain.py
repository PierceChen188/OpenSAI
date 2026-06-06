import os
import random
import logging
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
    def locality_strategy(cls, device_info_list: dict, subtask_info: SubTaskInfo,
                          pred_info=None, reservations=None):
        """T3.2 自适应放置：选「预计传输耗时 + 负载延迟」总代价最低的设备。

        pred_info: [(前驱device_no, 前驱实测产出字节数), ...]（同机前驱传输代价=0=就近收益）。
        reservations: 预留账本 {device_no: 已预留任务数}（T2.1，实时负载信号）。
        cost(d) = W_XFER*Σ传输秒数(非本机前驱) + W_LOAD*已预留任务数；取最小，按 device_no 稳定 tie-break。"""
        if not device_info_list:
            logging.info("无在线设备...")
            return None
        reservations = reservations or {}
        pred_info = pred_info or []
        best, best_cost = None, None
        for dno in sorted(device_info_list.keys()):
            info = device_info_list[dno]
            xfer = sum(cls._transfer_seconds(info, src, b, device_info_list)
                       for (src, b) in pred_info if src and src != dno)
            cost = cls.W_XFER * xfer + cls.W_LOAD * reservations.get(dno, 0)
            if best_cost is None or cost < best_cost:
                best_cost, best = cost, info
        logging.info(f"[placement] 子任务 {subtask_info.get_sub_task_id()} -> "
                     f"{best.get('device_no') if best else None} (cost={best_cost:.3f}s, preds={pred_info})")
        return best

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





