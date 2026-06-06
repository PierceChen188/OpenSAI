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


    # —— T3.1 位置/资源感知放置 ——
    # 权衡：本地命中(避免一次跨设备传输)收益远大于负载/链路差异 -> W_LOC 最大。
    # 三个权重可由环境变量覆盖，便于调参/实验（W_LOAD 越大越倾向分散、越小越倾向聚合）。
    W_LOC = float(os.environ.get('OPENSAI_W_LOC', '100'))   # 每个「前驱产出已在本机」的本地命中收益
    W_XFER = float(os.environ.get('OPENSAI_W_XFER', '1'))   # 每 ms 链路时延的传输代价（前驱不在本机时）
    W_LOAD = float(os.environ.get('OPENSAI_W_LOAD', '5'))   # 负载惩罚权重（=0 则独立任务聚合，>0 则分散）
    DEFAULT_LINK_MS = 50.0   # 未测过的链路，先假设一个中等代价

    @classmethod
    def _endpoint(cls, dev: dict):
        ip, dp = dev.get('ip_address'), dev.get('data_port')
        if ip and dp is not None:
            return f"{ip}:{int(dp)}"
        return None

    @classmethod
    def _link_ms(cls, cand_info: dict, src_dno: str, device_info_list: dict):
        """候选设备 cand 从源设备 src_dno 拉数据的时延(ms)。
        用 cand 的被动链路遥测 observed_links[src_endpoint]；没测过则用默认值。"""
        src = device_info_list.get(src_dno)
        if not src:
            return cls.DEFAULT_LINK_MS
        ep = cls._endpoint(src)
        st = (cand_info.get('observed_links') or {}).get(ep) if ep else None
        if st and st.get('last_ms') is not None:
            return float(st['last_ms'])
        return cls.DEFAULT_LINK_MS

    @classmethod
    def _load_penalty(cls, info: dict, reserved: int):
        """设备负载惩罚：CPU 使用率 + 并发压力(运行中+已预留)/核数 + 内存压力。"""
        try:
            cpu = float(str(info.get('used_cpu', '0')).strip('%')) / 100.0
        except Exception:
            cpu = 0.0
        cores = info.get('core_num') or 1
        running = info.get('work_status') or 0
        busy = (running + reserved) / max(1, cores)
        idle = info.get('idle_memory') or 0.0
        total = info.get('total_memory') or 1.0
        mem_press = 1.0 - (idle / total if total else 1.0)
        return cpu + 2.0 * busy + 0.5 * mem_press

    @classmethod
    def locality_strategy(cls, device_info_list: dict, subtask_info: SubTaskInfo,
                          pred_devices=None, reservations=None):
        """位置感知 + 资源感知放置。

        pred_devices: 该子任务各前驱产出所在的 device_no 列表（server 提供，未知为 None）。
        reservations: 预留账本 {device_no: 已预留任务数}（T2.1）。
        得分 = W_LOC*本地命中 - W_XFER*Σ(非本地前驱的链路时延) - W_LOAD*负载惩罚；取最高，按 device_no 稳定 tie-break。
        无前驱时退化为「挑负载最低的设备」。"""
        if not device_info_list:
            logging.info("无在线设备...")
            return None
        reservations = reservations or {}
        pred_set = [d for d in (pred_devices or []) if d]
        best, best_score = None, None
        for dno in sorted(device_info_list.keys()):
            info = device_info_list[dno]
            local_hits = sum(1 for d in pred_set if d == dno)
            xfer = sum(cls._link_ms(info, d, device_info_list) for d in pred_set if d != dno)
            load = cls._load_penalty(info, reservations.get(dno, 0))
            score = cls.W_LOC * local_hits - cls.W_XFER * xfer - cls.W_LOAD * load
            if best_score is None or score > best_score:
                best_score, best = score, info
        logging.info(f"[placement] 子任务 {subtask_info.get_sub_task_id()} 选中设备 "
                     f"{best.get('device_no') if best else None} (score={best_score:.2f}, preds={pred_set})")
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





