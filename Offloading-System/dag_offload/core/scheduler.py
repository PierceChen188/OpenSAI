"""计算分布化（#3）：DAG 生命周期状态 + 调度/完成/汇总方法。

P6a：从 `UserServer` 抽出。本类**持有** DAG 状态对象（all_task_dict / all_task_decoded /
ready_subtask_queue / output_sizes / inflight）；UserServer 以类属性**别名**指向同一对象，
使既有 44 处内联引用零改动、行为不变（这些状态只被 mutate（subscript/.put/.pop），不被整体重绑，
故别名安全）。线程安全沿用协调器的 data_lock（本类方法在该锁下被调用）。

派发主循环与 dag_subtask_rsp 完成处理暂仍由协调器编排，后续逐步迁入本类。
"""
import queue
import time

from config.sub_task_status import SubTaskStatus
from predis.implements.device_service_impl import DeviceServiceImpl


class Scheduler:
    def __init__(self):
        self.all_task_dict = {}        # task_id -> {"time": timestamp, "queue": [SubTaskInfo, ...]}
        self.all_task_decoded = {}     # task_id -> decoded dagData
        self.ready_subtask_queue = queue.Queue()
        self.output_sizes = {}         # (task_id, node_id) -> 实测产出字节数
        self.inflight = {}             # (task_id, node_id) -> (device_no, 放置时间戳)

    def update_decoded_by_dict(self, task_id):
        task_dict = self.all_task_dict[task_id]
        task_encoded = self.all_task_decoded[task_id]
        for node in task_encoded['dagData']['nodes']:
            node['result']['status'] = task_dict["queue"][node['id'] - 1].get_subtask_status()
            node['result']['delay'] = task_dict["queue"][node['id'] - 1].get_subtask_delay()
            node['result']['deviceId'] = task_dict["queue"][node['id'] - 1].get_subtask_device()

    @staticmethod
    def mark_sub_task_status(sub_task):
        choice = 1
        if choice == 1:
            sub_task.set_sub_task_status(SubTaskStatus.FINISH.value)
        elif choice == 2:
            sub_task.set_sub_task_status(SubTaskStatus.EARLY_EXIT.value)
        else:
            sub_task.set_sub_task_status(SubTaskStatus.DROPPED.value)
        return sub_task

    def summarize_task_result(self, task_id):
        subtask_results = []
        for subtask in self.all_task_dict[task_id]["queue"]:
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
        start_time = self.all_task_dict[task_id]["time"]
        result_dict = {
            "dagResult": {"taskName": task_id, "finishedTime": time.time() - start_time, "nodes": subtask_results}}
        return result_dict
