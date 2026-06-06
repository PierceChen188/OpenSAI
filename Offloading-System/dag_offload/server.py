import json
import logging
import socket
import time
import queue
import datetime
import threading
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 将上一级目录添加到Python的模块搜索路径
from config.logging_config import LOG_FORMAT
from config.network_config import HOST, PORT
from config.sub_task_status import SubTaskStatus
from device.bean.devce_info import DeviceInfo
from network.utils.network_utils import NetworkUtils
from network.protocol.packet_protocol_conversion import PacketProtocolConversion
from predis.implements.device_service_impl import DeviceServiceImpl
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

    @classmethod
    def update_decoded_by_dict(cls, task_id: str):
        task_dict = cls.all_task_dict[task_id]
        task_encoded = cls.all_task_decoded[task_id]
        for node in task_encoded['dagData']['nodes']:
            node['result']['status'] = task_dict["queue"][node['id'] - 1].get_subtask_status()
            node['result']['delay'] = task_dict["queue"][node['id'] - 1].get_subtask_delay()
            node['result']['deviceId'] = task_dict["queue"][node['id'] - 1].get_subtask_device()

    @classmethod
    def offloading_strategy(cls, subtask):
        '''
            抽象出一层内容,将具体的卸载决策放在./offloading_brain里
        '''
        logging.info('making offloading strategy for subtask')
        device_info_list = DeviceServiceImpl.read_device_info()
        if subtask.get_sub_task_direct_allocate_id() != -1:
            # 手动指定（directAllocationId>0）：保持原行为不变。
            device_dict = OffloadingImpl.direct_strategy(device_info_list, subtask)
        else:
            # T3.1 位置/资源感知放置（替换原 random）：传入前驱所在设备 + 预留账本。
            pred_devices = cls._predecessor_devices(subtask)
            device_dict = OffloadingImpl.locality_strategy(
                device_info_list, subtask, pred_devices, cls.reservations)
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
        return True

    @classmethod
    def _predecessor_devices(cls, sub_task_info):
        """返回该子任务各前驱产出所在的 device_no 列表（未知为 None）。

        前驱用原始 DAG 的 beforeNodes；前驱产出在哪台机 = 该前驱子任务被放置到的设备
        （运行期已通过 set_device_info 记录）。这就是 T2.2「数据位置追踪」的来源。"""
        task_id = sub_task_info.get_task_id()
        node_id = sub_task_info.get_sub_task_id()
        decoded = cls.all_task_decoded.get(task_id)
        devs = []
        if not decoded:
            return devs
        preds = []
        for node in decoded['dagData']['nodes']:
            if node['id'] == node_id:
                preds = node['form'].get('beforeNodes', []) or []
                break
        for p in preds:
            try:
                pred_dev = cls.all_task_dict[task_id]["queue"][p - 1].get_device_info()
                devs.append(pred_dev.get_device_no() if pred_dev else None)
            except Exception:
                devs.append(None)
        return devs

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
        while True:

            try:
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
                artifacts.append({
                    'artifact_id': f"{task_id}:{p}",
                    'src_ip': dev.get('ip_address'),
                    'src_port': dev.get('data_port'),
                    'pred_id': p,
                })
            except Exception as e:
                logging.error(f"构造前驱{p}产物拉取项失败: {e}")
        return artifacts

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
                                cls.client_sockets[device_no].send(json.dumps(p2p_msg).encode('utf-8'))
                                logging.info(f"通过TCP发送任务到设备: {device_no}")
                                time.sleep(0.2) # 防止TCP粘包
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
                                cls.client_sockets[device_no].send(json.dumps(p2p_msg).encode('utf-8'))
                                logging.info(f"通过TCP发送任务到设备: {device_no}")
                                time.sleep(0.2) # 防止TCP粘包
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
                    cls.client_sockets[username].send(json.dumps(base_info).encode('utf-8'))
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
            while True:
                data = client_sock.recv(65535)
                if not data:
                    logging.info(f"客户端断开连接: {client_addr}")
                    break

                # 处理接收到的数据
                if NetworkUtils.is_json(data.decode('utf-8')):
                    data_decode = json.loads(data.decode('utf-8'))
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
                            client_sock.send(json.dumps(
                                {"SERVER RESPONSE IP": HOST, "SERVER RESPONSE PORT": PORT,
                                 "peers": peers}).encode('utf-8'))

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
                            client_sock.send(json.dumps(rsp_dict).encode('utf-8'))

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

                                    if fin_subtask.get_sub_task_status() == SubTaskStatus.EARLY_EXIT.value:
                                        # subtask 早退，应用任务提早完成
                                        logging.info(f"DAG任务{task_id}提前完成, 将发送结果报文至client")
                                        result_dict = cls.summarize_task_result(task_id)
                                        # 发送给对应的用户客户端
                                        for username, addr in user_dict.items():
                                            if task_dict[task_id] == addr and username in cls.client_sockets:
                                                cls.client_sockets[username].send(json.dumps(result_dict).encode('utf-8'))
                                                break

                                    elif fin_subtask.get_sub_task_status() == SubTaskStatus.DROPPED.value:
                                        # subtask 被丢弃
                                        logging.info(f"DAG任务{task_id}被丢弃,整体卸载失败，将发送结果报文至client")
                                        result_dict = cls.summarize_task_result(task_id)
                                        # 发送给对应的用户客户端
                                        for username, addr in user_dict.items():
                                            if task_dict[task_id] == addr and username in cls.client_sockets:
                                                cls.client_sockets[username].send(json.dumps(result_dict).encode('utf-8'))
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
                                            result_dict = cls.summarize_task_result(task_id)
                                            # 发送给对应的用户客户端
                                            for username, addr in user_dict.items():
                                                if task_dict[task_id] == addr and username in cls.client_sockets:
                                                    cls.client_sockets[username].send(json.dumps(result_dict).encode('utf-8'))
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
                                    cls.client_sockets[username].send(json.dumps(result_dict).encode('utf-8'))
                                    break

                        # 处理设备/算法/策略信息请求
                        elif 'devicesInfo' in data_decode or 'algorithmsInfo' in data_decode or 'policiesInfo' in data_decode:
                            key = next(iter(data_decode))
                            cls.push_connected_device_info(key)
                            # logging.info(f"接收前端 client 对 base_info 的反馈请求, 将更新结果报文至client")
                        
                        # 处理P2P消息
                        elif 'udp_p2p_msg' in data_decode:
                            logging.info(f"TCP-P2P报文: {data_decode}")
                            to_device_no = data_decode['to_device_no']

                            # 查找设备连接
                            if to_device_no in cls.client_sockets:
                                try:
                                    cls.client_sockets[to_device_no].send(data)  # 原样转发数据
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
                                        cls.client_sockets[to_device_no].send(json.dumps(p2p_msg_dict).encode('utf-8'))
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