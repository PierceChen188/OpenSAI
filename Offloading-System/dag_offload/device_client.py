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

name = "DEVICE_" + get_gl_device_no()


class DeviceClient:
    # 添加容器管理器实例（支持容器化任务执行和复用）
    container_controller = ResourceControlInterface()
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

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
            algorithm_name = "device.implements." + data_decode['msg']['algo_name']
            # 执行算法
            result_1 = cls.execute_task(
                algorithm_name, 
                data_decode['msg']['param_dict'],
                data_decode['msg'].get('data_dict', {})
            )

            logging.info(f"任务计算结果： {result_1}")
            
            # 构建响应消息
            msg_dict = {'task_id': data_decode['msg']['task_id'],
                        'sub_task_id': data_decode['msg']['sub_task_id'],
                        'device_no': data_decode['msg']['device_no'],
                        'finished_time': "",
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
                    sock.send(json.dumps(rsp_dict).encode('utf-8'))
                    logging.info(f"Sent task result back to server: {rsp_dict}")
            except (socket.error, OSError) as e:
                logging.warning(f"Socket error during task response sending: {e}")

        except Exception as e:
            logging.error(f"Error processing task: {e}")

    @classmethod
    def receive_msg(cls, sock, addr):
        '''
        接受来自server分发的任务，并调用相关函数进行处理
        '''
        # 发送设备初始化消息，格式化为JSON以便服务器识别
        init_msg = {'heart_beat': True, 'device_name': name, 'device_no': get_gl_device_no()}
        sock.send(json.dumps(init_msg).encode('utf-8'))  # TCP: sendto -> send
        logging.info(f"Sent device initialization message to {addr}")

        while True:
            try:
                data = sock.recv(65535)  # TCP: recvfrom -> recv
                if not data:  # 如果没有接收到数据，说明连接可能已断开
                    logging.warning("Connection closed by server")
                    break
                logging.info(data.decode('utf-8'))
            except (socket.error, OSError) as e:
                # 捕获 socket 相关异常，如 socket 被关闭等
                logging.warning(f"[EXIT] The receive_msg thread is over, Socket error occurred: {e}")
                break

            # ---------------处理对应的dag任务-------------
            if NetworkUtils.is_json(data.decode('utf-8')):
                data_decode = json.loads(data.decode('utf-8'))
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
                elif NetworkUtils.is_dict(data_decode) and 'SERVER RESPONSE IP' in data_decode:
                    # 服务器响应心跳
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
        device_info = DeviceMonitor.get_physical_device()
        # 确保心跳包中包含heart_beat标识和device_no字段
        device_info['heart_beat'] = True
        device_info['device_no'] = get_gl_device_no()
        print(device_info)

        while True:
            # message = "Heartbeat"
            device_info.update(cls.calculate_network_metrics(sock, addr))
            device_info_json = json.dumps(device_info)
            # print(device_info_json)
            message = device_info_json
            try:
                sock.send(message.encode('utf-8'))  # TCP: sendto -> send
                logging.info(f"Sent heartbeat to server: {message}")
            except (socket.error, OSError) as e:
                # 捕获 socket 相关异常，如 socket 被关闭等
                logging.info(f"[EXIT] The receive_msg thread is over, Socket error occurred: {e}")
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