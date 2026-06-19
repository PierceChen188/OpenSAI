from flask import Flask, render_template, request, jsonify, Response
# from flask_cors import CORS
import logging
import socket
import threading
import json
import sys
import time
import queue
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 将上一级目录添加到Python的模块搜索路径
from config.network_config import HOST, PORT
from config.logging_config import LOG_FORMAT
from config.sub_task_status import SubTaskStatus
from device.bean.devce_info import DeviceInfo
from network.implements.msg_transfer_tool_impl import MsgTransferToolImpl
from network.utils.network_utils import NetworkUtils
from predis.implements.device_service_impl import DeviceServiceImpl
from task.bean.sub_task_info import SubTaskInfo
from task.interfaces.algorithm_transmission import AlgorithmTransmission
from task.implements.algorithm_transmission_impl import AlgorithmTransmissionImpl

# 全局变量，用于存储从请求中获取的dag数据
data_dag = None
data_lock = threading.Lock()  # 用于线程间安全地访问 global_data 的锁

# 创建一个线程安全的队列，用于在线程之间传递数据
results_queue = queue.Queue()
feedback_queue = queue.Queue()
algorithm_queue = queue.Queue()
devices_queue = queue.Queue()
policy_queue = queue.Queue()

'''
    消息回传前端的逻辑待完善
'''


def msg_broker(sock, addr):
    '''
    消息处理中介，接受来自前端的request 或 后端的feedback，并调用相关函数进行消息传递
    '''
    # 在接收消息前先发送一条消息以便让系统知道所占端口（仅针对特定系统需求，可根据实际情况调整）
    user = {"user_beat": {"name": "user1"}}
    NetworkUtils.send_msg(sock, user)
    logging.info(f"Start receiving messages from {addr}")

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
        if data_decode is None:
            logging.warning("Connection closed by server")
            break

        # ---------------处理对应的dag任务，将result回传给前端-------------
        if data_decode is not None:
            if NetworkUtils.is_dict(data_decode):
                if 'devicesInfo' in data_decode:
                    logging.info(f"[BASE] base devices info is collected!")
                    devices_queue.put(data_decode)
                elif 'algorithmsInfo' in data_decode:
                    logging.info(f"[BASE] base algorithms info is collected!")
                    algorithm_queue.put(data_decode)
                elif 'policiesInfo' in data_decode:
                    logging.info(f"[BASE] base policies info is collected!")
                    policy_queue.put(data_decode)
                elif 'dagResult' in data_decode:
                    logging.info(f"[FINISH] The dag task is done! Congratulation!")
                    results_queue.put(data_decode)
                    print(data_decode)
                elif 'dagUpdateFeedback' in data_decode:
                    logging.info(f"[Update] The dag task is update! Loading!")
                    feedback_queue.put(data_decode)


def generate(used_queue):
    """
        长轮询视图函数：检查队列中是否有新数据，有的话返回数据，没有的话继续等待。
    """
    # while True:
    try:
        # 获取队列中的新数据（阻塞操作，等待数据出现）
        data = used_queue.get(timeout=20)  # 20秒超时
        yield f"data: {json.dumps(data)}\n\n"
    except queue.Empty:
        # 超时后再循环，继续等待数据
        yield "data: {}\n\n"


if __name__ == '__main__':
    '''
        single client 依次执行：CNN训练、 CNN推理两个任务，具体配置参数和执行算法见下方dag_data
    '''
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # TCP: SOCK_DGRAM -> SOCK_STREAM
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
    addr = (HOST, PORT)
    sock.connect(addr)  # TCP需要先建立连接

    broker_msg = threading.Thread(target=msg_broker, args=(sock, addr))
    broker_msg.daemon = True
    broker_msg.start()

    logging.info(f"[START] Send the dag tasks to server!")
    dag_data = {'dagData':
                    {'taskName': 'app1',
                     'taskId': '1',
                     'taskDesc': '协同感知任务',
                     'taskCreateTime': time.time(),
                     'taskPolicy': '策略1',
                     'nodes':
                         [{
                             'id': 1,
                             'name': 'Node1',
                             'form': {
                                 'beforeNodes': [],
                                 'afterNodes': [2],
                                 'taskAlgorithm': 'Algorithm_Pi_Calculation',
                                 'taskVersion': '1.0',
                                 'taskInputInfo': {"inDataPath": './data/',
                                                   "outDataPath": './data/',
                                                   "count_num": 10000},
                                 'details': '描述子任务信息1',
                                 'deviceParam': {},
                                 'directAllocationId': -1,
                                 'containerConfig': {
                                 'cpu_cores': 4.0,
                                 'memory_mb': 4096
                                 }
                             },
                             'result': {"taskResultInfo": '任务结果', 'status': 1, 'delay': 0, 'deviceId': 1001}
                         },
                             {
                                 'id': 2,
                                 'name': 'Node2',
                                 'form': {
                                     'beforeNodes': [1],
                                     'afterNodes': [],
                                     'taskAlgorithm': 'Algorithm_CNN',
                                     'taskVersion': '1.0',
                                     'taskInputInfo': { "inDataPath": '/data/',
                                                        "outDataPath": '/data/',
                                                        "model_name":"model.pth", "scale_num": 100, "count_num": 1000},
                                     'details': '描述子任务信息1',
                                     'deviceParam': {},
                                     'directAllocationId': -1,
                                     'containerConfig': {
                                     'cpu_cores': 2.0,
                                     'memory_mb': 2048
                                     }
                                 },
                                 'result': {"taskResultInfo": '任务结果', 'status': 1, 'delay': 0, 'deviceId': 1001}
                             }
                         ]
                     }}

    # dag_data2 = {'dagData':
    #                 {'taskName': 'app123',
    #                  'taskId': '1',
    #                  'taskDesc': '协同感知任务',
    #                  'taskCreateTime': time.time(),
    #                  'taskPolicy': 1,
    #                  'nodes':
    #                      [{
    #                          'id': 1,
    #                          'name': 'Node1',
    #                          'form': {
    #                              'beforeNodes': [],
    #                              'afterNodes': [2],
    #                              'taskAlgorithm': 'Algorithm_Train',
    #                              'taskVersion': '1.0',
    #                              'taskInputInfo': {"inDataPath": './data/',
    #                                                "outDataPath": './data/',
    #                                                "DataSize": 50,
    #                                                "isExit": 0,
    #                                                "model_name": "model.pth", "scale_num": 100, "num_epochs": 50},
    #                              'details': '描述子任务信息1',
    #                              'directAllocationId': -1,
    #                              'deviceParam': {}
    #                          },
    #                          'result': {"taskResultInfo": '任务结果', 'status': 1, 'delay': 0, 'deviceId': 1001}
    #                      },
    #                          {
    #                              'id': 2,
    #                              'name': 'Node2',
    #                              'form': {
    #                                  'beforeNodes': [1],
    #                                  'afterNodes': [],
    #                                  'taskAlgorithm': 'Algorithm_Random',
    #                                  'taskVersion': '1.0',
    #                                  'taskInputInfo': {"inDataPath": './data/',
    #                                                    "outDataPath": './data/',
    #                                                    "DataSize": 40,
    #                                                    "isExit": 1,
    #                                                    "model_name": "model.pth", "scale_num": 100, "count_num": 1000},
    #                                  'details': '描述子任务信息1',
    #                                  'deviceParam': {},
    #                                  'directAllocationId': -1
    #                              },
    #                              'result': {"taskResultInfo": '任务结果', 'status': 1, 'delay': 0, 'deviceId': 1001}
    #                          }
    #                      ]
    #                  }}
    data_encoded = json.dumps(dag_data)
    NetworkUtils.send_line(sock, data_encoded)
    # 根据任务执行时间适当调整
    time.sleep(128)
    # app.run(debug=True)