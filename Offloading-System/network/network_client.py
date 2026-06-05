import json
import logging
import threading
import time
import socket
from config.client_boot import CLIENT_SOCK
from config.logging_config import LOG_FORMAT
from config.network_config import HOST, PORT
from device.device_monitor import DeviceMonitor, get_gl_device_no
from network.implements.msg_transfer_tool_impl import MsgTransferToolImpl
from network.utils.network_utils import NetworkUtils

name = "DEVICE_" + get_gl_device_no()


class NetworkClient:
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def receive_msg(cls, sock, addr):
        '''
        一个UDP连接在接收消息前必须要让系统知道所占端口
        也就是需要send一次，否则win下会报错
        '''
        # 在接收消息前先发送一条消息以便让系统知道所占端口（仅针对特定系统需求，可根据实际情况调整）
        sock.sendto(name.encode('utf-8'), addr)
        logging.info(f"Start receiving messages from {addr}")

        while True:
            try:
                data, addr = sock.recvfrom(65535)  # 接收数据
                logging.info(data.decode('utf-8'))
            except (socket.error, OSError) as e:
            # 捕获 socket 相关异常，如 socket 被关闭等
                logging.info(f"[EXIT] The receive_msg thread is over, Socket error occurred: {e}")
                break

            if NetworkUtils.is_json(data.decode('utf-8')):
                data_decode = json.loads(data.decode('utf-8'))
                if NetworkUtils.is_dict(data_decode) and 'udp_p2p_msg' in data_decode:
                    logging.info(f"Received UDP-P2P message: {data_decode}")
                    MsgTransferToolImpl.receive_p2p_big_msg(data_decode)



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
            print(name)
            data = message.encode('utf-8')
            sock.sendto(data, addr)
            logging.info(f"send msg {string} to {addr}")
            if string.lower() == 'EXIT'.lower():
                sock.close()
                break
        logging.info(f"[EXIT] The sending thread is over")

    @classmethod
    def send_request(cls, sock, addr):
        '''
          发送dag请求至server，获取对应的调度决策；
          参数：
            sock：定义一个实例化socket对象
            server：传递的服务器IP和端口
        '''
        while True:
            string = input('')
            message = name + ' : ' + string
            print(name)
            data = message.encode('utf-8')
            try:
                sock.sendto(data, addr)
            except (socket.error, OSError) as e:
                # 捕获 socket 相关异常，如 socket 被关闭等
                logging.info(f"[EXIT] The send_request thread is over, Socket error occurred: {e}")
                break

            logging.info(f"send msg {string} to {addr}")
            if string.lower() == 'EXIT'.lower():
                sock.close()
                break
        logging.info(f"[EXIT] The sending thread is over")

    @classmethod
    def send_heartbeat(cls, sock, addr):
        while True:
            # message = "Heartbeat"
            device_info = DeviceMonitor.get_physical_device()
            device_info.update(cls.calculate_network_metrics(sock, addr))
            device_info_json = json.dumps(device_info)
            message = device_info_json
            try:
                sock.sendto(message.encode('utf-8'), addr)
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
        sock = CLIENT_SOCK
        server = (HOST, PORT)
        logging.info("-----欢迎使用OpenSAI Offloading System,退出连接请输入'EXIT(不分大小写)'-----")
        receive_msg_thread = threading.Thread(target=cls.receive_msg, args=(sock, server), daemon=True)
        receive_msg_thread.start()

        ts = threading.Thread(target=cls.send, args=(sock, server))
        ts.start()

        heartbeat_thread = threading.Thread(target=cls.send_heartbeat, args=(sock, server))
        heartbeat_thread.start()

        # 不阻塞主线程，让三个线程在后台运行
        receive_msg_thread.join()
        ts.join()
        heartbeat_thread.join()

        sock.close()


if __name__ == '__main__':
    NetworkClient.activate_client()
