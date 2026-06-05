import json
import logging
import socket
import time

from config.logging_config import LOG_FORMAT
from config.network_config import HOST, PORT
from network.utils.network_utils import NetworkUtils
from network.protocol.packet_protocol_conversion import PacketProtocolConversion
from predis.implements.device_service_impl import DeviceServiceImpl


class NetworkServer:
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def activate_server(cls, host, port):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # 创建socket对象
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)

        address = addr = (host, port)
        sock.bind(addr)  # 绑定地址和端口

        logging.info('UDP Server on %s:%s...', addr[0], addr[1])

        user = {}  # 存放字典{addr:name}
        while True:
            try:
                data, addr = sock.recvfrom(65535)  # 等待接收客户端消息存放在2个变量data和addr里
                if NetworkUtils.is_json(data.decode('utf-8')):
                    data_decode = json.loads(data.decode('utf-8'))
                    print("data", data_decode)
                    if NetworkUtils.is_dict(data_decode):
                        if 'heart_beat' in data_decode:
                            data_decode['ip_address'] = addr[0]
                            data_decode.update({'port': addr[1]})
                            DeviceServiceImpl.write_device_to_cache(data_decode)
                            logging.info(f"设备信息: {data_decode}")
                            sock.sendto(json.dumps(
                                {"SERVER RESPONSE IP": address[0], "SERVER RESPONSE PORT": address[1]}).encode('utf-8'),
                                        addr)  # 发送Pong消息到client
                        elif 'udp_p2p_msg' in data_decode:  # 通过P2P方式发送UDP报文消息到指定client
                            logging.info(f"UDP-P2P报文: {data_decode}")
                            to_device_ip = data_decode['to_device_ip']
                            to_device_port = data_decode['to_device_port']
                            to_addr = (to_device_ip, to_device_port)
                            sock.sendto(data, to_addr)
                        elif 'udp_bc_msg' in data_decode:  # 通过BROADCAST方式发送UDP报文消息广播到到指定clients群组
                            logging.info(f"UDP-BC报文: {data_decode}")
                            for num in range(len(data_decode['to_device_no_list'])):
                                p2p_msg_dict, to_device_ip, to_device_port = PacketProtocolConversion.change_bc_p2p(
                                    data_dict=data_decode, index=num)
                                to_bc_addr = (to_device_ip, to_device_port)
                                sock.sendto(json.dumps(p2p_msg_dict), to_bc_addr)
                elif NetworkUtils.is_dict(data):
                    pass
            except ConnectionResetError:
                logging.warning('Someone left un-except!')


if __name__ == '__main__':
    NetworkServer.activate_server(HOST, PORT)
