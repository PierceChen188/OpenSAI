import base64
import json
import logging
import math
import os
import time
from typing import Tuple, List

import cv2
import numpy as np
import yaml

from config.client_boot import get_client_socket
from config.network_config import HOST, PORT
from network.interfaces.msg_transfer_tool import MsgTransferTool
from network.protocol.network_broadcast_msg_info import BroadCastMessageInfo
from network.protocol.network_p2p_msg_info import PointToPointMessageInfo
from predis.implements.device_service_impl import DeviceServiceImpl
from task.implements import buff


class MsgTransferToolImpl(MsgTransferTool):

    @classmethod
    def send_p2p_msg(cls, msg: dict, msg_type: str, from_device_no: str, to_device_no: str,
                     to_addr: Tuple[str, int]) -> bool:
        p2p_msg_info = PointToPointMessageInfo()
        cur_path = os.path.dirname(os.path.realpath(__file__))
        base_path = cur_path[0:len(cur_path) - 10]
        yaml_path = os.path.join(base_path, "constant\\local_client.yml")
        file = open(yaml_path, 'r', encoding='utf-8')
        config = yaml.load(file.read(), Loader=yaml.FullLoader)
        file.close()
        device_info = DeviceServiceImpl.query_device_info_by_id(config['device_no'])
        LOCAL_CLIENT_IP = device_info['ip_address']
        LOCAL_CLIENT_PORT = device_info['port']
        p2p_msg_info.set_message_info(from_device_no=from_device_no, from_device_ip=LOCAL_CLIENT_IP,
                                      from_device_port=LOCAL_CLIENT_PORT, to_device_no=to_device_no,
                                      to_device_ip=to_addr[0],
                                      to_device_port=to_addr[1], msg=msg, msg_type=msg_type, is_slice=False,
                                      slice_no=-1, slice_len=0)
        msg_info_dict = p2p_msg_info.get_message_by_dict()
        msg_info_json = json.dumps(msg_info_dict)
        get_client_socket().send(msg_info_json.encode('utf-8'))  # 修改：使用延迟连接
        return True

    @classmethod
    def send_bc_msg(cls, msg: dict, msg_type: str, from_device_no: str, to_device_no_list: List[str],
                    to_addr_list: List[Tuple[str, int]]) -> bool:
        bc_msg_info = BroadCastMessageInfo()
        to_ip_list = []
        to_port_list = []
        for to_addr in to_addr_list:
            to_ip_list.append(to_addr[0])
            to_port_list.append(to_addr[1])
        cur_path = os.getcwd()
        base_path = cur_path[0:len(cur_path) - 10]
        yaml_path = os.path.join(base_path, "constant\\local_client.yml")
        file = open(yaml_path, 'r', encoding='utf-8')
        config = yaml.load(file.read(), Loader=yaml.FullLoader)
        file.close()
        device_info = DeviceServiceImpl.query_device_info_by_id(config['device_no'])
        LOCAL_CLIENT_IP = device_info['ip_address']
        LOCAL_CLIENT_PORT = device_info['port']
        bc_msg_info.set_message_info(from_device_no=from_device_no, from_device_ip=LOCAL_CLIENT_IP,
                                     from_device_port=LOCAL_CLIENT_PORT, to_device_no_list=to_device_no_list,
                                     to_device_ip_list=to_ip_list, to_device_port_list=to_port_list, msg=msg,
                                     msg_type=msg_type, slice_no=-1, is_slice=False, slice_len=0)
        msg_info_dict = bc_msg_info.get_message_by_dict()
        msg_info_json = json.dumps(msg_info_dict)
        get_client_socket().send(msg_info_json.encode('utf-8'))  # 修改：使用延迟连接
        return True

    @classmethod
    def send_p2p_big_msg(cls, msg: bytes, msg_type: str, from_device_no: str, to_device_no: str,
                         to_addr: Tuple[str, int]) -> bool:
        p2p_msg_info = PointToPointMessageInfo()
        cur_path = os.path.dirname(os.path.realpath(__file__))
        base_path = cur_path[0:len(cur_path) - 10]
        yaml_path = os.path.join(base_path, "constant\\local_client.yml")
        file = open(yaml_path, 'r', encoding='utf-8')
        config = yaml.load(file.read(), Loader=yaml.FullLoader)
        file.close()
        device_info = DeviceServiceImpl.query_device_info_by_id(config['device_no'])
        LOCAL_CLIENT_IP = device_info['ip_address']
        LOCAL_CLIENT_PORT = device_info['port']
        pack_size = 40960
        send_times = math.ceil(len(msg) / pack_size)
        for count in range(send_times):
            if count < send_times - 1:
                p2p_msg_info.set_message_info(from_device_no=from_device_no, from_device_ip=LOCAL_CLIENT_IP,
                                              from_device_port=LOCAL_CLIENT_PORT, to_device_no=to_device_no,
                                              to_device_ip=to_addr[0],
                                              to_device_port=to_addr[1],
                                              msg=base64.b64encode(msg[pack_size * count:pack_size * (count + 1)]).decode('utf-8'), msg_type=msg_type,
                                              is_slice=True,
                                              slice_no=count, slice_len=send_times)
                msg_info_dict = p2p_msg_info.get_message_by_dict()
                msg_info_json = json.dumps(msg_info_dict)
                logging.info(msg_info_json)
                get_client_socket().send(msg_info_json.encode('utf-8'))  # 修改：使用延迟连接
            else:
                p2p_msg_info.set_message_info(from_device_no=from_device_no, from_device_ip=LOCAL_CLIENT_IP,
                                              from_device_port=LOCAL_CLIENT_PORT, to_device_no=to_device_no,
                                              to_device_ip=to_addr[0],
                                              to_device_port=to_addr[1],
                                              msg=base64.b64encode(msg[pack_size * count:]).decode('utf-8'), msg_type=msg_type,
                                              is_slice=True,
                                              slice_no=count, slice_len=send_times)
                msg_info_dict = p2p_msg_info.get_message_by_dict()
                msg_info_json = json.dumps(msg_info_dict)
                logging.info(msg_info_json)
                get_client_socket().send(msg_info_json.encode('utf-8'))  # 修改：使用延迟连接
        return True

    @classmethod
    def send_bc_big_msg(cls, msg: bytes, msg_type: str, from_device_no: str, to_device_no_list: List[str],
                        to_addr_list: List[Tuple[str, int]]) -> bool:
        bc_msg_info = BroadCastMessageInfo()
        to_ip_list = []
        to_port_list = []
        for to_addr in to_addr_list:
            to_ip_list.append(to_addr[0])
            to_port_list.append(to_addr[1])
        cur_path = os.getcwd()
        base_path = cur_path[0:len(cur_path) - 10]
        yaml_path = os.path.join(base_path, "constant\\local_client.yml")
        file = open(yaml_path, 'r', encoding='utf-8')
        config = yaml.load(file.read(), Loader=yaml.FullLoader)
        file.close()
        device_info = DeviceServiceImpl.query_device_info_by_id(config['device_no'])
        LOCAL_CLIENT_IP = device_info['ip_address']
        LOCAL_CLIENT_PORT = device_info['port']
        pack_size = 40960
        send_times = math.ceil(len(msg) / pack_size)
        for count in range(send_times):
            if count < send_times - 1:
                bc_msg_info.set_message_info(from_device_no=from_device_no, from_device_ip=LOCAL_CLIENT_IP,
                                             from_device_port=LOCAL_CLIENT_PORT, to_device_no_list=to_device_no_list,
                                             to_device_ip_list=to_ip_list, to_device_port_list=to_port_list,
                                             msg=base64.b64encode(msg[pack_size * count:pack_size * (count + 1)]).decode('utf-8'),
                                             msg_type=msg_type, slice_no=count, is_slice=True, slice_len=send_times)
                msg_info_dict = bc_msg_info.get_message_by_dict()
                msg_info_json = json.dumps(msg_info_dict)
                get_client_socket().send(msg_info_json.encode('utf-8'))  # 修改：使用延迟连接
            else:
                bc_msg_info.set_message_info(from_device_no=from_device_no, from_device_ip=LOCAL_CLIENT_IP,
                                             from_device_port=LOCAL_CLIENT_PORT, to_device_no_list=to_device_no_list,
                                             to_device_ip_list=to_ip_list, to_device_port_list=to_port_list,
                                             msg=base64.b64encode(msg[pack_size * count:]).decode('utf-8'),
                                             msg_type=msg_type, slice_no=count, is_slice=True, slice_len=send_times)
                msg_info_dict = bc_msg_info.get_message_by_dict()
                msg_info_json = json.dumps(msg_info_dict)
                get_client_socket().send(msg_info_json.encode('utf-8'))  # 修改：使用延迟连接
        return True

    def receive_p2p_msg(self, msg: dict) -> bool:
        return super().receive_p2p_msg(msg)

    @classmethod
    def receive_p2p_big_msg(cls, msg: dict) -> bool:
        buff.data_total += base64.b64decode((json.dumps(msg['msg']).encode('utf-8')))
        logging.info('received big data!')
        if msg['slice_no'] == msg['slice_len'] - 1:
            nparr = np.fromstring(buff.data_total, np.uint8)
            buff.data_total = b''
            img_decode = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            # 创建名为'Remote-Frame'的窗口并设置属性为可调整大小
            cv2.namedWindow('Remote-Frame', cv2.WINDOW_NORMAL)
            # 获取显示器桌面大小
            # 设置窗口大小为显示器桌面大小的1/2
            window_width = int(1024 / 2)
            window_height = int(728 / 2)
            cv2.resizeWindow('Remote-Frame', window_width, window_height)
            cv2.imshow('Remote-Frame', img_decode)
            # 保存截图
            # cv2.imwrite('D:\\123\\{}.jpg'.format(time.perf_counter()), img_decode)
            cv2.waitKey(1)  # 等待一段时间，例如1毫秒，你可以根据需要调整这个值
        # cv2.destroyAllWindows()  # 销毁所有窗口，以便下一次显示新图像
        return True