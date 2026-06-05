from network.protocol.network_p2p_msg_info import PointToPointMessageInfo


class PacketProtocolConversion:
    @classmethod
    def change_bc_p2p(cls, data_dict: dict, index: int):
        """
        将广播报文信息转换成P2P报文信息
        :param data_dict: 广播报文信息(dict形式)
        :param index: 索引
        :return: P2P报文信息(dict形式)，to_device_ip, to_device_port
        """
        p2p_msg_info = PointToPointMessageInfo()
        p2p_msg_info.set_message_info(from_device_no=data_dict['from_device_no'],
                                      from_device_ip=data_dict['from_device_ip'],
                                      from_device_port=data_dict['from_device_port'],
                                      to_device_no=data_dict['to_device_no_list'][index],
                                      to_device_ip=data_dict['to_device_ip_list'][index],
                                      to_device_port=data_dict['to_device_port_list'][index], msg=data_dict['msg'],
                                      msg_type=data_dict['msg_type'], is_slice=data_dict['is_slice'],
                                      slice_no=data_dict['slice_no'], slice_len=data_dict['slice_len'])
        to_device_ip = data_dict['to_device_ip_list'][index]
        to_device_port = data_dict['to_device_port_list'][index]
        return p2p_msg_info.get_message_by_dict(), to_device_ip, to_device_port
