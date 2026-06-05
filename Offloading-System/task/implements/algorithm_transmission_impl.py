import logging

from config.logging_config import LOG_FORMAT
from config.sub_task_status import SubTaskStatus
from device.bean.devce_info import DeviceInfo
from network.implements.msg_transfer_tool_impl import MsgTransferToolImpl
from predis.implements.device_service_impl import DeviceServiceImpl
from task.bean.sub_task_info import SubTaskInfo
from task.interfaces.algorithm_transmission import AlgorithmTransmission


class AlgorithmTransmissionImpl(AlgorithmTransmission):
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    @classmethod
    def send_algo_no_data(cls, sub_task_info: SubTaskInfo, device_info: DeviceInfo, task_id: str, sub_task_id: str,
                          device_no: str, algo_name: str, algo_version: str, param_dict: dict) -> bool:
        logging.info(f"发送任务编号: {task_id}")
        logging.info(f"发送子任务到编号: {sub_task_id}")
        logging.info(f"发送消息到设备: {device_no}")
        logging.info(f"发送算法名称: {algo_name}")
        logging.info(f"发送算法版本: {algo_version}")
        logging.info(f"算法参数列表: {param_dict}")
        msg_dict = {'task_id': task_id, 'sub_task_id': sub_task_id, 'device_no': device_no, 'algo_name': algo_name,
                    'algo_version': algo_version, 'param_dict': param_dict}
        # TCP模式：保持原有调用方式，内部会使用TCP发送
        MsgTransferToolImpl.send_p2p_msg(msg=msg_dict, msg_type="algo", from_device_no="#", to_device_no=device_no,
                                         to_addr=(device_info.get_ip_address(), device_info.get_port()))
        return True

    @classmethod
    def send_algo_with_data(cls, sub_task_info: SubTaskInfo, device_info: DeviceInfo, task_id: str, sub_task_id: str,
                            device_no: str, algo_name: str, algo_version: str, param_dict: dict,
                            data_dict: dict) -> bool:
        logging.info(f"发送任务编号: {task_id}")
        logging.info(f"发送子任务到编号: {sub_task_id}")
        logging.info(f"发送消息到设备: {device_no}")
        logging.info(f"发送算法名称: {algo_name}")
        logging.info(f"发送算法版本: {algo_version}")
        logging.info(f"算法参数列表: {param_dict}")
        logging.info(f"数据列表: {data_dict}")
        msg_dict = {'task_id': task_id, 'sub_task_id': sub_task_id, 'device_no': device_no, 'algo_name': algo_name,
                    'algo_version': algo_version, 'param_dict': param_dict, 'data_dict': data_dict}
        # TCP模式：保持原有调用方式，内部会使用TCP发送
        MsgTransferToolImpl.send_p2p_msg(msg=msg_dict, msg_type="algo", from_device_no="#", to_device_no=device_no,
                                         to_addr=(device_info.get_ip_address(), device_info.get_port()))
        return True


if __name__ == '__main__':
    sub_task_info = SubTaskInfo()
    sub_task_info.set_sub_task_info(task_id="1", sub_task_id="0", sub_task_name="cnn", sub_task_version="#2023.9.8.001",
                                    sub_task_location="http://www.test.com/task/", task_algorithm="Resnet34",
                                    sub_task_data_list=["ftp://jbb_1", "ftp://jbb_2"], sub_task_result_info=None,
                                    sub_task_status=SubTaskStatus.WORK, sub_task_count=10)
    device_info_list = DeviceServiceImpl.read_device_info_random(1)
    keys_list = list(device_info_list.keys())
    key = keys_list[0]
    device_dict = device_info_list[key]
    device_info = DeviceInfo()
    device_info.set_device_from_dict(device_dict)
    print("get it!", device_info, sub_task_info)
    AlgorithmTransmissionImpl.send_algo_no_data(sub_task_info, device_info, task_id=sub_task_info.get_task_id(),
                                                sub_task_id=sub_task_info.get_sub_task_id(),
                                                device_no=device_info.get_device_no(),
                                                algo_name=sub_task_info.get_task_algorithm(),
                                                algo_version=sub_task_info.get_sub_task_version(),
                                                param_dict={'parm_conv': 3, 'param_pooling': 2, 'param_batch_size': 10})