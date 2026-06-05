import cv2

from device.bean.devce_info import DeviceInfo
from network.implements.msg_transfer_tool_impl import MsgTransferToolImpl
from predis.implements.device_service_impl import DeviceServiceImpl

if __name__ == '__main__':
    device_info_list = DeviceServiceImpl.read_device_info_random(1)
    keys_list = list(device_info_list.keys())
    key = keys_list[0]
    device_dict = device_info_list[key]
    device_info = DeviceInfo()
    device_info.set_device_from_dict(device_dict)
    # 创建一个VideoCapture对象来读取视频文件
    cap = cv2.VideoCapture('video.mp4')

    # 检查视频是否成功打开
    if not cap.isOpened():
        print("Could not open video")
        exit()

    while True:
        # 逐帧读取视频
        ret, frame = cap.read()
        if not ret:
            break

            # 在这里，你可以处理每一帧，例如，将其发送到UDP服务器
        # 发送帧的代码将在这里

        # 显示帧（可选）
        # 创建名为'Local-Frame'的窗口并设置属性为可调整大小
        cv2.namedWindow('Local-Frame', cv2.WINDOW_NORMAL)
        # 获取显示器桌面大小
        # 设置窗口大小为显示器桌面大小的1/2
        window_width = int(1024 / 2)
        window_height = int(728 / 2)
        cv2.resizeWindow('Local-Frame', window_width, window_height)
        cv2.imshow('Local-Frame', frame)
        _, jpeg_image = cv2.imencode('.jpg', frame)
        data = jpeg_image.tobytes()
        MsgTransferToolImpl.send_p2p_big_msg(msg=data, msg_type="mp4", from_device_no="#",
                                             to_device_no=device_info.get_device_no(),
                                             to_addr=(device_info.get_ip_address(), device_info.get_port()))

        # 如果按下q键，则退出循环
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

        # 释放VideoCapture对象并关闭所有窗口
    cap.release()
    cv2.destroyAllWindows()
    # img = cv2.imread('image.jpg')
    # data = img.tobytes()
    # MsgTransferToolImpl.send_p2p_big_msg(msg=data, msg_type="mp4", from_device_no="#",
    #                                      to_device_no=device_info.get_device_no(),
    #                                      to_addr=(device_info.get_ip_address(), device_info.get_port()))
