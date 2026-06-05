import json
import os

import psutil
import datetime
import socket
import uuid

import yaml

GL_DEVICE_NO = str(uuid.uuid4())

# CWD 无关的本地配置路径：始终相对于本文件定位 Offloading-System/network/constant/，
# 避免依赖“必须从仓库根目录启动”（旧代码用相对路径 'Offloading-System/network/constant/...'，
# 只有当工作目录恰好是仓库根时才找得到）。
_CONSTANT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'network', 'constant')
_LOCAL_CLIENT_YML = os.path.join(_CONSTANT_DIR, 'local_client.yml')


def get_gl_device_no():
    return GL_DEVICE_NO


class DeviceMonitor:
    # 获取Mac地址
    @classmethod
    def get_mac_address(cls):
        mac = uuid.UUID(int=uuid.getnode()).hex[-12:]
        return ":".join([mac[e:e + 2] for e in range(0, 11, 2)])

    # 磁盘 磁盘的使用量等等
    @classmethod
    def get_physical_device(cls):
        # if not os.path.exists('../network/constant/local_client.yml'):
        # 物理机间的联调采用上面注释的方式
        # 多线程间的操作可以采用（1），但要注意device_no的存取不再依赖../network/constant/local_client.yml
        device_no = get_gl_device_no()
        config = {'device_no': device_no}
        # 使用基于本文件定位的绝对路径 + 自动建目录，确保从任意工作目录启动都能运行
        os.makedirs(_CONSTANT_DIR, exist_ok=True)
        with open(_LOCAL_CLIENT_YML, 'w', encoding='utf-8') as f:
            yaml.dump(config, f)
        with open(_LOCAL_CLIENT_YML, 'r', encoding='utf-8') as f:
            config = yaml.load(f.read(), Loader=yaml.FullLoader)
        device_no = config['device_no']
        # 获取主机名
        hostname = socket.gethostname()
        # 获取IP
        ip = socket.gethostbyname(hostname)
        # 系统用户
        users_list = ",".join([u.name for u in psutil.users()])
        # print(u"当前有%s个用户，分别是%s" % (users_count, users_list))
        # 系统启动时间
        start_time = datetime.datetime.fromtimestamp(psutil.boot_time()).strftime("%Y-%m-%d %H:%M:%S")
        sys_info = {"device_name": hostname, "ip_address": ip, "mac": DeviceMonitor.get_mac_address(),
                    "user": users_list,
                    "start_time": start_time}

        # 01.cpu信息
        cpu1 = psutil.cpu_count()
        cpu2 = str(psutil.cpu_percent(interval=1)) + '%'
        # print(u"物理CPU个数 %s" % psutil.cpu_count(logical=False))
        cpu = {"amount": cpu1, "rate": cpu2}

        # 02.内存信息
        mem = psutil.virtual_memory()
        mem_total = round(mem.total / 1024 / 1024 / 1024, 2)
        mem_free = round(mem.free / 1024 / 1024 / 1024, 2)
        mem_percent = str(mem.percent) + '%'
        mem_used = round(mem.used / 1024 / 1024 / 1024, 2)
        memory = {"total": mem_total, "free": mem_free, "rate": mem_percent, "used": mem_used}

        # 03.磁盘信息(磁盘空间使用占比)
        # io = psutil.disk_partitions()
        # disk = []
        # for i in io:
        #     # print(i.device)
        #     o = psutil.disk_usage(i.device)
        #     disk_data = {"disk_name": i.device,
        #                  "total": round(o.total / (1024.0 * 1024.0 * 1024.0), 1),
        #                  "used": round(o.used / (1024.0 * 1024.0 * 1024.0), 1),
        #                  "surplus": round(o.free / (1024.0 * 1024.0 * 1024.0), 1),
        #                  "rate": psutil.disk_usage(i.device).percent}
        #     disk.append(disk_data)

        # 04.网卡，可以得到网卡属性，连接数，当前流量等信息
        net_info = psutil.net_io_counters()
        bytes_sent = '{0:.2f}'.format(net_info.bytes_recv / 1024 / 1024)  # mb
        bytes_rcvd = '{0:.2f}'.format(net_info.bytes_sent / 1024 / 1024)
        net = {"bytes_sent": bytes_sent, "bytes_rcvd": bytes_rcvd}

        # 数据字典
        # data = {"sys": sys_info, "cpu": cpu, "ddr": ddr, "disk": disk, "net": net}
        # data = {"DeviceInfo": sys_info, "cpu": cpu, "memory": memory, "net": net}
        # device_info = {"DeviceInfo": sys_info, "cpu": cpu, "memory": memory, "net": net}
        device_info = {"heart_beat": "heart_beat", "device_no": device_no, "server_no": "#", "used_cpu": cpu["rate"],
                       "core_num": cpu["amount"], "idle_memory": mem_free, "used_memory": mem_used,
                       "total_memory": mem_total, "work_status": 0, "lng": 0.000000, "lat": 0.000000, "order_value": -1,
                       "ip_address": ip}
        return device_info


if __name__ == '__main__':
    data = DeviceMonitor.get_physical_device()
    re = json.dumps(data)
    print(data)
