class DeviceInfo:
    def __init__(self):
        # 设备编号
        self.__device_no = ""
        # 连接主机编号
        self.__server_no = ""
        # cpu使用情况
        self.__used_cpu = ""
        # 内核数量
        self.__core_num = 0
        # 空闲内存
        self.__idle_memory = 0.000000
        # 已使用内存
        self.__used_memory = 0.000000
        # 总内存
        self.__total_memory = 0.000000
        # 工作状态
        self.__work_status = 0
        # 经度
        self.__lng = 0.000000
        # 纬度
        self.__lat = 0.000000
        # 设备上下线
        self.__order_value = -1
        # 局域网内IP地址
        self.__ip_address = ""
        # 网络端口号
        self.__port = -1
        # 吞吐量
        self.__throughput = 0.000000
        # 带宽
        self.__bandwidth = 0.000000
        # 网络使用量
        self.__network_usage = 0.000000
        # 网络延迟
        self.__delay = 0.000000

    def set_device_info(self, device_no: str, server_no: str, used_cpu: str, core_num: int, idle_memory: float,
                        used_memory: float, total_memory: float, work_status: int, lng: float, lat: float,
                        order_value: int, ip_address: str, port: int, throughput: float, bandwidth: float,
                        network_usage: float, delay: float):
        self.__device_no = device_no
        self.__server_no = server_no
        self.__used_cpu = used_cpu
        self.__core_num = core_num
        self.__idle_memory = idle_memory
        self.__used_memory = used_memory
        self.__total_memory = total_memory
        self.__work_status = work_status
        self.__lng = lng
        self.__lat = lat
        self.__order_value = order_value
        self.__ip_address = ip_address
        self.__port = port
        self.__throughput = throughput
        self.__bandwidth = bandwidth
        self.__network_usage = network_usage
        self.__delay = delay

    def set_device_from_dict(self, device_info_dict: dict):
        self.__device_no = device_info_dict["device_no"]
        self.__server_no = device_info_dict["server_no"]
        self.__used_cpu = device_info_dict["used_cpu"]
        self.__core_num = device_info_dict["core_num"]
        self.__idle_memory = device_info_dict["idle_memory"]
        self.__used_memory = device_info_dict["used_memory"]
        self.__total_memory = device_info_dict["total_memory"]
        self.__work_status = device_info_dict["work_status"]
        self.__lng = device_info_dict["lng"]
        self.__lat = device_info_dict["lat"]
        self.__order_value = device_info_dict["order_value"]
        self.__ip_address = device_info_dict["ip_address"]
        self.__port = device_info_dict['port']
        self.__throughput = device_info_dict["throughput"]
        self.__bandwidth = device_info_dict["bandwidth"]
        self.__network_usage = device_info_dict["network_usage"]
        self.__delay = device_info_dict["delay"]

    def get_device_by_dict(self) -> dict:
        device_info_dict = {"device_no": self.get_device_no(), "server_no": self.get_server_no(),
                            "used_cpu": self.get_used_cpu(), "core_num": self.get_core_num(),
                            "idle_memory": self.get_idle_memory(), "used_memory": self.get_used_memory(),
                            "total_memory": self.get_total_memory(), "work_status": self.get_work_status(),
                            "lng": self.get_lng(), "lat": self.get_lat(), "order_value": self.get_order_value(),
                            "ip_address": self.get_ip_address(), "port": self.get_port(),
                            "throughput": self.get_throughput(), "bandwidth": self.get_bandwidth(),
                            "network_usage": self.get_network_usage(),
                            "delay": self.get_delay()}
        return device_info_dict

    def set_device_no(self, device_no: str):
        self.__device_no = device_no

    def get_device_no(self) -> str:
        return self.__device_no

    def set_server_no(self, server_no: str):
        self.__server_no = server_no

    def get_server_no(self):
        return self.__server_no

    def set_used_cpu(self, used_cpu: str):
        self.__used_cpu = used_cpu

    def get_used_cpu(self) -> str:
        return self.__used_cpu

    def set_core_num(self, core_num: int):
        self.__core_num = core_num

    def get_core_num(self) -> int:
        return self.__core_num

    def set_idle_memory(self, idle_memory: float):
        self.__idle_memory = idle_memory

    def get_idle_memory(self) -> float:
        return self.__idle_memory

    def set_used_memory(self, used_memory: float):
        self.__used_memory = used_memory

    def get_used_memory(self) -> float:
        return self.__used_memory

    def set_total_memory(self, total_memory: float):
        self.__total_memory = total_memory

    def get_total_memory(self) -> float:
        return self.__total_memory

    def set_work_status(self, work_status: int):
        self.__work_status = work_status

    def get_work_status(self) -> int:
        return self.__work_status

    def set_lng(self, lng: float):
        self.__lng = lng

    def get_lng(self) -> float:
        return self.__lng

    def set_lat(self, lat: float):
        self.__lat = lat

    def get_lat(self) -> float:
        return self.__lat

    def set_order_value(self, order_value: int):
        self.__order_value = order_value

    def get_order_value(self) -> int:
        return self.__order_value

    def set_ip_address(self, ip_address: str):
        self.__ip_address = ip_address

    def get_ip_address(self) -> str:
        return self.__ip_address

    def set_port(self, port: int):
        self.__port = port

    def get_port(self) -> int:
        return self.__port

    def set_throughput(self, throughput: float):
        self.__throughput = throughput

    def get_throughput(self) -> float:
        return self.__throughput

    def set_bandwidth(self, bandwidth: float):
        self.__bandwidth = bandwidth

    def get_bandwidth(self) -> float:
        return self.__bandwidth

    def set_network_usage(self, network_usage: float):
        self.__network_usage = network_usage

    def get_network_usage(self) -> float:
        return self.__network_usage

    def set_delay(self, delay: float):
        self.__delay = delay

    def get_delay(self) -> float:
        return self.__delay

    def __str__(self):
        return f"DeviceInfo(device_no={self.__device_no}, server_no={self.__server_no}, used_cpu={self.__used_cpu}, " \
               f"core_num={self.__core_num}, idle_memory={self.__idle_memory}, used_memory={self.__used_memory}, " \
               f"total_memory={self.__total_memory}, work_status={self.__work_status}, lng={self.__lng}, " \
               f"lat={self.__lat}, order_value={self.__order_value}, ip_address={self.__ip_address}, port={self.__port}" \
               f"throughput={self.__throughput}, bandwidth={self.__bandwidth},network_usage={self.__network_usage}, " \
               f"delay={self.__delay})"
