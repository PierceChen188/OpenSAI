"""轻量设备启动器（推荐用它启动 device，而非直接跑 device_client.py）。

为什么需要它：算法执行进程池在 Windows 上用 spawn 启动 worker 时，会**重新 import 入口脚本
(__main__)**。若入口是 device_client.py，worker 就要把它那一堆重型依赖(aioquic/资源控制等)全
重新 import，可能卡死。这里把入口做得极薄——device_client 只在 __main__ 守卫内 import，于是
worker 重新 import 本文件时几乎什么都不做，进程池可正常 spawn。
"""
if __name__ == '__main__':
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from dag_offload.device_client import DeviceClient
    DeviceClient.activate_client()
