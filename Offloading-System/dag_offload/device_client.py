import json
import logging
import threading
import time
import socket
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 将上一级目录添加到Python的模块搜索路径
# 添加容器管理接口导入
from device.interfaces.resource_control_interface import ResourceControlInterface
from config.logging_config import LOG_FORMAT
from config.network_config import HOST, PORT
from device.device_monitor import DeviceMonitor, get_gl_device_no
from device.implements.actuator_impl import ActuatorImpl
from network.utils.network_utils import NetworkUtils
from network.dataplane.quic_server import ArtifactServer
from network.dataplane.quic_client import fetch_blob
from config.dataplane_config import DATA_PORT, ARTIFACTS_DIR

name = "DEVICE_" + get_gl_device_no()


class DeviceClient:
    # 添加容器管理器实例（支持容器化任务执行和复用）
    container_controller = ResourceControlInterface()
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    # 数据面：本机产物服务器（在 activate_client 中启动）
    artifact_server = None

    # —— 监测信号（线程间共享，由心跳线程统一上报到 server/Redis）——
    running_tasks = 0                 # 当前正在执行的任务数 -> work_status
    running_lock = threading.Lock()
    last_hb_sent = 0.0                # 最近一次心跳发送时刻（测 RTT 用）
    last_rtt_ms = 0.0                 # 最近一次服务器↔设备往返时延(ms)
    observed_links = {}               # 链路遥测(被动+主动): "src_ip:data_port" -> {mbps,last_ms,bytes,samples}
    links_lock = threading.Lock()
    peers = []                        # 在线对端清单(来自心跳ACK): [{device_no,ip_address,data_port}]
    PROBE_ID = '__probe__'            # 主动探测用的固定 artifact id
    PROBE_INTERVAL = int(os.environ.get('OPENSAI_PROBE_INTERVAL', '20'))  # 秒；主动探测间隔

    @classmethod
    # def execute_task(cls, algorithm_name, param_dict, data_dict):
    #     result = ActuatorImpl.run_actuator(algorithm_name, param_dict, data_dict)
    #     return result
    def execute_task(cls, algorithm_name, param_dict, data_dict):
        try:
            # 从 algorithm_name 中提取纯算法名
            if '.' in algorithm_name:
                pure_algorithm_name = algorithm_name.split('.')[-1]
            else:
                pure_algorithm_name = algorithm_name

            # 从 param_dict 读取容器资源配置（来自 DAG 图）
            container_config = param_dict.get('containerConfig', {})
            cpu_cores = container_config.get('cpu_cores', 2.0)  # 默认2核
            memory_mb = container_config.get('memory_mb', 2048)  # 默认2GB

            logging.info(f"使用容器执行任务: {pure_algorithm_name}, CPU={cpu_cores}核, 内存={memory_mb}MB")
            
            # 创建新容器执行任务（等待结果）
            result_dict = cls.container_controller.start_task(
                task_name=pure_algorithm_name,
                cpu_cores=cpu_cores,
                memory_mb=memory_mb,
                param_dict=param_dict,  
                data_dict=data_dict      
            )
            
            # 提取执行结果
            if result_dict.get('result', {}).get('status') == 1:
                # 任务执行成功
                task_result = result_dict['result'].get('taskResultInfo', '任务执行完成')
                logging.info(f"容器任务执行成功: {task_result}")
                result = task_result
            else:
                # 容器执行失败（如本机无 Docker / 镜像不可用）——按设计降级为本地直接执行。
                # 注意：start_task 会吞掉自身异常并返回 status!=1，因此这里不能只靠 except 兜底，
                # 必须在 status!=1 时显式降级，否则无 Docker 时每个任务都会返回失败串。
                error_msg = result_dict.get('result', {}).get('taskResultInfo', '任务执行失败')
                logging.warning(f"容器任务执行失败({error_msg})，降级使用本地直接执行")
                result = ActuatorImpl.run_actuator(algorithm_name, param_dict, data_dict)

            return result
            
        except Exception as e:
            # 容器执行异常，降级使用本地直接执行
            logging.error(f"容器执行任务异常: {e}")
            logging.warning("降级使用本地直接执行方式")
            result = ActuatorImpl.run_actuator(algorithm_name, param_dict, data_dict)
            return result

    @classmethod
    def process_task(cls, sock, data_decode):
        """
        在独立线程中处理单个任务
        """
        try:
            with cls.running_lock:
                cls.running_tasks += 1
            msg = data_decode['msg']
            task_id = msg['task_id']
            node_id = msg['sub_task_id']
            algorithm_name = "device.implements." + msg['algo_name']
            param_dict = dict(msg.get('param_dict') or {})

            # ---- 数据面：拉取前驱产物，注入 _artifact_in（pred_id -> 本地文件） ----
            artifact_in = {}
            for art in (msg.get('input_artifacts') or []):
                pred_id = art.get('pred_id')
                # locality 优化：若该前驱产物就在本机（同机放置），直接本地读，跳过 QUIC 传输。
                here = cls.artifact_server.local_path(art['artifact_id']) if cls.artifact_server else None
                if here:
                    artifact_in[pred_id] = here
                    logging.info(f"[dataplane] 前驱产物 {art['artifact_id']} 在本机，直接本地读取（跳过传输）")
                    continue
                local = os.path.join(ARTIFACTS_DIR, f"{task_id}_{pred_id}.in.blob")
                try:
                    # rate_mbps 由 server 在 input_artifacts 里给出（适配优化按任务分配带宽，T5.3）；None=不限。
                    stats = fetch_blob(art['src_ip'], int(art['src_port']), art['artifact_id'], local,
                                       rate_mbps=art.get('rate_mbps'))
                    artifact_in[pred_id] = local
                    cls._record_link(art['src_ip'], art['src_port'], stats)
                    logging.info(f"[dataplane] 拉取前驱产物 {art['artifact_id']} 自 "
                                 f"{art['src_ip']}:{art['src_port']} "
                                 f"({stats['bytes']} bytes, {stats['elapsed']:.3f}s)")
                except Exception as e:
                    logging.error(f"[dataplane] 拉取前驱产物失败 {art}: {e}")
            if artifact_in:
                param_dict['_artifact_in'] = artifact_in

            # ---- 数据面：本节点产出路径，注入 _artifact_out ----
            artifact_out = os.path.join(ARTIFACTS_DIR, f"{task_id}_{node_id}.blob")
            param_dict['_artifact_out'] = artifact_out

            # 执行算法
            result_1 = cls.execute_task(
                algorithm_name,
                param_dict,
                msg.get('data_dict', {})
            )

            # ---- 数据面：登记本节点产出，供后继 device 拉取 ----
            out_bytes = 0
            if cls.artifact_server is not None and os.path.exists(artifact_out):
                cls.artifact_server.register(f"{task_id}:{node_id}", artifact_out)
                try:
                    out_bytes = os.path.getsize(artifact_out)   # 上报产出大小，供 server 自适应放置(T3.2)
                except OSError:
                    out_bytes = 0

            logging.info(f"任务计算结果： {result_1}")

            # 构建响应消息
            msg_dict = {'task_id': data_decode['msg']['task_id'],
                        'sub_task_id': data_decode['msg']['sub_task_id'],
                        'device_no': data_decode['msg']['device_no'],
                        'finished_time': "",
                        'output_bytes': out_bytes,
                        'result': result_1}
            rsp_dict = {
                'dag_subtask_rsp': 'dag_subtask_rsp',
                'from_device_no': data_decode['to_device_no'],
                'from_device_ip': data_decode['to_device_ip'],
                'from_device_port': data_decode['to_device_port'],
                'to_device_no': data_decode['from_device_no'],
                'to_device_ip': data_decode['from_device_ip'],
                'to_device_port': data_decode['from_device_port'],
                'msg': msg_dict
            }

            # 发送响应
            # 注意: socket发送应该是线程安全的，但如果是高并发场景可能需要锁，
            # 这里简单起见直接发送。
            try:
                # 再次检查 socket 是否有效
                if sock.fileno() != -1:
                    sock.send(json.dumps(rsp_dict).encode('utf-8'))
                    logging.info(f"Sent task result back to server: {rsp_dict}")
            except (socket.error, OSError) as e:
                logging.warning(f"Socket error during task response sending: {e}")

        except Exception as e:
            logging.error(f"Error processing task: {e}")
        finally:
            with cls.running_lock:
                cls.running_tasks = max(0, cls.running_tasks - 1)

    @classmethod
    def _record_link(cls, src_ip, src_port, stats):
        """被动链路遥测：用一次真实产物拉取的 字节数/耗时 估算到源设备的有效带宽(MB/s)。

        小数据(如 256B 特征)的耗时主要是连接建立/时延，所以这里同时记录 last_ms 作为时延参考；
        EWMA 平滑多次采样。仅做记录，不影响任务执行。
        """
        try:
            elapsed = float(stats.get('elapsed') or 0.0)
            nbytes = int(stats.get('bytes') or 0)
            if elapsed <= 0:
                return
            mbps = (nbytes / elapsed) / (1024 * 1024)
            key = f"{src_ip}:{int(src_port)}"
            with cls.links_lock:
                prev = cls.observed_links.get(key, {})
                a = 0.5  # EWMA 权重
                mbps_s = mbps if 'mbps' not in prev else a * mbps + (1 - a) * prev['mbps']
                ms_s = elapsed * 1000 if 'last_ms' not in prev else a * elapsed * 1000 + (1 - a) * prev['last_ms']
                cls.observed_links[key] = {'mbps': round(mbps_s, 3), 'last_ms': round(ms_s, 2),
                                           'bytes': nbytes, 'samples': prev.get('samples', 0) + 1}
        except Exception:
            pass

    @classmethod
    def _probe_loop(cls):
        """T1.2 主动链路探测：周期性从每个在线对端拉取探测块，测真实带宽/时延，写入
        observed_links。这样即使两台设备之间还没发生过真实产物传输，放置决策也有「先验」
        链路信息可用（精准放置）。被动遥测与主动探测共用同一 observed_links（EWMA 融合）。"""
        recv_path = os.path.join(ARTIFACTS_DIR, '__probe_recv__.blob')
        time.sleep(min(8, cls.PROBE_INTERVAL))   # 先等首个心跳 ACK 把对端清单填好
        while True:
            for peer in list(cls.peers):
                ip, port = peer.get('ip_address'), peer.get('data_port')
                if not ip or port is None:
                    continue
                try:
                    stats = fetch_blob(ip, int(port), cls.PROBE_ID, recv_path, timeout=30)
                    cls._record_link(ip, port, stats)
                    logging.info(f"[probe] 主动探测 {ip}:{port} "
                                 f"({stats['bytes']} bytes, {stats['elapsed']:.3f}s)")
                except Exception as e:
                    logging.warning(f"[probe] 探测对端 {ip}:{port} 失败: {e}")
            time.sleep(cls.PROBE_INTERVAL)

    @classmethod
    def receive_msg(cls, sock, addr):
        '''
        接受来自server分发的任务，并调用相关函数进行处理
        '''
        # 发送设备初始化消息，格式化为JSON以便服务器识别
        init_msg = {'heart_beat': True, 'device_name': name, 'device_no': get_gl_device_no(),
                    'data_port': DATA_PORT}
        sock.send(json.dumps(init_msg).encode('utf-8'))  # TCP: sendto -> send
        logging.info(f"Sent device initialization message to {addr}")

        while True:
            try:
                data = sock.recv(65535)  # TCP: recvfrom -> recv
                if not data:  # 如果没有接收到数据，说明连接可能已断开
                    logging.warning("Connection closed by server")
                    break
                logging.info(data.decode('utf-8'))
            except (socket.error, OSError) as e:
                # 捕获 socket 相关异常，如 socket 被关闭等
                logging.warning(f"[EXIT] The receive_msg thread is over, Socket error occurred: {e}")
                break

            # ---------------处理对应的dag任务-------------
            if NetworkUtils.is_json(data.decode('utf-8')):
                data_decode = json.loads(data.decode('utf-8'))
                if NetworkUtils.is_dict(data_decode) and 'udp_p2p_msg' in data_decode:
                    logging.info(f"Received P2P message: {data_decode}")
                    AlgorithmList = ActuatorImpl.read_algorithms()
                    if data_decode['msg']['algo_name'] in AlgorithmList:
                        # 开启新线程并行处理任务
                        task_thread = threading.Thread(
                            target=cls.process_task, 
                            args=(sock, data_decode),
                            daemon=True
                        )
                        task_thread.start()
                        logging.info(f"Started new thread to process task: {data_decode['msg']['algo_name']}")

                    else:
                        logging.error(f"[ERR] The receive_msg algorithm is not exist. {data_decode['msg']['algo_name']}")
                elif NetworkUtils.is_dict(data_decode) and 'SERVER RESPONSE IP' in data_decode:
                    # 服务器响应心跳：收到 ACK 的时刻 - 最近一次心跳发送时刻 = 真实 RTT
                    if cls.last_hb_sent:
                        cls.last_rtt_ms = (time.time() - cls.last_hb_sent) * 1000.0
                    # 记录在线对端清单，供主动探测线程使用
                    if isinstance(data_decode.get('peers'), list):
                        cls.peers = data_decode['peers']
                    logging.info(f"Received server heartbeat response: {data_decode}")
                else:
                    logging.info(f"Received other message: {data_decode}")

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
            data = message.encode('utf-8')
            sock.send(data)  # TCP: sendto -> send
            logging.info(f"send msg {string} to {addr}")
            if string.lower() == 'EXIT'.lower():
                sock.close()
                break
        logging.info(f"[EXIT] The sending thread is over")

    @classmethod
    def send_heartbeat(cls, sock, addr):
        while True:
            # 每个心跳都重新采样物理信息，保证 CPU/内存是实时值（get_physical_device 内部
            # cpu_percent(interval=1) 会阻塞约 1s，相对 10s 心跳间隔可接受）。
            device_info = DeviceMonitor.get_physical_device()
            device_info['heart_beat'] = True
            device_info['device_no'] = get_gl_device_no()
            device_info['data_port'] = DATA_PORT  # 数据面端口，供 server 告知后继“去哪拉产物”
            device_info.update(cls.calculate_network_metrics(sock, addr))  # throughput/bandwidth/network_usage 仍为占位
            # —— 真实监测信号（覆盖占位值）——
            device_info['work_status'] = cls.running_tasks            # 正在执行的任务数
            device_info['delay'] = round(cls.last_rtt_ms, 2)          # 服务器↔设备 RTT(ms)
            with cls.links_lock:
                device_info['observed_links'] = dict(cls.observed_links)  # 被动链路遥测
            message = json.dumps(device_info)
            try:
                cls.last_hb_sent = time.time()
                sock.send(message.encode('utf-8'))  # TCP: sendto -> send
                logging.info(f"Sent heartbeat to server: {message}")
            except (socket.error, OSError) as e:
                # 捕获 socket 相关异常，如 socket 被关闭等
                logging.info(f"[EXIT] The heartbeat thread is over, Socket error occurred: {e}")
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
        # 主线程预加载算法模块：避免子线程首次 import 重型 C 扩展(numpy/torch)在 Windows 上死锁。
        ActuatorImpl.preload_algorithms()

        # 启动数据面产物服务器（设备间大块数据直传 / QUIC）
        os.makedirs(ARTIFACTS_DIR, exist_ok=True)
        cls.artifact_server = ArtifactServer(port=DATA_PORT)
        cls.artifact_server.start()
        logging.info(f"数据面 ArtifactServer 已启动: 端口 {DATA_PORT}, 产物目录 {ARTIFACTS_DIR}")

        # T1.2 主动探测：提供一个固定大小的探测块，供其它设备测量到本机的链路带宽/时延。
        probe_path = os.path.join(ARTIFACTS_DIR, '__probe__.blob')
        with open(probe_path, 'wb') as f:
            f.write(b'\0' * (4 * 1024 * 1024))   # 4MB，足够压过建连开销、测得有意义的带宽
        cls.artifact_server.register(cls.PROBE_ID, probe_path)
        threading.Thread(target=cls._probe_loop, daemon=True).start()

        # 创建TCP socket而不是使用CLIENT_SOCK
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # TCP
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)  # 保持与原代码一致的缓冲区设置
        server = (HOST, PORT)

        # TCP需要先连接到服务器
        try:
            sock.connect(server)
            logging.info(f"Connected to server at {server}")
        except Exception as e:
            logging.error(f"Failed to connect to server: {e}")
            return

        logging.info("-----欢迎使用OpenSAI Offloading System,退出连接请输入'EXIT(不分大小写)'-----")
        receive_msg_thread = threading.Thread(target=cls.receive_msg, args=(sock, server), daemon=True)
        receive_msg_thread.start()

        ts = threading.Thread(target=cls.send, args=(sock, server))
        ts.start()

        heartbeat_thread = threading.Thread(target=cls.send_heartbeat, args=(sock, server))
        heartbeat_thread.start()

        # 不阻塞主线程，让三个线程在后台运行
        # receive_msg_thread.join()
        ts.join()
        heartbeat_thread.join()
        sock.close()


if __name__ == '__main__':
    DeviceClient.activate_client()