from flask import Flask, render_template, request, jsonify,Response
from flask_cors import CORS
import logging
import socket
import threading
import json
import sys
import datetime
import queue
import os
from werkzeug.utils import secure_filename
# 获取当前文件的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取父目录 (Offloading-System) 并添加到 sys.path
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config.network_config import HOST, PORT
from network.utils.network_utils import NetworkUtils

# 全局变量，用于存储从请求中获取的dag数据
data_dag = None
data_lock = threading.Lock()  # 用于线程间安全地访问 global_data 的锁
app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

# 允许的文件扩展名
ALLOWED_EXTENSIONS = {'py'}

# 创建一个线程安全的队列，用于在线程之间传递数据
results_queue = queue.Queue()
feedback_queue = queue.Queue()
algorithm_queue = queue.Queue()
devices_queue = queue.Queue()
policy_queue = queue.Queue()


'''
    消息回传前端的逻辑待完善
'''
def msg_broker(sock, addr):
    '''
    消息处理中介，接受来自前端的request 或 后端的feedback，并调用相关函数进行消息传递
    '''
    # 在接收消息前先发送一条消息以便让系统知道所占端口（仅针对特定系统需求，可根据实际情况调整）
    user = {"user_beat": {"name": "user1"}}
    sock.send(json.dumps(user).encode('utf-8'))
    logging.info(f"Start receiving messages from {addr}")

    while True:
        try:
            data = sock.recv(65535)  # 接收数据
            if not data:
                logging.warning("Connection closed by server")
                break
            # Reduce log noise
            # logging.info(data.decode('utf-8')) 
        except (socket.error, OSError) as e:
        # 捕获 socket 相关异常，如 socket 被关闭等
            logging.warning(f"[EXIT] The receive_msg thread is over, Socket error occurred: {e}")
            break

        # ---------------处理对应的dag任务，将result回传给前端-------------
        if NetworkUtils.is_json(data.decode('utf-8')):
            data_decode = json.loads(data.decode('utf-8'))
            if NetworkUtils.is_dict(data_decode):
                if 'devicesInfo' in data_decode:
                    # logging.info(f"[BASE] base devices info is collected!")
                    devices_queue.put(data_decode)
                elif 'algorithmsInfo' in data_decode:
                    # logging.info(f"[BASE] base algorithms info is collected!")
                    algorithm_queue.put(data_decode)
                elif 'policiesInfo' in data_decode:
                    # logging.info(f"[BASE] base policies info is collected!")
                    policy_queue.put(data_decode)
                elif 'dagResult' in data_decode:
                    logging.info(f"[FINISH] The dag task is done! Congratulation!")
                    # results_queue.put(data_decode)
                    print(data_decode)
                elif 'dagUpdateFeedback' in data_decode:
                    logging.info(f"[Update] The dag task is update! Loading!")
                    feedback_queue.put(data_decode)
                elif "dagRspStatus" in data_decode:
                    logging.info(f"[Rsp] The dag is added in the queue!")
                    results_queue.put(data_decode)

def generate(used_queue):
    """
        长轮询视图函数：检查队列中是否有新数据，有的话返回数据，没有的话继续等待。
    """
    # while True:
    try:
        # 获取队列中的新数据（阻塞操作，等待数据出现）
        data = used_queue.get(timeout=20)  # 20秒超时
        yield f"data: {json.dumps(data)}\n\n"
    except queue.Empty:
        # 超时后再循环，继续等待数据
        yield "data: {}\n\n"

def allowed_file(filename):
    """
    检查文件扩展名是否在允许列表中
    """
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    return render_template('index.html')

# 处理 GET 请求
@app.route('/api/status', methods=['GET'])
def status_stream():
    if request.method == 'OPTIONS':
        return '', 200
    # 获取前端发送的 JSON 数据

    global sock
    addr = (HOST, PORT)
    data = {}
    data_encoded = json.dumps(data)
    sock.send(data_encoded.encode('utf-8'))
    # print("/api/status", data)
    return Response(generate(results_queue), mimetype='text/event-stream')

# 处理 GET 请求
@app.route('/api/update', methods=['GET','POST'])
def update_stream():
    if request.method == 'OPTIONS':
        return '', 200
    # 获取前端发送的 JSON 数据
    task_id = None
    data = request.json
    print(data)
    dt = datetime.datetime.strptime(data['dagData']["taskCreateTime"], "%Y-%m-%d %H:%M:%S")

    # 假设时间字符串是 UTC 时间（需显式设置时区）
    dt_utc = dt.replace(tzinfo=datetime.timezone.utc)
    timestamp = dt_utc.timestamp()
    task_id = data['dagData']["taskName"] + str(timestamp)
    data = {"dagUpdate":"data",'dagUpData':{"taskName":task_id}}
    global sock
    addr = (HOST, PORT)
    data_encoded = json.dumps(data)
    sock.send(data_encoded.encode('utf-8'))
    # print("/api/update", data)
    return Response(generate(feedback_queue), mimetype='text/event-stream')

# 处理 GET 请求
@app.route('/api/policies', methods=['GET'])
def policies_stream():
    if request.method == 'OPTIONS':
        return '', 200
    # 获取前端发送的 JSON 数据
    # policy_queue = queue.Queue()
    data = {"policiesInfo":"data"}
    global sock
    addr = (HOST, PORT)
    data_encoded = json.dumps(data)
    sock.send(data_encoded.encode('utf-8'))
    # print("/api/dag/policies", data)
    return Response(generate(policy_queue), mimetype='text/event-stream')

@app.route('/api/algorithms', methods=['GET'])
def algorithms_stream():
    if request.method == 'OPTIONS':
        return '', 200
    # 获取前端发送的 JSON 数据
    # algorithm_queue = queue.Queue()
    data = {"algorithmsInfo":"data"}
    global sock
    addr = (HOST, PORT)
    data_encoded = json.dumps(data)
    # print("/api/dag/algorithms", data)
    sock.send(data_encoded.encode('utf-8'))

    return Response(generate(algorithm_queue), mimetype='text/event-stream')


@app.route('/api/devices', methods=['GET'])
def devices_stream():
    if request.method == 'OPTIONS':
        return '', 200

    # devices_queue = queue.Queue()
    # 获取前端发送的 JSON 数据
    data = {"devicesInfo":"data"}
    global sock
    addr = (HOST, PORT)
    data_encoded = json.dumps(data)
    # print("/api/devices", data)
    sock.send(data_encoded.encode('utf-8'))

    return Response(generate(devices_queue), mimetype='text/event-stream')




# 处理 POST 请求
@app.route('/api/dag/upload', methods=['POST', 'OPTIONS'])
def dag_receive_data():
    # 如果是 OPTIONS 请求，直接返回空响应，表示允许跨域
    if request.method == 'OPTIONS':
        return '', 200
    # 获取前端发送的 JSON 数据
    data = request.json

    global sock
    addr = (HOST, PORT)
    # data = {}
    data_encoded = json.dumps(data)
    # print("/api/dag/upload", data)
    sock.send(data_encoded.encode('utf-8'))

    return Response(generate(results_queue), mimetype='text/event-stream')

@app.route('/api/upload_py', methods=['POST', 'OPTIONS'])
def upload_py_file():
    """
    接收前端上传的 .py 文件，并保存到后端指定目录。
    路径: Offloading-System/device/implements
    """
    # 支持 CORS 预检请求
    if request.method == 'OPTIONS':
        return '', 200

    # 检查请求中是否包含文件字段
    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400

    file = request.files['file']
    # 用户没有选择文件
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # 校验扩展名
    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file extension, only .py allowed'}), 400

    # 使用 secure_filename 让文件名安全，避免目录穿越
    filename = secure_filename(file.filename)

    # 构造保存目录
    # 先拿到 dag_offload 的路径
    current_dir = os.path.abspath(os.path.dirname(__file__))
    # 再上移一级到 Offloading-System 根目录
    project_root = os.path.dirname(current_dir)
    # 指定保存目录到 device/implements
    save_dir = os.path.join(project_root, 'device', 'implements')

    # 确保目录存在
    os.makedirs(save_dir, exist_ok=True)

    file_path = os.path.join(save_dir, filename)
    try:
        # 将文件写入磁盘
        file.save(file_path)
    except Exception as e:
        return jsonify({'error': f'Failed to save file: {str(e)}'}), 500
    
    algorithm_name = os.path.splitext(filename)[0]
    # 返回成功响应，并可选返回文件存储路径
    return jsonify({
        'message': 'File saved successfully',
        'path': algorithm_name
    }), 200




if __name__ == '__main__':
    # 禁用 Flask 默认的访问日志
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # TCP Socket
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
    addr = (HOST, PORT)
    try:
        sock.connect(addr)
        logging.info(f"Connected to server at {addr}")
    except Exception as e:
        logging.error(f"Failed to connect to server at {addr}: {e}")

    broker_msg = threading.Thread(target=msg_broker, args=(sock, addr))
    broker_msg.daemon = True
    broker_msg.start()

    '''
    dag_data = {'dagData':
                    {'taskName': 'app1',
                     'taskId': '1',
                     'taskDesc': '协同感知任务',
                     'taskCreateTime': time.time(),
                     'taskPolicy': '策略1',
                     'nodes':
                         [{
                             'id': 1,
                             'name': 'Node1',
                             'form': {
                                 'beforeNodes': [],
                                 'afterNodes': [],
                                 'taskAlgorithm': 'Algorithm_Pi_Calculation',
                                 'taskVersion': '1.0',
                                 'taskInputInfo': {"inDataPath": '待执行任务取数据位置',
                                                   "outDataPath": '待执行任务产生数据的位置', "count_num": 1000000},
                                 'details': '描述子任务信息1',
                                 'deviceParam': {}
                             },
                             'result': {"taskResultInfo": '任务结果', 'status': 1, 'delay': 0, 'deviceId': 1001}
                         }]
                     }}

    data_encoded = json.dumps(dag_data)
    sock.sendto(data_encoded.encode('utf-8'), addr)

    time.sleep(100)
    '''

    app.run(debug=True)

