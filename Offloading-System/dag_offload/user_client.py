"""OpenSAI web bridge (Flask :5000).

Holds ONE persistent TCP socket to the server (:8888), forwards browser requests, and
routes the server's replies. Serves the new **OpenSAI Console** (static/console.html) at `/`
and the legacy compiled SPA at `/legacy`.

Two API styles coexist:
  * NEW JSON endpoints (used by the console): /api/monitor, /api/algos, /api/basic,
    /api/submit, /api/result, /api/config, /api/upload_py.
  * LEGACY one-shot SSE endpoints (used by the old SPA): /api/status, /api/update,
    /api/devices, /api/algorithms, /api/policies, /api/dag/upload.

Monitoring reads the server's aggregated view straight from Redis (key OPENSAI_MONITOR_VIEW,
published by the server every ~5s) — device resources + brain ledgers. Runtime-tunable
(SAFE) system params are written to Redis keys OPENSAI_RT:* which the server applies live.
"""
import datetime
import json
import logging
import os
import queue
import socket
import sys
import threading
import time

from flask import Flask, Response, jsonify, render_template, request
from flask_cors import CORS
from werkzeug.utils import secure_filename

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config.network_config import HOST, PORT
from network.utils.network_utils import NetworkUtils

try:
    import redis
    from config.redis_config import REDIS_HOST, REDIS_PORT, REDIS_DB
    R = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
except Exception:  # redis optional for the bridge to import
    R = None

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)
ALLOWED_EXTENSIONS = {'py'}

# server socket + a lock so multiple Flask threads don't interleave sends on it
sock = None
sock_lock = threading.Lock()

# latest cached state from the server (filled by the broker thread)
_state = {'algorithms': [], 'devices': [], 'policies': [], 'results': {}}
_state_lock = threading.Lock()

# legacy SSE queues (old SPA)
results_queue = queue.Queue()
feedback_queue = queue.Queue()
algorithm_queue = queue.Queue()
devices_queue = queue.Queue()
policy_queue = queue.Queue()

RT_KEYS = {'placement': 'OPENSAI_RT:placement', 'w_load': 'OPENSAI_RT:w_load',
           'w_xfer': 'OPENSAI_RT:w_xfer', 'auto_mem': 'OPENSAI_RT:auto_mem',
           'target_xfer_s': 'OPENSAI_RT:target_xfer_s'}


def _send(obj):
    """Thread-safe send of a JSON message to the server."""
    if sock is None:
        return False
    try:
        with sock_lock:
            sock.send(json.dumps(obj).encode('utf-8'))
        return True
    except (socket.error, OSError) as e:
        logging.warning(f"send to server failed: {e}")
        return False


def _handle_server_msg(obj):
    if not NetworkUtils.is_dict(obj):
        return
    if 'devicesInfo' in obj or 'algorithmsInfo' in obj or 'policiesInfo' in obj:
        key = next(iter(obj))
        info = obj.get(key) or {}
        with _state_lock:
            if isinstance(info, dict):
                _state['algorithms'] = info.get('algorithms', _state['algorithms'])
                _state['devices'] = info.get('devices', _state['devices'])
                _state['policies'] = info.get('policies', _state['policies'])
        # also feed legacy queues
        {'devicesInfo': devices_queue, 'algorithmsInfo': algorithm_queue,
         'policiesInfo': policy_queue}.get(key, devices_queue).put(obj)
    elif 'dagResult' in obj:
        dr = obj['dagResult']
        with _state_lock:
            _state['results'][dr.get('taskName')] = dr
            _state['results']['_latest'] = dr
        logging.info(f"[FINISH] DAG done: {dr.get('taskName')}")
    elif 'dagUpdateFeedback' in obj:
        feedback_queue.put(obj)
    elif 'dagRspStatus' in obj:
        results_queue.put(obj)


def msg_broker(s):
    """Read the server socket continuously; robustly split concatenated/partial JSON."""
    _send({'user_beat': {'name': 'user1'}})
    logging.info("broker: receiving from server")
    buf = ''
    dec = json.JSONDecoder()
    while True:
        try:
            data = s.recv(262144)
        except (socket.error, OSError) as e:
            logging.warning(f"[EXIT] broker socket error: {e}")
            break
        if not data:
            logging.warning("broker: server closed connection")
            break
        buf += data.decode('utf-8', 'replace')
        while buf.strip():
            buf = buf.lstrip()
            try:
                obj, idx = dec.raw_decode(buf)
            except ValueError:
                break  # incomplete; wait for more bytes
            buf = buf[idx:]
            try:
                _handle_server_msg(obj)
            except Exception as e:  # noqa: BLE001
                logging.warning(f"broker handle error: {e}")


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# ----------------------------------------------------------------------------- pages
@app.route('/')
def index():
    return app.send_static_file('console.html')


@app.route('/legacy')
def legacy():
    return render_template('index.html')


# ----------------------------------------------------------------------- new JSON API
@app.route('/api/monitor', methods=['GET'])
def api_monitor():
    """Server-side aggregated view (device resources + brain ledgers) from Redis."""
    view = {}
    if R is not None:
        try:
            raw = R.get('OPENSAI_MONITOR_VIEW')
            if raw:
                view = json.loads(raw)
        except Exception as e:  # noqa: BLE001
            logging.debug(f"monitor read failed: {e}")
    return jsonify(view)


@app.route('/api/basic', methods=['GET'])
def api_basic():
    """Cached {algorithms, devices, policies} (server-sourced)."""
    with _state_lock:
        return jsonify({'algorithms': _state['algorithms'], 'devices': _state['devices'],
                        'policies': _state['policies']})


@app.route('/api/algos', methods=['GET'])
def api_algos():
    _send({'algorithmsInfo': 'data'})
    for _ in range(15):                       # wait briefly for the broker to cache the reply
        with _state_lock:
            if _state['algorithms']:
                break
        time.sleep(0.1)
    with _state_lock:
        return jsonify({'algorithms': _state['algorithms']})


@app.route('/api/submit', methods=['POST', 'OPTIONS'])
def api_submit():
    if request.method == 'OPTIONS':
        return '', 200
    body = request.get_json(force=True, silent=True) or {}
    dag = body.get('dagData') if 'dagData' in body else body
    if not isinstance(dag, dict) or 'taskName' not in dag or 'nodes' not in dag:
        return jsonify({'error': 'expected {taskName, nodes, ...}'}), 400
    ts = time.time()
    dag['taskCreateTime'] = ts                       # server uses float ts directly
    dag.setdefault('taskId', '1')
    dag.setdefault('taskDesc', '')
    dag.setdefault('taskPolicy', 'p')
    task_id = str(dag['taskName']) + str(ts)         # matches server's task_id keying
    with _state_lock:
        _state['results'].pop(task_id, None)
    if not _send({'dagData': dag}):
        return jsonify({'error': 'server not connected'}), 503
    return jsonify({'ok': True, 'task_id': task_id, 'nodes': len(dag.get('nodes', []))})


@app.route('/api/result', methods=['GET'])
def api_result():
    task_id = request.args.get('task_id', '')
    with _state_lock:
        dr = _state['results'].get(task_id)
    if dr:
        return jsonify({'ready': True, 'result': dr})
    return jsonify({'ready': False})


@app.route('/api/config', methods=['GET', 'POST', 'OPTIONS'])
def api_config():
    if request.method == 'OPTIONS':
        return '', 200
    if R is None:
        return jsonify({'error': 'redis unavailable'}), 503
    if request.method == 'POST':
        body = request.get_json(force=True, silent=True) or {}
        applied = {}
        for field, key in RT_KEYS.items():
            if field in body and body[field] is not None:
                val = body[field]
                if field == 'auto_mem':
                    val = '1' if val in (True, 1, '1', 'true', 'on') else '0'
                elif field == 'placement':
                    if val not in ('plan', 'smart', 'random'):
                        continue
                else:
                    try:
                        val = str(float(val))
                    except (ValueError, TypeError):
                        continue
                try:
                    R.set(key, str(val))
                    applied[field] = str(val)
                except Exception:
                    pass
        return jsonify({'ok': True, 'applied': applied})
    # GET: prefer the server's reported effective runtime (from the monitor view); fall back to RT keys
    eff = {}
    try:
        raw = R.get('OPENSAI_MONITOR_VIEW')
        if raw:
            eff = (json.loads(raw) or {}).get('runtime', {})
    except Exception:
        pass
    rt = {}
    for field, key in RT_KEYS.items():
        try:
            v = R.get(key)
            rt[field] = v.decode() if isinstance(v, bytes) else v
        except Exception:
            rt[field] = None
    return jsonify({'effective': eff, 'pending': rt})


@app.route('/api/upload_py', methods=['POST', 'OPTIONS'])
def upload_py_file():
    if request.method == 'OPTIONS':
        return '', 200
    if 'file' not in request.files:
        return jsonify({'error': 'No file part in the request'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    if not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file extension, only .py allowed'}), 400
    filename = secure_filename(file.filename)
    save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'device', 'implements')
    save_dir = os.path.abspath(save_dir)
    os.makedirs(save_dir, exist_ok=True)
    try:
        file.save(os.path.join(save_dir, filename))
    except Exception as e:  # noqa: BLE001
        return jsonify({'error': f'Failed to save file: {e}'}), 500
    return jsonify({'message': 'File saved successfully', 'path': os.path.splitext(filename)[0]}), 200


# --------------------------------------------------------------- legacy SSE endpoints
def generate(used_queue):
    try:
        data = used_queue.get(timeout=20)
        yield f"data: {json.dumps(data)}\n\n"
    except queue.Empty:
        yield "data: {}\n\n"


@app.route('/api/status', methods=['GET'])
def status_stream():
    _send({})
    return Response(generate(results_queue), mimetype='text/event-stream')


@app.route('/api/update', methods=['GET', 'POST'])
def update_stream():
    data = request.json
    dt = datetime.datetime.strptime(data['dagData']["taskCreateTime"], "%Y-%m-%d %H:%M:%S")
    timestamp = dt.replace(tzinfo=datetime.timezone.utc).timestamp()
    task_id = data['dagData']["taskName"] + str(timestamp)
    _send({"dagUpdate": "data", 'dagUpData': {"taskName": task_id}})
    return Response(generate(feedback_queue), mimetype='text/event-stream')


@app.route('/api/policies', methods=['GET'])
def policies_stream():
    _send({"policiesInfo": "data"})
    return Response(generate(policy_queue), mimetype='text/event-stream')


@app.route('/api/algorithms', methods=['GET'])
def algorithms_stream():
    _send({"algorithmsInfo": "data"})
    return Response(generate(algorithm_queue), mimetype='text/event-stream')


@app.route('/api/devices', methods=['GET'])
def devices_stream():
    _send({"devicesInfo": "data"})
    return Response(generate(devices_queue), mimetype='text/event-stream')


@app.route('/api/dag/upload', methods=['POST', 'OPTIONS'])
def dag_receive_data():
    if request.method == 'OPTIONS':
        return '', 200
    _send(request.json)
    return Response(generate(results_queue), mimetype='text/event-stream')


def _connect_and_serve():
    global sock
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
    try:
        s.connect((HOST, PORT))
        logging.info(f"Connected to server at {(HOST, PORT)}")
    except Exception as e:  # noqa: BLE001
        logging.error(f"Failed to connect to server at {(HOST, PORT)}: {e}")
    sock = s
    # ONE socket, sends only on user action (user_beat at startup, dagData on submit, *Info on
    # demand). No periodic poller: the server uses naive recv-per-message framing, so back-to-back
    # sends can coalesce into one recv and get dropped (then user_beat is lost -> results never
    # route back). Keeping sends user-paced avoids that; the dashboard reads Redis via /api/monitor.
    threading.Thread(target=msg_broker, args=(s,), daemon=True).start()


if __name__ == '__main__':
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    _connect_and_serve()
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
