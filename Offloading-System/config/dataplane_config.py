import os

# ===== OpenSAI 数据面（设备间大块数据直传 / QUIC）配置 =====
# 控制面仍走 TCP 星型(server:8888)，数据面是独立的、设备间直连的 QUIC 通道。

# 数据面 QUIC 服务监听地址/端口。默认 bind 0.0.0.0 以便被其它设备拉取；
# 端口可用环境变量覆盖（同机多 device 自测时给每个 device 不同端口）。
DATA_HOST = os.environ.get('OPENSAI_DATA_HOST', '0.0.0.0')
DATA_PORT = int(os.environ.get('OPENSAI_DATA_PORT', '9999'))

# QUIC ALPN 协议名（client/server 必须一致才能握手）
ALPN = 'opensai-blob'

# 开发用自签证书路径（首次使用时自动生成；已 gitignore，不入库）
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
_DATAPLANE_DIR = os.path.join(os.path.dirname(_CONFIG_DIR), 'network', 'dataplane')
CERT_FILE = os.path.join(_DATAPLANE_DIR, 'dev_cert.pem')
KEY_FILE = os.path.join(_DATAPLANE_DIR, 'dev_key.pem')
