import socket
import logging
from config.network_config import HOST, PORT

# 延迟连接：只创建socket，不立即连接
CLIENT_SOCK = None
_connected = False

def get_client_socket():
    """获取客户端socket，如果未连接则自动连接"""
    global CLIENT_SOCK, _connected
    if not _connected:
        try:
            CLIENT_SOCK = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            CLIENT_SOCK.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128000000)
            CLIENT_SOCK.connect((HOST, PORT))
            _connected = True
            logging.info(f"TCP connection established to {HOST}:{PORT}")
        except Exception as e:
            logging.error(f"Failed to establish TCP connection: {e}")
            raise
    return CLIENT_SOCK