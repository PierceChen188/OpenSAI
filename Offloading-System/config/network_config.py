import os

# 网络端点。默认回环地址（单机自测）。
# 多机部署：把 server 所在主机的局域网 IP 设到所有机器的环境变量 OPENSAI_HOST
# （server 会 bind 到它并接受远程连接，device/user 会连到它），无需改动源码。
HOST = os.environ.get('OPENSAI_HOST', '127.0.0.1')
PORT = int(os.environ.get('OPENSAI_PORT', '8888'))

