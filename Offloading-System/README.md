# OPENSAI 计算卸载底座API

启动redis：
sudo redis-server /etc/redis/redis.conf --daemonize yes

* README.md - 本文件
* Jenkinsfile - 用以自动构建和测试的脚本
* Dockerfile - 用以自动构建 Docker 镜像的脚本
* requirements.txt - 依赖包文件
* app.py - 主 Flask 服务器端源代码
* ./config - 配置文件
* ./device - 设备info
* ./network - 网络通信API
* ./predis - redis接口API
* ./result - 任务结果info
* ./task - 任务定义
* ./dag_offload - 计算卸载DEMO
