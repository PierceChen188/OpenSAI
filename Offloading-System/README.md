# OPENSAI 计算卸载底座

> 本目录是 OpenSAI 主系统。**使用指南和架构说明请看仓库根目录的文档**，不要以本文件为准：
>
> - 使用 / 快速上手：[../README.md](../README.md)
> - 前端需求与接口规格（重做中）：[../docs/FRONTEND_REQUIREMENTS.md](../docs/FRONTEND_REQUIREMENTS.md)
> - 系统架构与功能：[../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
> - 路线图与进度：[../docs/ROADMAP.md](../docs/ROADMAP.md)

真正的入口在 `dag_offload/`：`server.py`（卸载决策+分发中枢）、`device_client.py`
（边缘设备执行端）、`user_client.py`（Web/API 前端）。`dag_offload/README.md` 内含
详细的 DAG 数据格式示例。

目录速览：
* `dag_offload/` - 主程序（server / device_client / user_client / offloading_brain）
* `device/` - 设备信息、算法实现（`implements/Algorithm_*.py`）、Docker 资源控制
* `network/` - 通信工具（`utils`、`protocol`、`implements` 的 P2P 消息 API）
* `predis/` - Redis 接口/服务层
* `task/` `result/` - 任务与结果的数据模型
* `config/` - 配置（端点、Redis、状态枚举、日志）
* `requirements.txt` / `requirements-ml.txt` / `requirements-optional.txt` - 依赖
* `Dockerfile` / `Jenkinsfile` - 构建脚本

> 说明：`app.py` 是历史遗留的 flasgger 模板，**不是**本系统的服务端（真正的服务端是
> `dag_offload/server.py`）。
