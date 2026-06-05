# OpenSAI v1.1 — 代码地图（真实调用链 vs 死代码）

> 目的：帮你把**真正在运行的代码**和**遗留/死代码**彻底分开，避免改错地方。
> 本文基于对 import 关系的实际检索（2026-06，tag `v1.1.1`），不是凭印象。

---

## 0. TL;DR

- **通信传输是 TCP**（`socket.SOCK_STREAM`），不是 UDP。所有消息是 **JSON-over-TCP**，
  经 `server.py` 中转的**星型拓扑**（设备之间不直连，都通过 server 转发）。
  - ⚠️ 消息里的字段名 `udp_p2p_msg` / `udp_bc_msg` 是 **UDP 时代的历史命名**，
    传输其实是 TCP。别被名字误导。
- **真正的入口只有 3 个**（都在 `Offloading-System/dag_offload/`）：
  `server.py`、`device_client.py`、`user_client.py`。
- 仓库里约 **30% 是死代码 / 遗留**（见第 3 节），其中包括一整套**没用上的 UDP 网络层**。
- **重要发现**：3 个真实入口都 `import` 了 `AlgorithmTransmissionImpl` / `MsgTransferToolImpl`
  却**从不调用**它们 —— 而这两个类间接 `import cv2`。也就是说，**这 3 个无用 import 是核心
  路径唯一需要 opencv 的原因**（详见第 4 节）。

---

## 1. 真实运行时调用链（一个 DAG 从提交到出结果）

```
[前端/注入器]                [server.py  :8888 TCP]                 [device_client.py]
     |                              |                                      |
     |-- user_beat (注册用户) ----->| user_dict / client_sockets          |
     |-- dagData (提交DAG) -------->| handle_client_messages()            |
     |                              |   解析 nodes -> SubTaskInfo[]        |
     |                              |   all_task_dict[task_id]             |
     |                              |   update_ready_task() 标记就绪        |
     |                              |                                      |
     |                          [线程1] offloading_making_process()        |
     |                              |   offloading_strategy(subtask)       |
     |                              |     -> OffloadingImpl.random/direct  |
     |                              |        (读 DeviceServiceImpl ← Redis) |
     |                              |   放入 ready_subtask_queue            |
     |                              |                                      |
     |                          [线程2] handle_ready_subtasks()            |
     |                              |   构造 p2p_msg                        |
     |                              |-- client_sockets[device_no].send --->| receive_msg()
     |                              |   (裸 sock.send，非 MsgTransferTool)   |   process_task()
     |                              |                                      |     execute_task()
     |                              |                                      |       ├─ ResourceControlInterface
     |                              |                                      |       │   .start_task()  (Docker)
     |                              |                                      |       └─(失败则降级)
     |                              |                                      |          ActuatorImpl.run_actuator()
     |                              |                                      |            └─ 动态加载 device/implements/
     |                              |                                      |               Algorithm_*.run()
     |                              |<-- dag_subtask_rsp (结果) ------------|
     |                              |   标记 FINISH，清依赖，                 |
     |                              |   update_ready_task() 触发后继节点      |
     |                              |   (所有节点 FINISH 时)                  |
     |<-- dagResult (汇总) ---------|   summarize_task_result()            |
```

**这条链路上真正用到的模块（active）：**

| 模块 | 作用 |
|---|---|
| `dag_offload/server.py` | 卸载决策 + 分发中枢（TCP 服务端、两条后台线程、DAG 依赖调度） |
| `dag_offload/device_client.py` | 边缘设备执行端（心跳上报、收任务、容器/本地执行、回结果） |
| `dag_offload/user_client.py` | Flask 前端/API（`/api/dag/upload` 等，SSE 长轮询回传） |
| `offloading_brain/offloading_brain.py` | `random_strategy` / `direct_strategy`（`rl_based_strategy` 是空壳） |
| `task/bean/sub_task_info.py` 及 `task/bean/*`、`result/bean/*` | 数据模型 |
| `device/implements/actuator_impl.py` | 动态加载并执行 `Algorithm_*` |
| `device/interfaces/resource_control_interface.py` | Docker 资源控制（算力+内存；无带宽） |
| `device/device_monitor.py` | 采集 CPU/内存/IP（带宽/时延目前硬编码为 0） |
| `device/implements/Algorithm_*.py` | 具体算法（Pi/Sleep/Random 纯 Python；CNN/Train 需 torch） |
| `predis/implements/device_service_impl.py` + `predis/utils/redis_utils.py` | Redis 设备注册表 |
| `config/*` | 常量、Redis/网络配置、日志格式、子任务状态枚举 |
| `network/utils/network_utils.py` | `is_json` / `is_dict` 小工具 |
| `network/protocol/packet_protocol_conversion.py` | 广播→P2P 拆分（`udp_bc_msg` 分支用到） |

---

## 2. 活跃的 TCP 消息类型（都走 server 中转）

| 消息键 | 方向 | 含义 |
|---|---|---|
| `heart_beat` | device → server | 设备心跳/注册（写入 Redis 在线表） |
| `user_beat` | client → server | 用户注册（记录回传地址） |
| `dagData` | client → server | 提交 DAG 任务 |
| `dagRspStatus` | server → client | DAG 已入队 ACK |
| `udp_p2p_msg` | server ↔ device | 派发子任务 / 设备回结果（**TCP，命名遗留**） |
| `dag_subtask_rsp` | device → server | 子任务执行结果 |
| `dagResult` | server → client | 整个 DAG 完成汇总 |
| `dagUpdate` / `dagUpdateFeedback` | client ↔ server | 运行中查询任务进度 |
| `devicesInfo` / `algorithmsInfo` / `policiesInfo` | client ↔ server | 前端拉取设备/算法/策略 |
| `udp_bc_msg` | client → server | 广播（拆成多条 P2P 转发） |

---

## 3. 死代码 / 遗留清单（建议处理）

> 证据 = 在 `Offloading-System/` 全树检索 import 的结果。"无人 import" 即孤立。

### 3a. 完全孤立（无人 import，可安全归档/删除）
| 文件 | 说明 | 风险 |
|---|---|---|
| `network/network_server.py` | UDP 服务端（被 TCP 版取代） | 低 |
| `network/network_client.py` | UDP 客户端 | 低 |
| `network/socket_network.py` | UDP + pickle 工具 | 低 |
| `network/server.py` / `network/client.py` | UDP demo（import 即报错） | 低 |
| 根目录 `server.py` / `client.py` / `new_opensai.py` | 历史 demo / where2comm 设计草稿 | 低 |
| `task/implements/task_scheduler_manager_impl.py` | `TaskSchedulerManagerImpl` 无人 import | 低 |
| `task/implements/task_center_impl.py` | **空文件** | 低 |
| `task/interfaces/task_center.py` | 7 个 `pass` 桩，无人实现/调用 | 低 |
| `task/interfaces/data_center.py` | `pass` 桩，无人用 | 低 |
| `task/implements/big_data_transmission_test.py` | 大文件传输测试脚本 | 低 |

### 3b. 独立运行的脚本（不是被 import，是直接 `python xxx.py` 跑的）
| 文件 | 性质 |
|---|---|
| `dag_offload/single_client.py` | 单次 DAG 注入器（默认含 CNN，需 torch）；我们另写了更轻的 `scripts/inject_dag.py` |
| `dag_offload/task_form.py` | WebSocket 反馈 demo，未接入主流程 |
| `Offloading-System/app.py` | **flasgger 模板**（CODING 脚手架），**不是真正的服务端**（README 误标） |

### 3c. "貌似在用、其实是僵尸链"（见第 4 节）
- `task/implements/algorithm_transmission_impl.py`（`AlgorithmTransmissionImpl`）
- `network/implements/msg_transfer_tool_impl.py`（`MsgTransferToolImpl`）

> 这两个类只被「死代码」(`task_scheduler_manager_impl`、`network_client`、`big_data_transmission_test`)
> 和「3 个真实入口里的无用 import」引用。真实派发是裸 `sock.send(json)`，**没用到它们**。

---

## 4. 重要：3 个无用 import 把 opencv 拖成了硬依赖

`server.py:19`、`device_client.py:16`、`user_client.py:24/29` 各自 `import` 了
`AlgorithmTransmissionImpl` / `MsgTransferToolImpl`，**但全程没有调用**（检索证实只有 import 行）。
而 `msg_transfer_tool_impl.py:9` 有 `import cv2`、`import numpy`。

**后果**：纯 Python 的 Pi/Sleep DAG 本不需要 opencv，却因为这几个无用 import 而**必须装 opencv 才能启动**。

**建议（低风险、收益大）**：删掉这 3 处无用 import 后：
- 核心路径不再需要 `opencv-python` / `numpy`（可从 `requirements.txt` 移到可选）；
- 启动更快、依赖更少。
- 验证方式：删完跑 `scripts/smoke_test.ps1`，PASS 即安全。

---

## 5. 建议的清理顺序（每步都先跑 smoke_test）

1. **零风险**：删 `network/*` UDP 五件套 + 根目录 3 个 demo + `task_center_impl.py`（空）。
2. **低风险**：删 3 处无用 import → 把 opencv/numpy 降级为可选依赖。
3. **看情况**：`single_client.py`/`task_form.py`/`big_data_transmission_test.py` 移到 `examples/` 或删。
4. 每步：`git commit` 一次，`smoke_test.ps1` 必须 PASS。出问题 `git revert` 即可。

> 本文是"地图"，不含删除动作。要执行清理告诉我，我会按上面顺序逐步做 + 每步测试 + 单独提交。
