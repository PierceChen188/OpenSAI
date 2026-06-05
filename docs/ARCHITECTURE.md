# OpenSAI 系统架构与功能说明（v1.1.2）

本文说明 OpenSAI 的整体架构、各组件职责、运行时数据流、通信协议、任务模型，
以及**当前能做什么 / 还不能做什么**。配套文档：
[README.md](../README.md)（使用）、[CODE_MAP.md](CODE_MAP.md)（活/死代码）、
[DEPLOYMENT.md](DEPLOYMENT.md)（多机）。

---

## 1. 总览

OpenSAI 是一个 **DAG 计算卸载平台**：把一个应用拆成有依赖关系的子任务（DAG），
平台负责把子任务调度、分发到不同边缘设备执行，并按依赖顺序汇总结果。

设计上分三层（见仓库根目录 README 的架构图）：

| 层 | 设计职责 | 当前实现度 |
|---|---|---|
| **应用层** | 定义/发布 DAG 任务、查看设备状态与回传结果 | ✅ 基本完成 |
| **智联服务层** | 计算卸载决策、集群管理、适配优化（启发式/RL） | ⚠️ 卸载/调度可用；**智能优化是空壳** |
| **服务定制网络层** | 设备通信收发、资源监测、资源控制 | ⚠️ 通信+算力控制可用；**带宽/真实监测缺失** |

> 一句话现状：**“把 DAG 跑起来、按依赖分发到设备、收结果”这条主干是通的、扎实的；
> 真正体现科研价值的“智能/自适应”部分（启发式/RL 卸载、资源感知调度、带宽分配、
> 应用层反馈）目前是占位或缺失。**

---

## 2. 三层架构详解（对照代码）

### 2.1 应用层
- **任务定义与发布**：Web 前端（`dag_offload/user_client.py` + `static/` 预编译包）
  让用户可视化定制 DAG；也可用 `scripts/inject_dag.py` / `single_client.py` 直接提交。✅
- **设备状态 / 结果回传**：`user_client.py` 提供 SSE 接口
  （`/api/devices`、`/api/status`、`/api/update`、`/api/algorithms`、`/api/policies`），
  从 server 拉取设备列表、任务结果、运行反馈。✅
- **应用执行组件（通信调整 / 权重分配 / 模型结构调整）**：架构图中“应用历史执行状态反馈”
  的自适应闭环 —— **未实现** ❌。
- **Redis 存储**：设备在线表、历史记录（`predis/`）。✅（History 部分逻辑被注释）

### 2.2 智联服务层
- **计算卸载 + 分布式化（`server.py`）**：✅ 这是平台的核心引擎，**完整实现**：
  - 接收 DAG → 解析为 `SubTaskInfo` 列表 → 依赖就绪判定（前驱全完成才 READY）；
  - 两条后台线程：`offloading_making_process`（对就绪子任务做卸载决策）、
    `handle_ready_subtasks`（把决策后的子任务通过 TCP 发给目标设备）；
  - 收设备回传 → 标记完成 → 解除后继依赖 → 整图完成后汇总回传客户端。
- **卸载决策（`offloading_brain/offloading_brain.py`）**：⚠️ 仅有
  `random_strategy`（随机选一台在线设备）和 `direct_strategy`（按节点的
  `directAllocationId` 指定设备）；`rl_based_strategy` 是 **空壳** ❌。
- **集群管理（节点筛选 / 资源评估 / 数据质量评估）**：⚠️ 部分代码存在但**未接入主链路**：
  `device/implements/idle_device_impl.py`（空闲过滤）、`predis` 里按性能/内存/延迟打分的
  接口写好了，但活跃的 server 用的是“读全部设备 + 随机”。**数据质量评估未实现** ❌。
- **适配优化组件（启发式算法 / RL 模型）**：架构图里的核心差异点 —— **不存在** ❌。

### 2.3 服务定制网络层
- **通信收发**：⚠️ **传输是 TCP 星型**（所有 device/user 连 server，server 中转）。
  消息字段名里的 `udp_*` 是历史命名，不是 UDP。设计中的 P2P 原语
  （`network/implements/msg_transfer_tool_impl.py` 的 `send_p2p_msg`/`send_p2p_big_msg`）
  **未接入主链路**（清理后已不被核心导入）。
- **资源监测（`device/device_monitor.py`）**：⚠️ 真实采集 CPU/核数/内存/IP；
  但**吞吐/带宽/网络使用率/时延硬编码为 0** ❌（这是后续“适配优化”的前置依赖）。
- **资源控制（`device/interfaces/resource_control_interface.py`）**：⚠️ 通过 Docker 实现了
  **算力 + 内存**分配，且相当完整（见 §6）；**带宽分配、存储分配未实现** ❌。

---

## 3. 运行时数据流（一个 DAG 从提交到出结果）

```
[前端/注入器]            [server.py  :8888 TCP 中枢]              [device_client.py]
   |                          |                                        |
   |-- user_beat ------------>| 记录用户连接（回传用）                   |
   |-- dagData -------------->| 解析 nodes -> SubTaskInfo[]             |
   |                          | update_ready_task() 标记就绪            |
   |<-- dagRspStatus ---------| （已入队 ACK）                          |
   |                      [线程1] offloading_making_process            |
   |                          |  offloading_strategy(subtask)          |
   |                          |   -> OffloadingImpl.random/direct       |
   |                          |      (DeviceServiceImpl ← Redis 在线表)  |
   |                          |   无设备则保持 READY 重试（不崩）         |
   |                      [线程2] handle_ready_subtasks                |
   |                          |-- udp_p2p_msg (派发子任务) ------------>| receive_msg
   |                          |   (裸 sock.send JSON)                   |  process_task
   |                          |                                        |   execute_task
   |                          |                                        |    ├ Docker 容器(限 CPU/内存)
   |                          |                                        |    └ 失败则降级本地执行
   |                          |                                        |      ActuatorImpl 动态加载
   |                          |                                        |      Algorithm_*.run()
   |                          |<-- dag_subtask_rsp (结果) -------------|
   |                          |  标记 FINISH，解除后继依赖，               |
   |                          |  update_ready_task() 触发后继           |
   |<-- dagResult (汇总) -----|  整图完成 -> summarize_task_result      |
```

- 设备侧另有一条 **心跳线**：`device_client` 每 10s 上报 `heart_beat`（CPU/内存等），
  server 写入 Redis 在线表；设备断开时 server 将其从在线表移除（避免选中死设备）。

---

## 4. 核心组件清单

| 组件 | 文件 | 职责 |
|---|---|---|
| 卸载中枢 | `dag_offload/server.py` | TCP 服务端、DAG 依赖调度、决策、分发、结果汇总 |
| 设备执行端 | `dag_offload/device_client.py` | 心跳上报、收子任务、容器/本地执行、回结果 |
| Web/API 前端 | `dag_offload/user_client.py` | Flask 路由、SSE 回传、算法上传 |
| 卸载决策 | `offloading_brain/offloading_brain.py` | random / direct（RL 待实现） |
| 算法执行器 | `device/implements/actuator_impl.py` | 扫描并动态加载 `Algorithm_*`，调用 `run()` |
| 资源控制 | `device/interfaces/resource_control_interface.py` | Docker 容器：起/停/复用/回收、限 CPU/内存 |
| 设备注册表 | `predis/implements/device_service_impl.py` + `predis/utils/redis_utils.py` | 在线设备读写、移除 |
| 任务模型 | `task/bean/*`、`result/bean/*` | SubTaskInfo / 状态 / 结果等数据类 |
| 配置 | `config/*` | 端点(HOST/PORT)、Redis、状态枚举、日志 |

---

## 5. 通信协议（TCP，经 server 中转）

| 消息键 | 方向 | 含义 |
|---|---|---|
| `heart_beat` | device → server | 设备心跳/注册 |
| `user_beat` | client → server | 用户注册（用于结果回传定位） |
| `dagData` | client → server | 提交 DAG |
| `dagRspStatus` | server → client | DAG 已入队 ACK |
| `udp_p2p_msg` | server ↔ device | 派发子任务 / 设备回结果（**实为 TCP**） |
| `dag_subtask_rsp` | device → server | 子任务结果 |
| `dagResult` | server → client | 整图完成汇总 |
| `dagUpdate` / `dagUpdateFeedback` | client ↔ server | 运行中查询进度 |
| `devicesInfo` / `algorithmsInfo` / `policiesInfo` | client ↔ server | 拉取设备/算法/策略 |
| `udp_bc_msg` | client → server | 广播（拆成多条 P2P 转发） |

> 注意 TCP 粘包：代码用 `time.sleep(0.2)`、注入器用发送间隔来规避，是已知的健壮性弱点。

---

## 6. 任务模型与资源控制

### 子任务状态机（`config/sub_task_status.py`）
`UN_WORK`（未就绪）→ `READY`（依赖已满足）→ `WORK`（已分发）→ `FINISH`（完成）；
另有 `EARLY_EXIT`（提前完成）、`DROPPED`（丢弃）。server 据此驱动 DAG 推进。

### Docker 资源控制（`ResourceControlInterface`，约 1000 行，已实现）
- `start_task`：起容器执行任务，限 `--cpus` / `--memory`；
- `update_container_resources`：运行中动态调整 CPU/内存；
- `reuse_idle_container` / `get_idle_containers`：**容器复用**（空闲队列）；
- `stop_container` / `recycle_idle_containers`：停止 / 回收。
- 无 Docker 时 `device_client` 自动降级为**本地进程内执行**（`ActuatorImpl`）。

> 注意：当前主链路是“每任务起一个容器、用完删”，**未启用复用**；复用能力已实现但未接入。

---

## 7. 当前能力清单（今天就能用 ✅）

1. 以 JSON / Web 方式**定义并提交带依赖的 DAG 任务**。
2. server 按**依赖顺序调度**子任务，分配到设备（**随机**或按 `directAllocationId` **手动指定**）。
3. 设备**执行算法**（Docker 限资源执行，或无 Docker 时本地执行），返回结果。
4. **结果按依赖汇总**并回传客户端；支持早退/丢弃/整体完成的判定。
5. **设备注册与在线管理**（心跳、Redis 在线表、断开自动剔除）。
6. **按需扩展算法**：丢一个 `Algorithm_*.py` 即可（或前端上传）。
7. **单机 Docker 资源控制**：限/调 CPU 与内存、容器起停/复用/回收。
8. **多机部署**：设 `OPENSAI_HOST` 即可跨主机（见 DEPLOYMENT.md）。
9. **Web 可视化**：拖拽定制 DAG、查看设备与结果（预编译前端）。

## 8. 设计中但尚未实现（❌ 后续工作）

1. **智能卸载决策**：启发式 / RL 模型（`rl_based_strategy` 空壳）。
2. **资源感知的集群调度**：节点筛选 / 资源评估 / 数据质量评估（代码部分存在但未接入）。
3. **适配优化组件**：根据任务“计算复杂度 + 通信量”自适应分配“算力 + 带宽”。
4. **真实网络监测**：带宽 / 时延 / 吞吐目前硬编码为 0（是上面 1–3 的前置依赖）。
5. **带宽分配 / 存储分配**：资源控制目前只有算力+内存。
6. **应用层自适应反馈**：通信调整 / 权重分配 / 模型结构调整。
7. **跨设备数据流**：DAG 边只表示执行顺序，**不在设备间自动传输数据文件**（见 §9）。

## 9. 已知限制（部署/使用前须知）

- **跨节点数据依赖不自动传输**：上游节点产生的文件不会搬到下游所在的另一台设备；
  子任务只把“结果值”回传 server。数据流水线请用**共享存储**，或后续实现设备间传输。
- **卸载是随机/手动**，非负载/网络感知。
- **容器未复用**（每任务起删一个容器），有冷启动开销。
- **TCP 粘包**靠 sleep 规避，高并发下不够稳。
- **错误吞咽**：部分异常被静默 `try/except` 吞掉（如 `RedisUtils`），排障需看日志。
- 详细的“活代码 vs 死代码”见 [CODE_MAP.md](CODE_MAP.md)。

## 10. 演进方向（阶段 2 起点）

要实现真正的“按算力/带宽智能协同卸载”，建议的依赖顺序是：
**① 把网络监测做成真的（带宽/时延）→ ② 启发式/RL 卸载与资源决策 → ③（数据流水线则）设备间数据传输或共享存储**。
其中 ① 是 ②③ 的前提，建议优先。
