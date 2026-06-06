# OpenSAI 系统架构与功能说明（v1.1.2）

本文说明 OpenSAI 的整体架构、各组件职责、运行时数据流、通信协议、任务模型，
以及**当前能做什么 / 还不能做什么**。配套文档：
[README.md](../README.md)（使用）、[CODE_MAP.md](CODE_MAP.md)（活/死代码）、
[DEPLOYMENT.md](DEPLOYMENT.md)（多机）、[ROADMAP.md](ROADMAP.md)（Phase 2 路线图与进度）。

> **⚠️ Phase 2 进展提示（本文 §2/§8/§9 的部分基线已过时）**：§1–§10 描述的是 v1.1.2 基线。
> 此后 Phase 2 已落地：**QUIC 设备间数据面**（跨设备自动传输产物，§9 的“数据不自动传输”已不再成立）、
> **真实网络监测**（带宽/时延/RTT，§2.3 的“硬编码 0”已不再成立）、以及
> **自适应 + 前瞻的智能放置**（取代“随机/手动”，§2.2 的卸载决策已升级）。
> 放置/设备选择的当前实现见下方 **§11**；整体进度见 [ROADMAP.md](ROADMAP.md)。

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
- **卸载决策（`offloading_brain/offloading_brain.py`）**：✅ **已升级为自适应 + 前瞻的智能放置**
  （详见 §11）。`locality_strategy`（成本式自适应贪心）与 `plan_dag`（整图反向就近规划，默认）
  按真实数据量/链路/负载选设备；`random_strategy`/`direct_strategy` 保留作对照与手动指定；
  `rl_based_strategy` 仍是空壳 ❌。
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
其中 ① 是 ②③ 的前提，建议优先。（①③ 已在 Phase 2 完成，见下 §11 与 [ROADMAP.md](ROADMAP.md)。）

---

## 11. 设备选择（放置）机制【Phase 2 更新】

“每个就绪子任务放到哪台设备”由 `server.offloading_strategy` 决策，具体策略在
`offloading_brain/offloading_brain.py`。由环境变量 `OPENSAI_PLACEMENT` 选择模式：

| 模式 | 说明 |
|---|---|
| **plan（默认）** | 前瞻整图规划：DAG 提交后一次性规划全图 |
| **smart** | 自适应贪心：子任务就绪时逐个决策 |
| **random** | 随机选在线设备（实验对照基线） |

> 任何模式下，节点若带 `directAllocationId>0` 都**优先按手动指定**（实验/复现用），不参与自动决策。
> 规划得到的设备若已掉线，自动**回退贪心**，保证仍能放下去（plan 的混合鲁棒性）。

### 11.1 统一成本模型（plan / smart 共用）

把一个节点放到设备 `d` 的代价折算成**预计延迟秒数**，选代价最小者：

```
cost(d) = W_XFER × Σ 传输秒数(产物送往非本机的相关节点) + W_LOAD × 设备并发负载
传输秒数 = 链路时延 + 数据字节数 / 链路带宽
```

- **按数据量自适应**：传输项随真实数据量缩放——数据大 → 传输项主导 → **就近/聚合**（省搬运）；
  数据小 → 负载项主导 → **铺开**（抢并行）。拐点随数据量自动移动，无需手调权重。
- **链路带宽/时延**来自链路遥测（被动：真实拉取打点；主动：周期性 4MB 探测）。未知链路用
  `OPENSAI_DEFAULT_BW_MBPS`（默认 10MB/s）兜底。
- **数据量**：`smart` 用前驱**实测产出大小**（设备完成时在 `dag_subtask_rsp` 里上报 `output_bytes`，
  存于 `server.output_sizes`）；`plan` 在提交时用 DAG 声明的 `size_mb` 估。
- **并发负载**：来自**预留账本**（`server.reservations`）——已放置但未完成的任务数（实时信号，
  放置即 +1、完成即 −1，防止把任务都堆到同一台“看似空闲”的机器，即防超卖）。

### 11.2 smart：自适应贪心（逐节点，在线）

子任务就绪时即决策，只看它的**前驱产出在哪**（同机=0 传输，即“就近”）与当前负载。简单、在线、
对设备上下线鲁棒；但只“向后看”——**独立的源节点**（无前驱）只能按负载铺开，无法为下游汇聚预先聚拢。

### 11.3 plan：前瞻整图反向就近规划（默认）

DAG 提交后**一次性规划整张图**，按**倒拓扑序（消费者先放）**处理：放每个节点时让它偏向其
（已放置的）**后继**所在设备——“产出在将被消费的地方生产”。于是连源节点也会被拉向其汇聚点，
吃到贪心结构上够不到的聚合收益。关键点：负载项**只统计与当前节点相互独立的节点**（依赖链是
顺序执行、不并发，故同机放一条链**零并发惩罚**；只有独立的兄弟节点才相互竞争 → 该铺开则铺开）。
规划结果按 task 缓存于 `server.placement_plan`，DAG 完成后清除。

### 11.3.1 监测输入（均为实测值）

CPU/内存（每次心跳实时采样）、在建任务数（`work_status`）、服务器↔设备 RTT（心跳往返计时）、
**设备间链路带宽/时延**（数据面被动遥测 + 主动探测）。`scripts/show_monitor.py` 可实时查看设备表与链路。

### 11.4 可调旋钮（环境变量）

`OPENSAI_PLACEMENT`（plan/smart/random）、`OPENSAI_W_XFER`（传输权重）、
`OPENSAI_W_LOAD`（负载权重，↑ 更倾向分散、↓ 更倾向聚合）、`OPENSAI_DEFAULT_BW_MBPS`、
`OPENSAI_PROBE_INTERVAL`（主动探测间隔秒）。

### 11.5 实测效果（7 节点 DAG，对比 random 基线）

| 负载档 | smart（贪心） | plan（前瞻，默认） |
|---|---|---|
| 计算重（数据小） | +9% | **−32%** |
| 均衡 | −24% | −23% |
| 网络重（数据大） | −56% | **−84%**（接近“全聚合”理论上限） |

复现：`scripts/exp_placement_quant.py`（3 设备规模扫描）、`scripts/exp_placement_scale.py`
（24 节点 ×10 设备）。详细实验结论见 [ROADMAP.md](ROADMAP.md)。

> 仍未做（属适配优化的“给多少资源”那一维，依赖资源控制机制先落地）：**资源档位分配**
> （决定给每个任务多少 CPU/带宽并调用资源控制接口执行）——见 ROADMAP 的 T3.2 / T4 / T5.3。
