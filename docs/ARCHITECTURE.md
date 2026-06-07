# OpenSAI 系统架构与功能说明（v1.1.2）

本文说明 OpenSAI 的整体架构、各组件职责、运行时数据流、通信协议、任务模型，
以及**当前能做什么 / 还不能做什么**。配套文档：
[README.md](../README.md)（使用）、[FRONTEND_REQUIREMENTS.md](FRONTEND_REQUIREMENTS.md)（前端需求/接口规格）、
[ROADMAP.md](ROADMAP.md)（Phase 2 路线图与进度）。

> **⚠️ Phase 2 进展提示（§1–§10 是 v1.1.2 基线，多处已被超越）**：此后 Phase 2 已落地——
> **QUIC 设备间数据面**（§9 的“数据不自动传输”已不成立）、**真实网络/资源监测 + server 聚合视图**
> （§2.3 的“硬编码 0”已不成立）、**自适应 + 前瞻智能放置**（取代 §2.2 的“随机/手动”）、以及
> **资源控制 + 资源档位分配**（§2.3 的“带宽分配缺失”已不成立：算力绑核 / 内存上限 / 网络限速 +
> 大脑按任务分配 CPU/内存/带宽）。当前实现见 **§11 放置 · §12 资源控制机制 · §13 资源档位分配 ·
> §14 监测聚合 · §15 健壮性**；整体进度见 [ROADMAP.md](ROADMAP.md)。

---

## 1. 总览

OpenSAI 是一个 **DAG 计算卸载平台**：把一个应用拆成有依赖关系的子任务（DAG），
平台负责把子任务调度、分发到不同边缘设备执行，并按依赖顺序汇总结果。

设计上分三层（见仓库根目录 README 的架构图）：

| 层 | 设计职责 | 当前实现度 |
|---|---|---|
| **应用层** | 定义/发布 DAG 任务、查看设备状态与回传结果 | ✅ 基本完成 |
| **智联服务层** | 计算卸载决策、集群管理、适配优化（放置 + 资源档位） | ✅ 自适应/前瞻放置 + CPU/内存/带宽档位分配已落地（§11/§13）；RL 仍空壳 |
| **服务定制网络层** | 设备通信收发、资源监测、资源控制 | ✅ QUIC 数据面 + 真实监测/聚合视图 + 算力/内存/网络控制（§12/§14）；多路径待做 |

> 一句话现状（Phase 2 后）：**主干（按依赖把 DAG 分发到设备、收结果）扎实；“智能/自适应”这一层
> 已从占位变为可用——位置感知 + 前瞻放置、按任务分配 CPU/内存/带宽、真实资源/链路监测与 server 聚合
> 视图都已落地并有量化实验（§11–§15）。仍待做：RL 决策、多网卡多路径、应用层自适应反馈。**
> （下文 §2/§8/§9 为 v1.1.2 历史基线，保留备查；以 §11–§15 为准。）

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
8. **多机部署**：设 `OPENSAI_HOST` 即可跨主机（见 README §8）。
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
- 死代码/历史清理详情见 git 历史与 [ROADMAP.md](ROADMAP.md)。

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
（24 节点 ×10 设备）、`scripts/exp_collab_adaptation.py`（45 节点错综 lattice ×10 设备）。
后者：**网络重档 plan 比 random makespan −31%、跨设备传输 49.5→16/56**；计算重档仅 −4~−8%
（瓶颈在关键路径的串行单核计算，放置改不了它——分析与改进见 §13.4）。详见 [ROADMAP.md](ROADMAP.md)。

> 适配优化的另一维“**给多少资源**”（资源档位分配）现已落地：见下文
> **§12 资源控制机制**（算力/内存/网络的“执行手段”）、**§13 资源档位分配**（大脑“给多少”的决策）、
> **§14 监测·聚合视图**（决策依据）、**§15 健壮性兜底**。

---

## 12. 资源控制机制（Phase 2c：算力 / 内存 / 网络的“执行手段”）

资源控制分两层：**机制**（本节，怎么把限制施加到任务上）与**决策**（§13，大脑决定给多少）。
机制全部**跨平台、免容器**（领导要的是“进程控制”而非纯容器；Docker 仍可用但不再是主路径）。

### 12.1 进程池 + 绑核（T4.2，`dag_offload/task_pool.py`）

设备把 CPU 密集的算法执行交给一个**预建的 worker 进程池**（`ProcessPoolExecutor`，大小由
`OPENSAI_TASK_WORKERS` 指定）：

- **控并发 + 复用 + 真并行**：最多 N 个任务同时算、worker 循环复用、用进程绕开 GIL 真正吃多核；
  `worker_slots`（=池大小）随心跳上报，供放置做 capacity-aware 负载折算。
- **绑核 = 算力手环**：每个任务按大脑下发的 `_alloc.core_ids` 用 `psutil.cpu_affinity` 钉到一组核
  （`run_task` 里自绑），跨平台限 CPU。池构建时也把各 worker 按 PID 预绑到不相交的核片。
- **Windows spawn 三个坑（已解）**：① worker 重新 import 入口脚本——入口若是重型 `device_client`
  会卡死 → 新增轻量启动器 `run_device.py`（`device_client` 只在 `__main__` 内 import）；
  ② 与运行中的 asyncio IOCP 循环并发 spawn 会卡 → 进程池在事件循环/线程启动**前同步构建**，带超时
  预热自检（`OPENSAI_POOL_WARMUP_S`）失败则判不可用；③ 不用 `multiprocessing.Manager` 传核分配
  （spawn 下连代理会卡）→ 改由主进程按 PID 绑核。
- **回退**：池不可用（预热超时）或运行中损坏（`BrokenProcessPool`）→ 回退主进程内执行，保证设备照常出结果。

### 12.2 内存硬上限（B4，`task_pool._set_process_memory_limit`）

跨平台给 worker 进程设内存上限：**Windows 用作业对象 Job Object（`ProcessMemoryLimit`，可更新）**、
**POSIX 用 `RLIMIT_AS`**；超限的内存分配抛 `MemoryError`。每任务的上限语义见 §13.2（“基线 + 任务预算”）。
两个关键正确性点（各让回归先挂过 2/4 才修对）：
- **锚定“提交内存(commit/vms)”而非 RSS**：作业对象/RLIMIT_AS 限的是提交内存，torch/numpy 预留使
  commit 远大于 RSS，按 RSS 设上限会落在真实 commit 之下 → 立刻 `MemoryError`。
- **锚定“实时”值而非预热静态基线**：worker 复用且 Python 不把释放内存还给 OS，进程占用会漂高；
  按静态基线设的上限会落在当前占用之下 → 误杀无辜任务。故上限恒 ≥ 当前占用、只限任务自身增长。

### 12.3 网络限速 + 出口公平平分（T5.3 + A1，`network/dataplane/quic_server.py`）

数据面发送端（`ArtifactServer`）按**令牌桶**对“交给 QUIC 的字节速率”分块限速（不限速走整块快速路径）：

- **每流速率** = `min(本次请求 rate_mbps, 设备出口公平份额)`。
- **A1 出口公平平分**（`_EgressShare`）：设备出口预算 `OPENSAI_DATA_RATE_MBPS` 在**当前活跃的受控流**
  之间动态平分（`budget / 活跃流数`，随流加入/结束实时再平分）。修了一个真 bug：旧逻辑是**每条流**
  各自限到设备上限 → N 条并发各占满 = N 倍超额；现在守住设备出口。`_EgressShare` 每设备一个、所有
  连接共用、只在单一事件循环线程改计数（无锁）。
- 演示 `scripts/exp_egress_fairshare.py`：预算 4MB/s，双流各 6MB → 各 ~1.9、合计 3.8（守住 4 而非 8）。

---

## 13. 资源档位分配（适配优化的“给多少”维度，Phase 2c）

放置（§11）决定 **WHERE**；本节决定 **HOW MUCH**——大脑给每个任务算 CPU 核数 / 内存预算 / 传输带宽，
注入派发报文，由 §12 的机制执行。决策都**按设备上报的“可用”收口**，避免超额。

### 13.1 带宽分配（A：`server._artifact_rate_mbps`）

给“从源设备拉到本消费者”的每条跨设备传输分配限速（同机本地直读不分配）：
`rate = min(需求, 源设备出口预算 OPENSAI_DEVICE_BW_MBPS − 已承诺)`，地板 `OPENSAI_MIN_BW_MBPS`。其中**需求**：
- ① 节点显式声明 `bw_mbps`；
- ② **A2 自动**：未声明且设了目标时延 `OPENSAI_TARGET_XFER_S` 时，按 `前驱实测产出字节 / 目标时延`
  自动估算（产出字节存于 `server.output_sizes`，免手填）；
- ③ 都没有 → 不设需求上限。

分配即在源设备记账（`committed_bw`）、完成时在 `dag_subtask_rsp` 归还（= 带宽侧“已承诺/可用”）。
实际限速由 §12.3 的令牌桶执行；设备出口的并发公平由 §12.3 的 A1 兜住。

### 13.2 算力 / 内存档位（B：`server._alloc_compute` → `param_dict['_alloc']`）

为每个就绪子任务算一份 `{cores, mem_budget_mb, est_mem_mb}` 注入派发报文：
- **cores**：节点声明 `cores` / `OPENSAI_DEFAULT_CORES` / 否则设备默认槽（总核 ÷ worker_slots）；
  **B2 按设备可用核收口**（`cores_total − cores_committed`）。
- **mem_budget_mb（内存预算）**：节点显式声明 `mem_mb`（当作“基线之上的任务预算”）；或开了
  `OPENSAI_AUTO_MEM` 且有历史时**自动定档** = `max(历史边际×2, 256MB)`；按设备空闲内存收口。
  worker 端上限 = **实时提交内存 + 预算**（§12.2），所以自动定档也安全、不误杀。

**数据通路**：server `_alloc_compute` 注入 `_alloc` → 设备 `process_task` 读 `_alloc`、用 `CoreAllocator`
租 K 个不相交的核、把 `{core_ids, mem_budget_mb}` 传给 worker、并按预估把内存计入 `committed_mem`
（完成归还）→ worker `run_task` 自绑核 + 设内存上限后执行。演示：`scripts/exp_compute_alloc.py`
（同一 matmul 1 核 105s vs 16 核 0.2s ≈ 500×）、`scripts/exp_mem_budget.py`（400MB 任务 预算 100MB→被杀、800MB→通过）。

### 13.3 任务历史画像（B1，`server.algo_history`）

设备在 `dag_subtask_rsp` 上报每任务 `metrics{peak_mem_mb, task_mem_mb(扣掉 worker 基线的边际增长),
proc_secs}`；server 按算法名做 EWMA 聚合。用途：① 记账（`committed_mem` 用边际预估，更准）；
② §13.2 的内存自动定档（`OPENSAI_AUTO_MEM`）；③ 体现在 §14 的聚合视图里。

### 13.4 何时有用、何时帮不上（重要）

适配优化优化的是 **WHERE/HOW MUCH**，所以：
- **数据搬运是瓶颈时赢得多**（协同感知真实情形=大特征张量）：放置就近省传输 → 网络重档 **−31%**。
- **瓶颈在“关键路径上的串行单核计算”时几乎帮不上**（计算重档 −4%）：① 总时间被串行链卡死，放置
  只能并行独立分支、压不短串行链；② 桩计算是单核 GIL，**“多给核”这条杠杆对它空转**；③ 数据小，
  就近省不到东西；④ random 本就把独立任务铺得挺匀。**这不是失效，是瓶颈不在放置/分配上。**
- **改进方向**：用真多核计算（numpy/torch 释放 GIL，核分配立刻变巨大杠杆，见 `exp_compute_alloc`）；
  让并行宽度>设备容量并引入**设备异构**，则容量感知放置避免把活堆到弱设备（`scripts/exp_compute_adaptation.py`）。
- **并发下的瓶颈可能是传输本身**：`scripts/exp_multidag_contention.py`（8 DAG 并发）批完成时间被
  aioquic 数据面吞吐（~10MB/s 单事件循环）卡住，放置只改善尾延迟 −9% → 指向多路径传输（T5.4/T5.5）。

---

## 14. 监测·聚合视图（T1：决策依据，全为实测）

放置/分配的所有输入都来自真实监测（不再是硬编码 0）：

- **T1.1 已承诺/可用算力**：心跳上报 `cores_total/cores_committed`、`mem_committed_mb`、`worker_slots`、
  `work_status`（在建任务数）；“可用 = 总 − 已承诺”由读取方算。
- **T1.2 链路遥测**：`device_client.observed_links` = 设备间有效带宽/时延，**被动**（每次真实产物拉取
  的字节/耗时打点，EWMA）+ **主动**（`_probe_loop` 周期性拉 4MB `__probe__`，`OPENSAI_PROBE_INTERVAL<=0`
  关闭）。+ 服务器↔设备 RTT（心跳往返计时）。
- **T1.3 server 聚合视图**（`server.aggregate_monitor_view`）：把**设备心跳**与**大脑自有账本**
  （放置预留 `reservations` / 已承诺出口带宽 `committed_bw` / 在飞任务 `inflight` / 算法历史画像
  `algo_history`）合并成一份权威快照——这些账本只在 server 内存、Redis 心跳里没有，故只有 server
  能给出完整视图。卸载线程每 ~5s 发布到 Redis 键 `OPENSAI_MONITOR_VIEW`（TTL 30s）。
  `scripts/show_monitor.py` 优先展示它（含 预留/已承诺带宽/可用核/在飞/画像表），缺失则回退原始心跳。

---

## 15. 健壮性兜底（C1 / C2）

- **C1 预留泄漏回收**（`server._reclaim_stale_reservations`）：设备崩溃/任务永不上报完成时，其占的
  放置预留 + 带宽预留不会自动归还 → 卸载线程每 ~30s 按 TTL（`OPENSAI_RESERVATION_TTL`，默认 600s）
  回收超时在飞任务（`inflight` 登记），防账本越扣越满、最后谁都放不进去。
- **C2 回退守限**（`device_client.process_task`）：仅进程池**损坏**（`BrokenProcessPool`）才回退主进程内
  执行，且**大声告警 + 禁用坏池**；任务自身异常（超内存/算法错/超时）按 `ERROR:` 失败返回，
  **不绕过**核/内存限额。
