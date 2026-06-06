# OpenSAI Console — 使用说明书

一个全新的 Web 控制台，用来**演示并操作** OpenSAI 的全部能力：实时集群监控、可视化编排
并提交 DAG 任务、查看放置与执行结果、在线调整（安全的）系统参数、管理算法。

> 配套：[README.md](../README.md)（平台使用）、[ARCHITECTURE.md](ARCHITECTURE.md) §11–§15（实现原理）、
> [ROADMAP.md](ROADMAP.md)（进度）。本控制台是单文件前端 `Offloading-System/dag_offload/static/console.html`，
> 由 `user_client.py`（Flask）在 `:5000` 提供，**无需任何构建步骤、不依赖外网**。

---

## 1. 启动

在仓库根目录 `D:\opensai-master`：

```powershell
# 一次性安装（建 .venv、装依赖、下载便携 Redis）
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1

# 启动整套 + Web 控制台（Redis + server + 1 个 device + Flask :5000）
powershell -ExecutionPolicy Bypass -File scripts\run.ps1 -WithWeb
```

浏览器打开 **http://127.0.0.1:5000** 即见新控制台。多开几个设备（演示放置/并行更直观）：

```powershell
# 再起几台 device（不同数据端口；强弱可用 worker 数区分）
$env:OPENSAI_DATA_PORT=9998; $env:OPENSAI_TASK_WORKERS=3
.\.venv\Scripts\python.exe Offloading-System\dag_offload\run_device.py
```

> 旧版编译前端仍保留在 **http://127.0.0.1:5000/legacy** 。停止：`scripts\stop.ps1 -All`。

---

## 2. 界面总览

左侧 5 个标签页：

| 页 | 作用 |
|---|---|
| **集群概览** | 实时资源监控 + 大脑账本（每 2 秒刷新） |
| **任务编排** | 选模板 / 自定义 DAG、设每节点算法·依赖·资源，提交执行 |
| **运行结果** | makespan、放置图（按设备着色）、跨设备传输、节点明细、历史 |
| **系统参数** | 在线调整**安全可调**项（放置模式/权重/自动内存/自动带宽）；易出错项只读 |
| **算法库** | 算法目录 + 上传自定义算法插件 (.py) |

左下角是**连接状态**（绿点=已连接实时；红点=无设备/未连接）与当前放置模式。

---

## 3. 集群概览（Dashboard）

数据来自 server 每 2 秒发布的**聚合视图**（设备心跳 + 大脑账本，见 §8）。

- **顶部统计卡**：在线设备、可用/总核、在飞任务、运行中、已承诺出口带宽（数字平滑变化）。
- **设备卡**（每台一张，左侧色条=该设备配色，与结果图一致）：
  - CPU%、内存 已用/总（带进度条，>85% 红、>60% 黄）；
  - **核 已用/可用**、**运行/预留**（T2.1 放置预留）、**已承诺带宽**（T3.2-A）、**RTT**；
  - 右上 `N槽` = 该设备并行能力 `worker_slots`（≥3 标“强”、=1 标“弱”）。
- **设备间链路**：实测带宽/时延（被动遥测 + 主动探测，T1.2）。跑过跨设备任务后才有样本。
- **算法历史画像**（B1）：每个算法的实测峰值内存 / 任务边际内存 / 耗时 / 样本数（EWMA），
  用于内存自动定档与记账。

> 这些“大脑账本”（预留、已承诺带宽、在飞、画像）在原始设备心跳里是没有的，只有 server 内存里有——
> 控制台展示的是 server 把两者合并后的完整视图。

---

## 4. 任务编排（Compose）

### ① 选模板
一排模板按钮，点击即载入到编辑器，可再改：

| 模板 | 形状 / 演示点 |
|---|---|
| 基础 Pi→Sleep | 最小链路，验证调度/分发/执行/收集 |
| 跨设备数据 | Produce→Consume，产物经 QUIC 数据面跨设备传输 |
| 扇入融合 | 2 producer → 1 fusion，从两台设备各拉一份 |
| 协同感知(CIFAR) | 图左右半 → 两设备各提特征 → 融合分类（固定到 dev1/dev2，强制跨设备）|
| 归约树(6→3→1) | 多级归约，体现就近放置省传输 |
| 并行算力(MatMul×4) | 多核矩阵乘并行，体现算力分配 |

### ② 任务节点
每行一个节点：
- **算法**下拉（见算法库目录）；切换算法会自动带出该算法的参数。
- **依赖前驱**：勾选本节点依赖哪些前驱节点（决定执行顺序；上游产物会自动送到下游设备）。
- **参数**：算法自身参数（如 `size_mb`/`compute_ms`/`image_index` 等）。
- **资源档位**（留空=自动，由适配优化决定）：
  - `核` = 该任务的 CPU 核数；`内存MB` = 任务**在运行基线之上**的内存预算；`带宽MB/s` = 该传输的限速。
- **固定设备#**：填某台设备序号可强制放到该设备；`-1` = 交给适配优化放置（推荐）。
- `+ 节点` 添加、`×` 删除；顶部可改**任务名称**。

### ③ DAG 预览
右侧实时画出 DAG（节点按拓扑分层、画依赖边），随编辑更新。

点右上 **▶ 提交执行** → 自动跳到“运行结果”并开始轮询。

> 安全设计：控制台只暴露**安全可调**的每任务参数；端口、worker 数等易出错项不开放（见 §6）。

---

## 5. 运行结果（Results）

提交后实时轮询，完成即展示：
- **统计卡**：Makespan（端到端耗时）、节点数、**跨设备传输 / 总边数**、用到设备数、错误数。
- **放置与执行图**：DAG 按**设备着色**（图例在右上），**虚线**=跨设备传输的边；红框=出错节点。
  一眼看出适配优化把哪些节点放到了一起（少传输）/ 铺开（抢并行）。
- **节点明细**：每个节点的设备、耗时、结果串（错误标红）。
- **历史运行**：最近若干次运行，点击可回看。

> 想直观对比“有无适配优化”：在“系统参数”把放置模式切到 `random`，提交同一个模板，再切回
> `plan`/`smart` 提交，对比两次的 Makespan 与跨设备传输数。

---

## 6. 系统参数（Settings）

**在线可调（保存后约 2 秒生效，无需重启）：**
- **放置策略**：`plan 前瞻`（默认，整图就近规划）/ `smart 贪心`（逐节点自适应）/ `random 对照`。
- **W_LOAD 负载权重**：越大越**分散**（抢并行）、越小越**聚合**（省传输）。
- **W_XFER 传输权重**：越大越在意跨设备搬运成本。
- **自动内存定档**（`OPENSAI_AUTO_MEM`）：开启后按算法历史边际内存自动给每任务内存预算。
- **自动带宽目标时延**（秒）：>0 时按 `前驱数据量 / 该时延` 自动估算每条传输带宽；0=关。

点 **保存并应用** 写入后端。页面也会显示当前**生效值**（server 回报）。

**只读 / 需重启（不开放在线修改）：** 控制端口、每设备并行 worker 数、server 主机等——
这些属于易出错项，随意改可能导致集群异常，故只展示、加锁（避免误操作）。

---

## 7. 算法库（Algorithms）

- **算法目录**：控制台内置的算法及其参数（驱动编排页的参数输入）。含：Pi/Sleep/Random（纯计算）、
  ProduceBlob/ConsumeBlob/Relay（数据面桩）、MatMul（多核算力）、FeatureExtract/Fusion（协同感知）。
- **设备端已注册**：从 server 实时拉取（含你上传的）。
- **上传算法插件**：选一个 `.py` 上传到 `device/implements/`。要求：文件名以 `Algorithm` 开头、含同名类、
  构造签名 `__init__(self, param_dict, data_dict)`、实现 `run()`。上传后即可在编排页按名使用。

---

## 8. 实现原理（How it works）

### 8.1 桥接：浏览器 ↔ user_client(Flask) ↔ server(TCP)
`user_client.py` 持有**一条到 server 的常驻 TCP 连接**，把浏览器的 HTTP 请求翻译成 server 的
TCP/JSON 报文，并把 server 的回复路由回来。一个后台 broker 线程**稳健地按 JSON 边界切分**
（处理 TCP 粘包/拆包）server 的消息，缓存最新设备/算法/结果。

### 8.2 新 JSON API（控制台使用）
| 端点 | 方法 | 作用 |
|---|---|---|
| `/` | GET | 新控制台 `console.html`（`/legacy` 为旧前端） |
| `/api/monitor` | GET | server 聚合视图（设备资源 + 大脑账本），直接读 Redis 键 `OPENSAI_MONITOR_VIEW` |
| `/api/submit` | POST | 提交 DAG：写入 `taskCreateTime`、算出 `task_id`、发给 server，返回 `task_id` |
| `/api/result?task_id=` | GET | 轮询该任务的 `dagResult`（按 `dagResult.taskName==task_id` 路由）|
| `/api/config` | GET/POST | 读/写运行期参数（POST 写 Redis 键 `OPENSAI_RT:*`）|
| `/api/algos` `/api/basic` | GET | 算法/设备/策略列表（broker 缓存）|
| `/api/upload_py` | POST | 上传 `.py` 算法到设备实现目录 |

（旧的一次性 SSE 端点 `/api/status` `/api/devices` … 仍保留，供 `/legacy` 旧前端用。）

### 8.3 实时监控视图
server 的卸载线程每 ~2 秒调用 `aggregate_monitor_view()`，把**设备心跳**（Redis）与**大脑内存账本**
（放置预留 `reservations`、已承诺出口带宽 `committed_bw`、在飞任务 `inflight`、算法画像 `algo_history`）
合并，写到 Redis 键 `OPENSAI_MONITOR_VIEW`（TTL 30s）。控制台前端每 2 秒 GET `/api/monitor` 展示。

### 8.4 在线调参（安全）
控制台 POST `/api/config` → 写 Redis 键 `OPENSAI_RT:{placement,w_load,w_xfer,auto_mem,target_xfer_s}`。
server 的卸载线程每 ~2 秒 `_refresh_runtime_config()` 读取并应用：放置模式覆盖 `runtime_placement`、
权重更新 `OffloadingImpl.W_LOAD/W_XFER`、自动内存/带宽写回进程内环境变量。**只接受这几个安全键**，
端口/worker 数等不在其列。

### 8.5 提交与结果路由
提交时 user_client 用 `time.time()` 设 `taskCreateTime` 并算 `task_id = taskName + str(ts)`（与 server 的
任务键一致）；server 完成后回 `dagResult{taskName=task_id, finishedTime, nodes[]}`，broker 按 `taskName`
缓存，前端用 `task_id` 取回。**注意**：本控制台经一条共享连接提交，适合单任务演示；要并发提交多任务，
用 `scripts/exp_multidag_contention.py`（每任务独立连接）。

---

## 9. 故障排查

- **概览无设备**：先 `run.ps1`（或多起几个 `run_device.py`）；确认 Redis 在跑。
- **提交后结果一直转圈**：看 `.run\server.err.log` / `.run\device.err.log`；多为无在线设备（任务在
  设备上线后会自动继续）或算法报错（结果串会显示 `ERROR:`）。
- **改了系统参数没生效**：等约 2–4 秒（server 每 2 秒拉取一次）；放置模式只接受 plan/smart/random。
- **协同感知模板报错**：需先 `python scripts\train_collab.py` 训练权重 + 放好 CIFAR 数据（见 README §6）。
- **端口 8888/5000 被占**：`scripts\stop.ps1 -All` 清干净再 `run.ps1 -WithWeb`。
- **自检**：`.\.venv\Scripts\python.exe scripts\verify_frontend.py`（起栈 + 跑通全部新 API，应 10/10）。
