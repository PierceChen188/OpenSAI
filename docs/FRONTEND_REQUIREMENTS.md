# OpenSAI 前端需求规格说明书

> 给**前端设计师 / 前端工程师**上手用。本文说明 OpenSAI 前端要做成什么样、要实现哪些功能、
> 以及如何与后端对接（接口、数据格式、必须知道的坑）。
> 配套：[README.md](../README.md)（平台使用）、[ARCHITECTURE.md](ARCHITECTURE.md)（系统实现，§11–§15 是放置/资源/监控的细节）。

---

## 0. 一句话背景

OpenSAI 是一个**DAG 计算卸载平台**：用户把一个任务拆成有依赖关系的子任务（一张 DAG 图），
平台把这些子任务**智能地分发到多台边缘设备**上执行，并在设备间通过数据面自动传输中间产物，
最后把结果按依赖汇总返回。它的科研卖点是**适配优化**——“把哪个子任务放到哪台设备（放置）”
和“给每个子任务多少 CPU/内存/带宽（资源档位）”由系统自适应决策，目标是协同感知等场景。

**前端的两个核心使命**：
1. **操作**：可视化地编排 DAG、配置参数、提交任务、查看结果。
2. **监控与演示**：实时展示集群资源与“大脑账本”，并能直观对比“有无适配优化”的差异（给老师/来访者演示）。

> 注意：旧前端（已编译的静态包 + 一版自研控制台）效果不佳，**现决定重做**。本文是重做的依据。

---

# 第一部分 · 产品 / 功能 / 设计（面向设计师）

## 1. 目标用户与典型场景
- **用户**：科研人员、演示者（给导师/评审/来访者看效果）。中文界面为主。
- **场景**：
  1. 打开就能看到**集群是否健康、各设备资源**（监控大屏感）。
  2. 选个模板或自定义一张 DAG → 设参数 → **一键提交** → 看结果。
  3. 看**放置结果**（哪个节点跑在哪台设备）、**跨设备传输**、**端到端耗时(makespan)**。
  4. **切换放置策略**（智能 plan ↔ 随机 random）重跑，**对比收益**（演示适配优化价值）。
  5. 在线**调几个安全的系统参数**；**上传/查看算法**。

## 2. 设计风格要求（重点）
- **精致、大方、专业**，参考成熟产品级仪表盘的克制美学（如 Linear / Stripe / Vercel / Notion 一类）。
- **不要花哨、不要廉价的霓虹渐变**；**字体与字号要舒服**（合理的字号层级、行高、字重对比、留白）。
- **适度动效**（数值/进度条平滑过渡、视图切换淡入、live 指示），但克制，不喧宾夺主。
- **响应式**，至少适配常见桌面分辨率与投屏。
- **离线友好**：部署机可能**无外网或有代理**，**不要依赖运行时 CDN**（字体/JS/CSS 都要本地化或内联）。
- 深色/浅色不强制，但要统一、耐看；演示大屏下对比度足够。

## 3. 信息架构（建议的模块划分）
全局：左侧或顶部导航 + **连接状态指示**（已连接/无设备/断连）。五个模块：

| 模块 | 作用 |
|---|---|
| **A. 集群概览（监控）** | 实时资源 + 大脑账本，演示大屏的主角 |
| **B. 任务编排** | 选模板/自定义 DAG、设算法·依赖·资源、提交 |
| **C. 运行结果** | makespan、放置图、跨设备传输、节点明细、历史 |
| **D. 系统参数** | 在线调“安全可调”项；“易出错项”只读 |
| **E. 算法库** | 算法目录 + 上传 .py 插件 |

> 也可把 B+C 合并成“编排→提交→结果”的流程式单页，看设计师判断。

## 4. 各模块功能需求（详尽）

### A. 集群概览（监控）
数据每 ~2 秒刷新一次（数据源见 §7 的 `OPENSAI_MONITOR_VIEW`）。要展示：
- **汇总指标卡**：在线设备数、可用核/总核、在飞任务数、当前运行任务数、已承诺出口带宽合计。
- **每台设备一张卡**：
  - CPU 使用率、内存 已用/总（建议进度条，阈值变色）；
  - **核 已承诺/可用/总**、**worker_slots（并行能力）**、**当前运行任务数**；
  - **放置预留数**（reservations）、**已承诺出口带宽**（committed bw）、**服务器↔设备 RTT**；
  - 设备角色提示（worker_slots≥3 可标“强”、=1 标“弱”）。
- **设备间链路**：实测带宽/时延（来自 `observed_links`，可能为空，给空态）。
- **算法历史画像**：每个算法的实测峰值内存/边际内存/耗时/样本数（来自 `algo_history`）。
- 状态：加载中、无设备（提示去启动设备）、断连。

> 设计提示：这些“预留/已承诺带宽/在飞/画像”是**大脑账本**，是 OpenSAI 区别于普通调度器的亮点，
> 值得在监控页突出（设备心跳里没有这些，只有 server 聚合视图有）。

### B. 任务编排
- **模板区**：一排可一键载入的预置 DAG（载入后可再编辑）。建议模板：
  - 基础 Pi→Sleep（最小链路）；跨设备数据（产出→消费）；扇入融合（2 产出→1 融合）；
  - 协同感知(CIFAR)（图左右半→两设备各提特征→融合分类）；归约树（多级归约）；并行算力(多核 MatMul)。
- **节点编辑器**：每个节点可设
  - **算法**（下拉，见 §8 算法清单；切换算法应带出该算法的参数表单）；
  - **依赖前驱**（多选其它节点 id；决定执行顺序与数据流）；
  - **算法参数**（随算法而变，如 size_mb / compute_ms / image_index / half …）；
  - **资源档位**（可留空=自动）：`cores` 核数、`mem_mb` 内存预算、`bw_mbps` 传输带宽；
  - **固定设备**（directAllocationId：填设备序号=强制放该设备，-1=交给适配优化）。
  - 增/删节点；编辑任务名称。
- **DAG 可视化预览**：按拓扑分层画节点 + 依赖边，随编辑实时更新（建议 SVG/Canvas，自绘即可，别引 CDN 重库）。
- **提交**：一键提交 → 跳到结果页并开始查询。
- 校验/防呆：依赖不能成环；只暴露**安全参数**，不要让用户填端口之类易错项。

### C. 运行结果
提交后轮询/订阅结果（见 §7）。展示：
- **指标卡**：Makespan（端到端耗时）、节点数、**跨设备传输数 / 总边数**、用到设备数、错误数。
- **放置与执行图**：DAG 图，**节点按所在设备着色**，**虚线标跨设备传输的边**，出错节点高亮。
  —— 一眼看出适配优化把哪些节点放一起（省传输）/铺开（抢并行）。
- **节点明细表**：每个节点的设备、耗时、结果串（出错标红）。
- **历史运行**：最近若干次，可回看与**对比**（强烈建议支持两次结果并排，用于演示 plan vs random）。
- 状态：运行中（转圈/进度）、完成、超时、出错。

### D. 系统参数
- **安全可调（在线生效）**：放置策略（plan/smart/random，附简短解释）、W_LOAD/W_XFER 权重（滑杆 + 含义）、
  自动内存定档开关、自动带宽目标时延。保存后应回显**当前生效值**。
  > ⚠️ 当前后端通过**环境变量**读取这些，**在线热改需要后端支持**（见 §7.5）。设计上先按“可在线调”来做，
  > 后端的热更新机制由前端工程师与后端约定（已有可参考实现，见 §7.5）。
- **只读/加锁（易出错，不开放在线改）**：控制端口、每设备 worker 数、server 主机等——只展示、明确标注“需重启、不可在线修改”。

### E. 算法库
- **算法目录**：内置算法名 + 说明 + 主要参数（驱动编排页的参数表单，建议前端内置一份“算法元数据”，见 §8）。
- **设备端已注册算法**：从后端实时拉取（含用户上传的）。
- **上传算法插件**：上传 `.py` 到后端。要求：文件名以 `Algorithm` 开头、含同名类、构造签名
  `__init__(self, param_dict, data_dict)`、实现 `run()`。

## 5. 领域概念速查（帮助理解“在展示什么”）
- **DAG / 子任务(节点) / 依赖(beforeNodes)**：任务图；节点间有先后依赖。
- **放置(placement)**：决定每个子任务放哪台设备。模式：`plan`（整图前瞻就近，默认）、`smart`（逐节点贪心）、`random`（对照基线）。
- **资源档位**：给每个子任务的 CPU 核 / 内存预算 / 传输带宽（可显式声明或自动）。
- **大脑账本**：放置预留(reservations)、已承诺出口带宽(committed_bw)、在飞任务(inflight)、算法历史画像(algo_history)——只在 server 内存里。
- **数据面 / 跨设备传输**：上游产物经 QUIC 在设备间直传给下游；同机则本地直读（省传输）。
- **makespan**：整张 DAG 的端到端完成时间，越低越好。

---

# 第二部分 · 后端对接（面向前端工程师）

## 6. 系统架构与前端的位置
```
[浏览器/前端 SPA]  ←HTTP/(建议)WebSocket→  [Web 桥接服务]  ←TCP(:8888) JSON→  [server 卸载中枢]
                                              │                                  │ + 各设备(device)
                                              └────────── 读 Redis ─────────────┘
```
- 当前桥接是 `Offloading-System/dag_offload/user_client.py`（Flask，:5000），持有**一条到 server 的常驻 TCP 连接**，
  把浏览器请求翻成 server 的 TCP/JSON 报文，并把回复转回浏览器。**前端工程师可以重写这个桥接**（推荐做成干净的
  JSON/WebSocket 接口），只要遵守下面的协议与坑即可。
- **监控数据**最省事的来源是 **Redis 键 `OPENSAI_MONITOR_VIEW`**（server 每 ~5 秒发布，含设备资源 + 大脑账本），
  桥接直接读 Redis 返回即可，不必走 TCP。

## 7. 数据接口规格

### 7.1 监控视图（Redis 键 `OPENSAI_MONITOR_VIEW`，主数据源）
server 每 ~5s 发布一份 JSON（TTL 30s）。结构：
```jsonc
{
  "devices": {
    "<device_no>": {
      "used_cpu": "2.3%", "idle_memory": 104.0, "total_memory": 127.75,   // 内存单位 GB
      "work_status": 0,            // 当前运行任务数
      "worker_slots": 4,           // 并行能力(进程池大小)
      "cores_total": 32, "cores_committed": 0, "cores_available": 32,
      "mem_committed_mb": 0,
      "delay": 0.9,                // 服务器↔设备 RTT(ms)
      "data_port": 9999, "ip_address": "10.x.x.x",
      "observed_links": { "<ip:port>": {"mbps": 10.9, "last_ms": 365, "bytes": 4194304, "samples": 1} },
      "placement_reserved": 0,     // 大脑账本：放置预留(在飞任务数)
      "bw_committed_mbps": 0.0     // 大脑账本：已承诺出口带宽
    }
  },
  "inflight_tasks": 0,             // 在飞(已派发未完成)子任务总数
  "algo_history": { "Algorithm_ProduceBlob": {"peak_mem_mb": 268, "task_mem_mb": 0, "proc_secs": 1.3, "n": 5} },
  "ts": 1700000000.0
}
```

### 7.2 提交一个 DAG（核心）
DAG 的 JSON 格式（提交给 server 的 `dagData`）：
```jsonc
{ "dagData": {
  "taskName": "demo-task", "taskId": "1", "taskDesc": "",
  "taskCreateTime": 1700000000.0,   // 用 epoch 浮点(秒)；server 直接用它
  "taskPolicy": "p",
  "nodes": [
    { "id": 1, "name": "N1",
      "form": {
        "beforeNodes": [], "afterNodes": [2],     // 依赖关系(前驱/后继 id)
        "taskAlgorithm": "Algorithm_Pi_Calculation", "taskVersion": "1.0",
        "taskInputInfo": {                          // 传给算法 run() 的参数 dict
          "inDataPath": "./data/", "outDataPath": "./data/",
          "count_num": 1000000,                     // 算法自身参数
          "cores": 4, "mem_mb": 512, "bw_mbps": 8   // 可选：资源档位(留空=自动)
        },
        "details": "", "directAllocationId": -1,    // -1=自动放置；>0=固定到第N台设备
        "deviceParam": {}
      },
      "result": { "taskResultInfo": "", "status": 1, "delay": 0, "deviceId": 0 }
    }
    // ... 更多节点 ...
  ]
}}
```
- **task_id 约定**：server 把任务键成 `task_id = taskName + str(taskCreateTime)`。桥接据此匹配/路由结果。
- 提交后 server 立即回 `{"dagRspStatus": true}`（已入队 ACK）。

### 7.3 任务结果（`dagResult`）
DAG 跑完后 server 回：
```jsonc
{ "dagResult": {
  "taskName": "<= 上面的 task_id>",
  "finishedTime": 1.37,            // makespan(秒)
  "nodes": [
    { "id": 1, "name": "N1",
      "ResultInfo": 3.14159,       // 算法返回值(数/字符串)，出错时是 "ERROR: ..."
      "result": { "status": 4, "delay": 1.36, "deviceId": "<device_no>" } }   // deviceId=放置到的设备
  ]
}}
```
- 跨设备传输数 = 统计 `beforeNodes` 边里两端 `deviceId` 不同的条数（前端自己算，用于结果页）。

### 7.4 其它 TCP 消息（经桥接转发）
| 消息(请求) | 方向 | server 回复 |
|---|---|---|
| `{"user_beat":{"name":"user1"}}` | 前端→server | 注册用户连接（**结果回传定位靠它，必须先发**）|
| `{"devicesInfo":"data"}` / `{"algorithmsInfo":"data"}` / `{"policiesInfo":"data"}` | 前端→server | `{"<同名键>": {"algorithms":[...], "policies":[...], "devices":[...]}}` |
| `{"dagData":{...}}` | 前端→server | `{"dagRspStatus":true}`，之后异步回 `{"dagResult":{...}}` |
| 上传算法 | HTTP（非 TCP） | 把 `.py` 存到 `device/implements/`（见旧 `/api/upload_py`）|

### 7.5 运行期可调系统参数（“在线调参”如何落地）
- server 目前从**环境变量**读这些（启动时生效）：`OPENSAI_PLACEMENT`(plan/smart/random)、
  `OPENSAI_W_XFER`、`OPENSAI_W_LOAD`、`OPENSAI_AUTO_MEM`、`OPENSAI_TARGET_XFER_S` 等。
- **要让前端在线热改**，需要后端配合一个机制。**已有可参考的实现**：让 server 周期性从 Redis 键
  `OPENSAI_RT:*`（如 `OPENSAI_RT:placement`）读取并应用；前端/桥接写这些键即可热更新。
  参考 git 提交 **`26e3f69`** 里的 `server._refresh_runtime_config()`（该实现随旧前端一并回退了，可按需重新引入）。
- **只读项**（端口、worker 数、主机等）**不要**做成在线可改。

## 8. 内置算法清单（驱动编排页参数表单）
建议前端内置这份“算法元数据”（name / 说明 / 参数 / 是否产出数据 / 是否需前驱）：

| 算法 | 说明 | 主要参数 | 产出/前驱 |
|---|---|---|---|
| `Algorithm_Pi_Calculation` | 计算 π（纯算） | `count_num`(int) | — |
| `Algorithm_Sleep_1` | 睡眠 | `sleep_time`(秒) | — |
| `Algorithm_Random` | 随机数 | — | — |
| `Algorithm_ProduceBlob` | 产生数据块（数据面桩） | `size_mb`, `compute_ms` | 产出数据 |
| `Algorithm_ConsumeBlob` | 消费数据块 | `compute_ms` | 需前驱产物 |
| `Algorithm_Relay` | 中继(消费+产生) | `size_mb`, `compute_ms` | 产出数据 |
| `Algorithm_MatMul` | 矩阵乘(真多核) | `matrix`, `iters` | — |
| `Algorithm_FeatureExtract` | 协同感知·提特征 | `image_index`, `half`(left/right) | 产出数据 |
| `Algorithm_Fusion` | 协同感知·融合分类 | — | 需 2 份前驱特征 |
| `Algorithm_CNN`/`Algorithm_Train` | 深度学习(需 torch+CIFAR) | 视实现 | — |
- 通用资源档位（可加在任意节点的 `taskInputInfo`）：`cores` / `mem_mb` / `bw_mbps`；`directAllocationId`（固定设备）。
- 设备端真实算法清单可经 `algorithmsInfo` 实时拉取（含上传的）。

## 9. ⚠️ 必须知道的坑（前人踩过）
1. **server 的 TCP 收包是“一次 recv 一条消息”的朴素框架**：多条 JSON 报文**背靠背**发送会粘到一个缓冲里、
   被 `is_json` 判否而**整体丢弃**。
   - 后果：旧自研桥接因“启动时 user_beat 和一个轮询请求挤在一起被丢”→ user_beat 没注册 →
     **结果永远回传不到前端（一直转圈）**。
   - 对策：桥接对 server 的发送要**按用户操作节奏、避免背靠背**；或（更稳）在 **server 端把收包改成按
     JSON 边界缓冲切分**（`json.JSONDecoder().raw_decode` 循环），一次处理多条。**强烈建议后端做这个加固**。
2. **结果路由依赖 user_beat**：server 把 `dagResult` 发给“注册过 user_beat 的那条 user 连接”。
   桥接必须**先成功发 user_beat 并被 server 处理**，否则收不到结果。
3. **task_id 匹配**：`task_id = taskName + str(taskCreateTime)`；`dagResult.taskName` 即该 task_id。
4. **单机多设备的局限**（仅演示环境）：在一台机器上起多个带进程池的 device，Windows 下进程派生可能拥塞/卡住；
   真实多机部署（一台机一个 device）无此问题。不影响前端，但演示时设备数别堆太多。
5. **数据面吞吐**：当前 QUIC 数据面是纯 Python 实现，约 ~10MB/s；大数据块传输的演示会偏慢，注意 makespan 预期。
6. **Redis 客户端**锁定 5.0.x（见 requirements）；端口默认 server :8888、web :5000、device 数据面 :9999 起。

## 10. 非功能需求
- **性能**：监控 2 秒刷新流畅、不闪烁（建议数值/进度条做过渡而非整页重渲染）。
- **健壮性**：完善的 加载/空/错误/断连重连 状态；后端无设备/超时要友好提示，不要白屏或一直转圈。
- **离线**：不依赖运行时外网（CDN 字体/库都本地化）。
- **本地化**：界面中文为主，数字/时间格式规范。
- **可维护**：组件化、清晰的接口层（把“与桥接/Redis 的对接”收敛到一处，便于后端协议演进）。

## 11. 交付建议
- 一个**前端 SPA**（Vue 或 React 皆可），构建产物放到 `Offloading-System/dag_offload/static/`（或独立托管），
  入口由 `user_client.py` 提供。
- 桥接层可沿用/重写 `user_client.py`：**推荐做成干净的 JSON + WebSocket 接口**（监控/提交/结果/配置/算法），
  并在 server 端加 §9.1 的收包加固。
- 旧编译前端可保留在 `/legacy` 作回退参考，或直接替换。

---

## 附录 A · 环境变量清单（系统参数）
| 变量 | 含义 | 前端是否暴露 |
|---|---|---|
| `OPENSAI_PLACEMENT` | 放置模式 plan/smart/random | ✅ 安全可调 |
| `OPENSAI_W_XFER` / `OPENSAI_W_LOAD` | 放置代价权重(传输/负载) | ✅ 安全可调 |
| `OPENSAI_AUTO_MEM` | 自动内存定档(0/1) | ✅ 安全可调 |
| `OPENSAI_TARGET_XFER_S` | 自动带宽的目标传输时延(秒) | ✅ 安全可调 |
| `OPENSAI_DEFAULT_CORES` / `OPENSAI_DEFAULT_BW_MBPS` | 默认核/兜底带宽 | ◐ 可选 |
| `OPENSAI_DEVICE_BW_MBPS` / `OPENSAI_DATA_RATE_MBPS` | 设备出口带宽预算 | ◐ 可选 |
| `OPENSAI_PROBE_INTERVAL` / `OPENSAI_RESERVATION_TTL` | 链路探测间隔 / 预留回收 | ◐ 可选 |
| `OPENSAI_HOST` / `OPENSAI_PORT` / `OPENSAI_DATA_PORT` / `OPENSAI_TASK_WORKERS` | 主机/端口/数据端口/每设备worker数 | ❌ 只读·需重启 |

## 附录 B · 本地启动（联调用）
```powershell
powershell -ExecutionPolicy Bypass -File scripts\run.ps1 -WithWeb   # Redis + server + 1 device + web(:5000)
# 再加设备(演示放置对比)：
$env:OPENSAI_DATA_PORT=9998; .\.venv\Scripts\python.exe Offloading-System\dag_offload\run_device.py
```
- 监控视图自检：起栈后读 Redis 键 `OPENSAI_MONITOR_VIEW`（`scripts/show_monitor.py` 是一个只读查看器，可参考其读法）。
- 提交/结果联调：可参考 `scripts/run_tests.py` 里构造 DAG、收 `dagResult` 的最小写法。
