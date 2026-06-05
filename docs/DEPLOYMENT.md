# OpenSAI 多机部署说明（能否部署到不同设备？）

## 结论

**能。** OpenSAI 的通信是 **TCP 星型拓扑**：所有 device / user 都主动连接到中心
`server`，由 server 中转。因此跨主机部署只需要让所有机器都指向 **同一个 server 地址**。

- ✅ **跨主机的任务编排/卸载是可用的**：server 在主机 A，多个 device 在主机 B/C…，
  DAG 的依赖排序、子任务分发、结果回收都能跨主机正常工作。
- ⚠️ **有两个重要前提/限制**，见下方“限制”。

## 怎么部署（局域网，2 台及以上主机）

唯一必须改的就是把端点指向 server 的局域网 IP（已支持环境变量，无需改源码）。

**主机 A（server + redis）**
```powershell
# 启动 redis（与 server 同机即可）
& "$env:LOCALAPPDATA\opensai-redis\redis-server.exe" --port 6379 --save "" --appendonly no
# 让 server 绑定到 A 的局域网 IP（例：192.168.1.10）
$env:OPENSAI_HOST = "192.168.1.10"
python Offloading-System\dag_offload\server.py
```

**主机 B / C …（device）**
```powershell
$env:OPENSAI_HOST = "192.168.1.10"      # 指向 A 的 IP
python Offloading-System\dag_offload\device_client.py
```

**任意主机（Web 前端，可选）**
```powershell
$env:OPENSAI_HOST = "192.168.1.10"
python Offloading-System\dag_offload\user_client.py   # http://<该机>:5000
```

要点：
1. **所有机器设置 `OPENSAI_HOST` = server(A) 的局域网 IP**（端口默认 8888，可用 `OPENSAI_PORT` 改）。
2. **主机 A 防火墙放行 8888**（入站 TCP）。
3. **Redis 只在主机 A**（127.0.0.1 即可）——`device_client` 不直接连 Redis，只有 server/user 用。
4. device 在 B/C 上**只需能访问 A:8888**；A 不需要反向连 device（分发走已建立的 TCP 连接），
   所以 device 端在 NAT/防火墙后也没问题。

> 单机自测保持默认（`127.0.0.1`），`scripts\run.ps1` 不受影响。

## 限制（部署前务必了解）

1. **跨节点的“数据依赖”没有自动传输。** 当前实现里，DAG 的边只表示**执行顺序**
   （Node2 等 Node1 完成），并**不会把 Node1 产生的数据文件搬到运行 Node2 的另一台
   设备上**。子任务完成后只把“结果值/字符串”回传给 server，不在设备间传文件。
   - ✅ 适合：每个节点**自包含**的计算卸载（如 Pi / Sleep / 各自独立的推理）。
   - ❌ 不适合（需自行处理）：Node2 要读 Node1 在**另一台机器**上写的文件。
     解决办法：用**共享存储**（NFS/SMB 网络盘，把 `inDataPath/outDataPath` 指向它），
     或后续实现设备间文件传输（旧 `msg_transfer_tool_impl` 的 `send_p2p_big_msg`
     是为此设计的，但目前未接入主流程）。
2. **卸载决策是“随机/手动”，不是负载/网络感知。** `offloading_brain` 目前只有
   `random_strategy` 和 `direct_strategy`（按 `directAllocationId` 指定设备）。
3. **网络监测指标为 0。** 心跳里的带宽/时延是硬编码 0，所以没有“就近/低延迟”调度的依据
   （这是“适配优化组件/阶段 2”要补的部分）。

## 一句话总结
- **平台能跨设备跑**，把 `OPENSAI_HOST` 指到 server 的 IP、放行端口即可。
- **真正的“协同计算卸载”里，跨设备的数据流目前要靠共享存储兜底**；任务编排本身没问题。
- 想要“按算力/带宽智能分配”——那正是后续 **适配优化组件** 的工作。
