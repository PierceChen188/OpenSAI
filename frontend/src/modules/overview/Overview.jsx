import "../modules.css";
import { useState } from "react";
import { useStore } from "../../state/store";
import { Card, Stat, Badge, Meter, StateView, Button, Icon } from "../../components/ui";
import { utilization } from "../../api/normalize";
import { fmtMB, fmtMbps, fmtNum, fmtMs, fmtPct, fmtBytes, fmtDuration } from "../../lib/format";
import Stage3D from "../stage/Stage3D";

// Merged overview: the 3D topology stage on the left, the per-device ledger on
// the right (selection synced both ways), with cluster summary on top and link /
// algorithm-profile tables below. (Spec §3 + §11 combined onto one landing page.)
export default function Overview() {
  const conn = useStore((s) => s.conn);
  const devices = useStore((s) => s.devices);
  const links = useStore((s) => s.links);
  const summary = useStore((s) => s.summary);
  const algoHistory = useStore((s) => s.algoHistory);
  const setDemoMode = useStore((s) => s.setDemoMode);
  const [sel, setSel] = useState(null);

  if (conn === "connecting") return <StateView status="loading" title="正在连接中枢…" />;
  if (conn === "offline")
    return (
      <StateView
        status="error"
        title="无法连接桥接服务"
        message="无法连接 OpenSAI 桥接服务，将每 2 秒自动重试。请确认 user_client.py 已运行，或启用演示数据预览控制台。"
        action={<Button variant="primary" icon={Icon.play} onClick={() => setDemoMode(true)}>启用演示数据</Button>}
      />
    );
  if (conn === "empty" || devices.length === 0)
    return (
      <StateView
        status="empty"
        title="暂无在线设备"
        message="中枢已连接，但尚未有边缘设备注册。请启动设备（run_device.py）以填充集群。"
        action={<Button variant="ghost" icon={Icon.play} onClick={() => setDemoMode(true)}>使用演示数据预览</Button>}
      />
    );

  return (
    <div className="fade-in">
      <div className="stat-grid">
        <Stat label="在线设备" value={fmtNum(summary.onlineDevices)} accent="cpu" foot={`共 ${devices.length} 台`} />
        <Stat label="可用核心" value={fmtNum(summary.availableCores)} unit={`/ ${fmtNum(summary.totalCores)}`} accent="cpu" />
        <Stat label="在飞任务" value={fmtNum(summary.inflightTasks)} accent="violet" foot={`${fmtNum(summary.runningTasks)} 运行中`} />
        <Stat label="放置预留" value={fmtNum(summary.placementReserved)} accent="violet" />
        <Stat label="已承诺出口带宽" value={fmtMbps(summary.committedBw, 1)} accent="bw" />
        <Stat label="已承诺存储" value={fmtMB(summary.committedStorage)} accent="store" />
      </div>

      <div className="overview-main">
        <div className="card overview-stage-card">
          <div className="overview-stage">
            <Stage3D compact selectedId={sel} onSelect={setSel} />
          </div>
        </div>

        <div className="overview-devices">
          <div className="section-hd">
            <div><h3>设备</h3><span className="sub">每台设备的四维资源账本</span></div>
            <Badge kind="info" live>实时</Badge>
          </div>
          {devices.map((d) => (
            <DeviceCard key={d.id} d={d} selected={sel === d.id} onClick={() => setSel(sel === d.id ? null : d.id)} />
          ))}
        </div>
      </div>

      <div className="detail-grid">
        <LinksCard links={links} />
        <AlgoProfilesCard history={algoHistory} />
      </div>
    </div>
  );
}

function CpuRing({ value }) {
  const r = 22, c = 2 * Math.PI * r;
  const pct = Math.max(0, Math.min(1, (value ?? 0) / 100));
  return (
    <div style={{ position: "relative", width: 60, height: 60, flex: "none" }}>
      <svg width="60" height="60" style={{ transform: "rotate(-90deg)" }}>
        <circle cx="30" cy="30" r={r} fill="none" stroke="var(--bg-inset)" strokeWidth="5" />
        <circle
          cx="30" cy="30" r={r} fill="none"
          stroke="var(--cpu)" strokeWidth="5" strokeLinecap="round"
          strokeDasharray={c} strokeDashoffset={c * (1 - pct)}
          style={{ transition: "stroke-dashoffset var(--dur-slow) var(--ease)", filter: "drop-shadow(0 0 4px var(--accent-glow))" }}
        />
      </svg>
      <div style={{ position: "absolute", inset: 0, display: "grid", placeItems: "center", fontSize: "var(--fs-sm)", fontWeight: 700, color: "var(--text-hi)", fontVariantNumeric: "tabular-nums" }}>
        {fmtPct(value)}
      </div>
    </div>
  );
}

export function DeviceCard({ d, selected, onClick }) {
  const usedStorage = d.storage.totalMb - d.storage.freeMb;
  return (
    <div className={`device-card ${d.online ? "" : "offline"} ${selected ? "selected" : ""}`} onClick={onClick}
      style={{ cursor: onClick ? "pointer" : "default", borderColor: selected ? "var(--accent)" : undefined, boxShadow: selected ? "var(--shadow-glow)" : undefined }}>
      <div className="hd">
        <span className="icon-wrap"><Icon.server size={19} /></span>
        <div style={{ minWidth: 0 }}>
          <div className="id">{d.id}</div>
          <div className="ip">{d.ip || "—"}{d.port ? `:${d.port}` : ""}</div>
        </div>
        <div className="spacer" />
        {d.strength === "strong" && <Badge kind="info">强算力</Badge>}
        <Badge kind={d.online ? "ok" : "err"} dot>{d.online ? "在线" : "离线"}</Badge>
      </div>

      <div className="cpu-ring-row">
        <CpuRing value={d.cpuUsage} />
        <div className="chips" style={{ flex: 1 }}>
          <Badge kind="neutral">{d.workerSlots} 槽位</Badge>
          <Badge kind="neutral">{d.runningTasks} 运行中</Badge>
          <Badge kind="neutral">{d.placementReserved} 预留</Badge>
          <Badge kind="neutral">{d.nics} 网卡</Badge>
          <Badge kind="neutral">RTT {fmtMs(d.rtt)}</Badge>
        </div>
      </div>

      <div className="meters">
        <Meter kind="cpu" label="核心（已承诺/总）" used={d.cores.committed} total={d.cores.total}
          valueText={`${fmtNum(d.cores.committed)} / ${fmtNum(d.cores.total)} · 剩 ${fmtNum(d.cores.available)}`} />
        <Meter kind="mem" label="内存" used={d.mem.used} total={d.mem.total}
          valueText={`${d.mem.used.toFixed(1)} / ${d.mem.total.toFixed(0)} GB`} />
        <Meter kind="bw" label="已承诺出口带宽" used={d.bw.committedMbps} total={Math.max(d.bw.bandwidth, d.bw.committedMbps, 1)}
          valueText={`${fmtMbps(d.bw.committedMbps)}`} />
        <Meter kind="store" label="存储（已用/已承诺）" used={usedStorage} committed={d.storage.committedMb} total={Math.max(d.storage.totalMb, 1)}
          valueText={`${fmtMB(usedStorage)} · 承诺 ${fmtMB(d.storage.committedMb)}`} />
      </div>
    </div>
  );
}

function LinksCard({ links }) {
  return (
    <Card title="设备链路" sub="实测带宽与时延（链路遥测）">
      {links.length === 0 ? (
        <StateView status="empty" title="暂无链路遥测" message="设备相互探测后才会有链路数据。新集群上此处可能为空。" />
      ) : (
        <div className="tbl-wrap">
          <table className="tbl">
            <thead><tr><th>链路</th><th className="num">带宽</th><th className="num">时延</th></tr></thead>
            <tbody>
              {links.map((l) => (
                <tr key={`${l.a}-${l.b}`}>
                  <td><span className="mono">{l.a}</span> ↔ <span className="mono">{l.b}</span></td>
                  <td className="num">{l.bandwidth ? fmtMbps(l.bandwidth) : "—"}</td>
                  <td className="num">{l.delay ? fmtMs(l.delay) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

function AlgoProfilesCard({ history }) {
  const rows = history ? Object.entries(history) : [];
  return (
    <Card title="算法历史画像" sub="实测资源 / 耗时统计">
      {rows.length === 0 ? (
        <StateView status="empty" title="暂无画像" message="画像会随算法运行逐步累积，用于自动定档（内存/存储）与耗时预测。" />
      ) : (
        <div className="tbl-wrap">
          <table className="tbl">
            <thead><tr><th>算法</th><th className="num">边际内存</th><th className="num">耗时</th><th className="num">产出</th><th className="num">样本</th></tr></thead>
            <tbody>
              {rows.map(([name, h]) => (
                <tr key={name}>
                  <td className="mono" style={{ fontSize: "var(--fs-xs)" }}>{name}</td>
                  <td className="num">{h.task_mem_mb ? fmtMB(h.task_mem_mb) : "—"}</td>
                  <td className="num">{h.avg_runtime_s != null ? fmtDuration(h.avg_runtime_s) : "—"}</td>
                  <td className="num">{h.out_storage_mb != null ? fmtMB(h.out_storage_mb) : (h.out_bytes != null ? fmtBytes(h.out_bytes) : "—")}</td>
                  <td className="num">{h.samples ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}
