import "../modules.css";
import "./topology.css";
import { useState } from "react";
import { useStore } from "../../state/store";
import { Card, Button, Input, Select, Badge, StateView, Icon } from "../../components/ui";

// 设备拓扑编辑（spec §4）：编辑一套"情景(what-if)拓扑"——增删设备、设容量属性、连链路。
// 实时集群在「集群概览」只读；这里的拓扑用于规划与演示，本地持久化，可从实时集群一键导入。

export default function TopologyEditor() {
  const topo = useStore((s) => s.topology);
  const addDevice = useStore((s) => s.addTopoDevice);
  const updateDevice = useStore((s) => s.updateTopoDevice);
  const removeDevice = useStore((s) => s.removeTopoDevice);
  const addLink = useStore((s) => s.addTopoLink);
  const removeLink = useStore((s) => s.removeTopoLink);
  const importLive = useStore((s) => s.importLiveTopology);
  const reset = useStore((s) => s.resetTopology);

  const [lf, setLf] = useState({ from: "", to: "", mbps: 10, ms: 50 });
  const devById = Object.fromEntries(topo.devices.map((d) => [d.id, d]));
  const devOptions = [{ value: "", label: "选择设备" }, ...topo.devices.map((d) => ({ value: d.id, label: d.label }))];

  return (
    <div className="fade-in grid" style={{ gap: "var(--sp-5)" }}>
      <Card>
        <div className="row between wrap">
          <div>
            <h2 style={{ fontSize: "var(--fs-xl)" }}>设备拓扑编辑</h2>
            <p className="t-dim" style={{ marginTop: 6, maxWidth: 760 }}>
              编辑一套情景拓扑（设备 + 链路），用于规划与演示。实时集群在「集群概览」只读；本拓扑本地保存。
            </p>
          </div>
          <div className="row wrap" style={{ gap: "var(--sp-2)" }}>
            <Button icon={Icon.cube} onClick={importLive}>从实时集群导入</Button>
            <Button onClick={addDevice}>+ 新增设备</Button>
            <Button variant="ghost" onClick={reset}>清空</Button>
          </div>
        </div>
      </Card>

      <TopoPreview topo={topo} />

      <Card title="设备" sub={`${topo.devices.length} 台`}>
        {topo.devices.length === 0 ? (
          <StateView status="empty" glyph={Icon.cube} title="暂无设备"
            message="点击「新增设备」或「从实时集群导入」开始编辑拓扑。" />
        ) : (
          <table className="topo-table">
            <thead><tr><th>名称</th><th>并行槽位</th><th>核数</th><th>内存(GB)</th><th>网卡</th><th></th></tr></thead>
            <tbody>
              {topo.devices.map((d) => (
                <tr key={d.id}>
                  <td><Input value={d.label} onChange={(e) => updateDevice(d.id, { label: e.target.value })} /></td>
                  <td><Num v={d.slots} on={(v) => updateDevice(d.id, { slots: v })} /></td>
                  <td><Num v={d.cores} on={(v) => updateDevice(d.id, { cores: v })} /></td>
                  <td><Num v={d.memGB} on={(v) => updateDevice(d.id, { memGB: v })} /></td>
                  <td><Num v={d.nics} on={(v) => updateDevice(d.id, { nics: v })} /></td>
                  <td><Button variant="ghost" size="sm" onClick={() => removeDevice(d.id)}>删除</Button></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>

      <Card title="链路" sub={`${topo.links.length} 条`}>
        <div className="row wrap" style={{ gap: "var(--sp-2)", marginBottom: "var(--sp-3)", alignItems: "center" }}>
          <Select value={lf.from} onChange={(e) => setLf({ ...lf, from: e.target.value })} options={devOptions} />
          <span className="t-dim">↔</span>
          <Select value={lf.to} onChange={(e) => setLf({ ...lf, to: e.target.value })} options={devOptions} />
          <Input type="number" style={{ width: 120 }} value={lf.mbps} onChange={(e) => setLf({ ...lf, mbps: e.target.value })} placeholder="带宽 MB/s" />
          <Input type="number" style={{ width: 110 }} value={lf.ms} onChange={(e) => setLf({ ...lf, ms: e.target.value })} placeholder="时延 ms" />
          <Button onClick={() => { addLink(lf.from, lf.to, lf.mbps, lf.ms); setLf({ ...lf, from: "", to: "" }); }}>+ 添加链路</Button>
        </div>
        {topo.links.length === 0 ? (
          <p className="t-dim">暂无链路。选择两台设备并设定带宽/时延后添加。</p>
        ) : (
          <div className="grid" style={{ gap: "var(--sp-2)" }}>
            {topo.links.map((l, i) => (
              <div key={i} className="row between topo-link">
                <span className="row" style={{ gap: 8, alignItems: "center" }}>
                  <strong>{devById[l.from]?.label ?? l.from}</strong>
                  <span className="t-dim">↔</span>
                  <strong>{devById[l.to]?.label ?? l.to}</strong>
                  <Badge kind="neutral">{l.mbps} MB/s</Badge>
                  <Badge kind="neutral">{l.ms} ms</Badge>
                </span>
                <Button variant="ghost" size="sm" onClick={() => removeLink(i)}>删除</Button>
              </div>
            ))}
          </div>
        )}
      </Card>
    </div>
  );
}

function Num({ v, on }) {
  return <Input type="number" style={{ width: 90 }} value={v}
    onChange={(e) => on(Math.max(0, Number(e.target.value) || 0))} />;
}

// 轻量 2D 拓扑预览：环形布局放设备节点，画链路边（粗细随带宽）。
function TopoPreview({ topo }) {
  const n = topo.devices.length;
  if (n === 0) return null;
  const W = 720, H = 280, cx = W / 2, cy = H / 2, R = Math.min(W, H) / 2 - 50;
  const pos = {};
  topo.devices.forEach((d, i) => {
    const a = (i / n) * Math.PI * 2 - Math.PI / 2;
    pos[d.id] = { x: cx + R * Math.cos(a), y: cy + R * Math.sin(a) };
  });
  const maxBw = Math.max(1, ...topo.links.map((l) => l.mbps));
  return (
    <Card title="拓扑预览" sub="环形布局 · 边粗细随带宽">
      <svg viewBox={`0 0 ${W} ${H}`} className="topo-svg" role="img">
        {topo.links.map((l, i) => {
          const a = pos[l.from], b = pos[l.to];
          if (!a || !b) return null;
          return <line key={i} x1={a.x} y1={a.y} x2={b.x} y2={b.y}
            stroke="var(--accent)" strokeOpacity="0.5" strokeWidth={1 + 4 * (l.mbps / maxBw)} />;
        })}
        {topo.devices.map((d) => {
          const p = pos[d.id];
          return (
            <g key={d.id} transform={`translate(${p.x},${p.y})`}>
              <circle r="20" fill="var(--accent-soft)" stroke="var(--accent)" strokeWidth="1.5" />
              <text textAnchor="middle" dy="-26" fontSize="12" fill="var(--text-hi)">{d.label}</text>
              <text textAnchor="middle" dy="4" fontSize="10" fill="var(--text-dim)">{d.slots}槽/{d.cores}核</text>
            </g>
          );
        })}
      </svg>
    </Card>
  );
}
