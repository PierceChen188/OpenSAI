import { Icon, Badge } from "../ui";
import { useStore } from "../../state/store";
import { fmtClock } from "../../lib/format";

export const NAV = [
  { group: "概览", items: [
    { id: "overview", label: "集群概览", icon: Icon.cube, hero: true },
    { id: "topology", label: "拓扑编辑", icon: Icon.layers },
  ]},
  { group: "任务", items: [
    { id: "orchestrate", label: "任务编排", icon: Icon.flow },
    { id: "results", label: "任务与结果", icon: Icon.list },
  ]},
  { group: "决策", items: [
    { id: "placement", label: "放置决策", icon: Icon.layers },
    { id: "resources", label: "资源决策", icon: Icon.cpu },
    { id: "transfers", label: "数据传输", icon: Icon.link },
  ]},
  { group: "系统", items: [
    { id: "algorithms", label: "算法库", icon: Icon.box },
    { id: "params", label: "系统参数", icon: Icon.sliders },
  ]},
];

const CONN_LABEL = {
  online: { b: "已连接", s: "实时 · 2 秒刷新" },
  empty: { b: "无设备", s: "请启动设备" },
  offline: { b: "已断开", s: "重连中…" },
  connecting: { b: "连接中…", s: "" },
};

export default function Sidebar({ route, onNavigate }) {
  const conn = useStore((s) => s.conn);
  const lastTs = useStore((s) => s.lastTs);
  const onlineDevices = useStore((s) => s.summary?.onlineDevices ?? 0);
  const tasks = useStore((s) => s.tasks);
  const running = tasks.filter((t) => t.status === "running" || t.status === "queued").length;

  const c = CONN_LABEL[conn] || CONN_LABEL.connecting;

  return (
    <aside className="sidebar">
      <div className="brand">
        <span className="mark"><Icon.zap size={20} /></span>
        <div>
          <div className="name">OpenSAI</div>
          <div className="tagline">卸载控制台</div>
        </div>
      </div>

      {NAV.map((g) => (
        <div key={g.group}>
          <div className="nav-group-label">{g.group}</div>
          {g.items.map((it) => {
            const I = it.icon;
            const badge =
              it.id === "overview" && conn === "online" ? <Badge kind="neutral">{onlineDevices}</Badge> :
              it.id === "results" && running > 0 ? <Badge kind="running">{running}</Badge> : null;
            return (
              <div
                key={it.id}
                className={`nav-item ${route === it.id ? "active" : ""}`}
                onClick={() => onNavigate(it.id)}
              >
                <I size={18} />
                <span>{it.label}</span>
                {badge}
              </div>
            );
          })}
        </div>
      ))}

      <div className="sidebar-foot">
        <div className="conn-chip">
          <span className={`dot ${conn}`} />
          <span className="txt">
            <b>{c.b}</b>
            <small>{conn === "online" && lastTs ? `更新于 ${fmtClock(lastTs / 1000)}` : c.s}</small>
          </span>
        </div>
      </div>
    </aside>
  );
}
