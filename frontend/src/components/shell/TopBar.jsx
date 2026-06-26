import { Segmented, IconButton, Icon, Toggle, Button } from "../ui";
import { useStore } from "../../state/store";

const TITLES = {
  overview: { t: "集群概览", s: "三维拓扑、放置、传输与资源态势统一呈现" },
  topology: { t: "拓扑编辑", s: "编辑情景拓扑（设备 + 链路），用于规划与演示" },
  orchestrate: { t: "任务编排", s: "绘制 DAG 并提交到集群" },
  results: { t: "任务与结果", s: "已提交任务、运行结果与对比" },
  placement: { t: "放置决策", s: "每个子任务为什么放在这台设备" },
  resources: { t: "资源决策", s: "四维资源档位是如何决定的" },
  transfers: { t: "数据传输", s: "设备间的多链路数据面传输" },
  algorithms: { t: "算法库", s: "已注册算法与插件上传" },
  params: { t: "系统参数", s: "在线可调的策略与数据面参数" },
};

const MODES = [
  { value: "live", label: "实时" },
  { value: "replay", label: "回放" },
  { value: "edit", label: "编辑" },
  { value: "compare", label: "对比" },
];

export default function TopBar({ route }) {
  const viewMode = useStore((s) => s.viewMode);
  const setViewMode = useStore((s) => s.setViewMode);
  const demoMode = useStore((s) => s.demoMode);
  const setDemoMode = useStore((s) => s.setDemoMode);
  const refreshNow = useStore((s) => s.refreshNow);
  const runDemo = useStore((s) => s.runDemo);
  const conn = useStore((s) => s.conn);

  const title = TITLES[route] || { t: route, s: "" };
  const showModes = route === "overview";

  return (
    <header className="topbar">
      <div>
        <div className="page-title">{title.t}</div>
        <div className="page-sub">{title.s}</div>
      </div>
      <div className="spacer" />

      {showModes && <Segmented value={viewMode} onChange={setViewMode} options={MODES} />}

      <Button variant="primary" size="sm" icon={Icon.play} disabled={conn !== "online" && !demoMode}
        onClick={() => runDemo()}>一键演示</Button>


      <div className="row" style={{ gap: "var(--sp-2)" }} title="演示数据模式：不连后端，显示内置假数据用于离线预览/截图。真实联调请关闭。">
        <span className="t-faint" style={{ fontSize: "var(--fs-xs)", fontWeight: 600 }}>演示数据</span>
        <Toggle checked={demoMode} onChange={setDemoMode} />
      </div>

      <IconButton icon={Icon.refresh} title="立即刷新" onClick={refreshNow} />
    </header>
  );
}
