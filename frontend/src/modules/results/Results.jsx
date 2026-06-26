import "../modules.css";
import { useStore } from "../../state/store";
import { Card, Stat, Badge, StateView, Button, Icon } from "../../components/ui";
import { fmtDuration, fmtBytes, fmtDateTime, fmtClock } from "../../lib/format";

const STATUS_KIND = { queued: "queued", running: "running", completed: "ok", failed: "err", timeout: "warn" };
const STATUS_LABEL = { queued: "排队", running: "运行中", completed: "已完成", failed: "失败", timeout: "超时" };
const st = (s) => STATUS_LABEL[s] || s;

export default function Results({ navigate }) {
  const tasks = useStore((s) => s.tasks);
  const selectedName = useStore((s) => s.selectedTaskName);
  const selectTask = useStore((s) => s.selectTask);

  if (tasks.length === 0)
    return (
      <StateView
        status="empty" glyph={Icon.list}
        title="暂无任务"
        message="已提交的任务及其运行结果会显示在这里。先去编排一个 DAG 吧。"
        action={<Button variant="primary" icon={Icon.flow} onClick={() => navigate("orchestrate")}>去编排任务</Button>}
      />
    );

  const selected = tasks.find((t) => t.taskName === selectedName) || tasks[0];

  return (
    <div className="results-grid fade-in">
      <Card title="任务" sub={`已提交 ${tasks.length} 个`} bodyClass="">
        <div>
          {tasks.map((t) => (
            <div key={t.taskName} className={`task-row ${selected?.taskName === t.taskName ? "selected" : ""}`}
              onClick={() => selectTask(t.taskName)}>
              <div style={{ minWidth: 0, flex: 1 }}>
                <div className="nm" style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.taskName}</div>
                <div className="meta">{t.nodes?.length ?? 0} 节点 · {t.submittedAt ? fmtClock(t.submittedAt / 1000) : "—"}</div>
              </div>
              <Badge kind={STATUS_KIND[t.status] || "neutral"} dot live={t.status === "running"}>{st(t.status)}</Badge>
            </div>
          ))}
        </div>
      </Card>

      {selected ? <TaskDetail task={selected} /> : <Card><StateView status="empty" title="请选择任务" /></Card>}
    </div>
  );
}

function TaskDetail({ task }) {
  const nodes = task.nodes || [];
  const devicesUsed = new Set(nodes.map((n) => n.deviceId).filter((d) => d != null)).size;
  const errors = nodes.filter((n) => n.status === 2 || n.error).length;
  const crossEdges = countCrossDeviceEdges(nodes);
  const totalEdges = nodes.reduce((a, n) => a + (n.deps?.length || depsFromAfter(n)), 0);

  return (
    <div className="grid" style={{ gap: "var(--sp-4)" }}>
      <Card title={task.taskName} sub={task.submittedAt ? `提交于 ${fmtDateTime(task.submittedAt / 1000)}` : undefined}
        actions={<Badge kind={STATUS_KIND[task.status] || "neutral"} dot live={task.status === "running"}>{st(task.status)}</Badge>}>
        <div className="stat-grid" style={{ marginBottom: 0 }}>
          <Stat label="总完成时间" value={task.finishedTime != null ? fmtDuration(task.finishedTime) : "—"} accent="cpu" foot="makespan" />
          <Stat label="节点数" value={nodes.length} accent="violet" />
          <Stat label="跨设备" value={crossEdges} unit={totalEdges ? `/ ${totalEdges}` : ""} accent="bw" foot="传输 / 边数" />
          <Stat label="使用设备" value={devicesUsed || "—"} accent="store" />
          <Stat label="错误数" value={errors} accent={errors ? "store" : "cpu"} />
        </div>
      </Card>

      <Card title="节点明细" sub="每个子任务的放置、耗时与产出">
        <div className="tbl-wrap">
          <table className="tbl">
            <thead>
              <tr><th>节点</th><th>算法</th><th>设备</th><th className="num">耗时</th><th className="num">产出</th><th>状态</th></tr>
            </thead>
            <tbody>
              {nodes.map((n) => {
                const err = n.status === 2 || n.error;
                return (
                  <tr key={n.id} className={err ? "err-row" : ""}>
                    <td><b className="t-hi">{n.name || `#${n.id}`}</b></td>
                    <td className="mono" style={{ fontSize: "var(--fs-xs)" }}>{(n.taskAlgorithm || "").replace("Algorithm_", "") || "—"}</td>
                    <td className="mono">{n.deviceId ?? "—"}</td>
                    <td className="num">{n.delay != null ? fmtDuration(n.delay) : "—"}</td>
                    <td className="num">{n.outBytes != null ? fmtBytes(n.outBytes) : "—"}</td>
                    <td>{err ? <Badge kind="err" dot>错误</Badge> : n.status === 1 ? <Badge kind="ok" dot>完成</Badge> : <Badge kind="running" dot live>运行中</Badge>}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {task.status === "running" && (
          <p className="t-faint" style={{ fontSize: "var(--fs-sm)", marginTop: "var(--sp-3)" }}>
            当桥接开始转发 <code>dagResult</code> 事件后（规格 §14.2），逐节点结果会实时刷新。
          </p>
        )}
      </Card>
    </div>
  );
}

function depsFromAfter() { return 0; }
function countCrossDeviceEdges(nodes) {
  const dev = new Map(nodes.map((n) => [n.id, n.deviceId]));
  let n = 0;
  nodes.forEach((node) => (node.deps || []).forEach((d) => {
    if (dev.get(d) != null && node.deviceId != null && dev.get(d) !== node.deviceId) n++;
  }));
  return n;
}
