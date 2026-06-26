import "../modules.css";
import "./decisions.css";
import { useStore } from "../../components/../state/store";
import { Card, Badge, StateView, Icon, Meter } from "../../components/ui";

// Placement / Resource / Transfer decision process (spec §5/§6/§8). The live
// "decision events" (§14.2) are not yet emitted by the hub, so these views
// document the decision chain and surface what IS derivable today, degrading
// honestly with a clear "awaiting event stream" state.

const CONTENT = {
  placement: {
    title: "放置决策",
    lead: "每个子任务为什么放在这台设备 —— 候选集合、健康预筛、代价打分与最终选中。",
    steps: [
      { n: 1, k: "候选集合", d: "本轮参与竞争该子任务的设备。" },
      { n: 2, k: "健康预筛", d: "过载设备被标记为淘汰并给出原因；若全部过载则退回全量。" },
      { n: 3, k: "代价打分", d: "传输代价（按实测链路拉前驱产物）+ 负载代价（预留任务数 ÷ 槽位），按 W_XFER / W_LOAD 加权。" },
      { n: 4, k: "选中", d: "综合代价最低者胜出。若与输入数据同机（就近），会提示避免了一次跨设备传输。" },
    ],
    event: "放置决策事件",
  },
  resources: {
    title: "资源决策",
    lead: "四维资源档位（CPU / 内存 / 带宽 / 存储）是如何决定的，以及大脑账本如何随之增减。",
    steps: [
      { n: 1, k: "是否声明", d: "节点是否显式填了 cores / mem_mb / bw_mbps / storage_mb。" },
      { n: 2, k: "自动定档", d: "内存与存储按算法历史 × 系数；带宽按前驱产出 ÷ 目标传输时延。" },
      { n: 3, k: "按设备收口", d: "最终档位被（总 − 已承诺）× 安全系数夹住：想要 vs 实际给 vs 还剩。" },
      { n: 4, k: "账本变化", d: "派发即扣减，完成即归还；超时预留自动回收。" },
    ],
    event: "资源决策事件",
  },
  transfers: {
    title: "数据传输",
    lead: "一条逻辑传输可以并行走多条物理路径 —— 带宽叠加。",
    steps: [
      { n: 1, k: "本地直读", d: "上下游同机 → 本地读取，跳过网络。" },
      { n: 2, k: "单链路直连", d: "一条 QUIC 连接，源 → 目的。" },
      { n: 3, k: "中继多链路", d: "直连带宽不足 → 经中继设备开第二条路径，分条带并行传输后重组。" },
      { n: 4, k: "多网卡 / 叠加", d: "多网卡源跨网卡分流；中继 + 多网卡可叠加。" },
    ],
    event: "传输计划事件",
  },
};

const KIND_KEY = { placement: "placement", resources: "resource", transfers: "transfer" };

export default function DecisionView({ kind }) {
  const c = CONTENT[kind];
  const demo = useStore((s) => s.demoMode);
  const params = useStore((s) => s.params);
  const events = useStore((s) => s.events);
  const rows = events.filter((e) => e.kind === KIND_KEY[kind]).slice(-40).reverse();

  return (
    <div className="fade-in grid" style={{ gap: "var(--sp-5)" }}>
      <Card>
        <div className="decision-lead">
          <div className="row between">
            <div>
              <h2 style={{ fontSize: "var(--fs-xl)" }}>{c.title}</h2>
              <p className="t-dim" style={{ marginTop: 6, maxWidth: 720 }}>{c.lead}</p>
            </div>
            <Badge kind="info">过程视图</Badge>
          </div>
          <div className="decision-chain">
            {c.steps.map((s, i) => (
              <div className="chain-step" key={s.n}>
                <div className="chain-num">{s.n}</div>
                <div>
                  <div className="chain-k">{s.k}</div>
                  <div className="chain-d">{s.d}</div>
                </div>
                {i < c.steps.length - 1 && <span className="chain-arrow"><Icon.chevron size={16} /></span>}
              </div>
            ))}
          </div>
        </div>
      </Card>

      {kind === "placement" && (
        <Card title="当前权重" sub="实时放置打分旋钮（系统参数）">
          <div className="row wrap" style={{ gap: "var(--sp-3)" }}>
            <Badge kind="neutral">模式：{params.placementMode}</Badge>
            <Badge kind="neutral">W_XFER：{params.wXfer.toFixed(1)}</Badge>
            <Badge kind="neutral">W_LOAD：{params.wLoad.toFixed(1)}</Badge>
            <Badge kind={params.healthPrefilter ? "ok" : "warn"}>健康预筛：{params.healthPrefilter ? "开" : "关"}</Badge>
          </div>
        </Card>
      )}

      {kind === "transfers" && demo && <TransferDemo />}

      <Card title="实时决策流" sub={`中枢推送的结构化${c.event} · 最新 ${rows.length} 条`}>
        {rows.length === 0 ? (
          <StateView
            status="empty"
            glyph={Icon.flow}
            title="暂无决策事件"
            message={`提交一个任务后，中枢会通过桥接实时推送${c.event}，这里逐子任务演示决策过程。`}
          />
        ) : (
          <div className="grid" style={{ gap: "var(--sp-3)" }}>
            {rows.map((e) => <EventRow key={e.seq} kind={kind} e={e} />)}
          </div>
        )}
      </Card>
    </div>
  );
}

function EventRow({ kind, e }) {
  if (kind === "placement") {
    return (
      <div className="event-row">
        <div className="row between wrap">
          <strong>节点 {e.node_id} · {e.algorithm}</strong>
          <span className="row" style={{ gap: 6 }}>
            <Badge kind="neutral">{e.mode}</Badge>
            <Badge kind="ok">选中 {e.chosen}</Badge>
            {e.locality_hit && <Badge kind="info">就近命中</Badge>}
          </span>
        </div>
        <div className="t-dim" style={{ fontSize: "var(--fs-sm)", margin: "4px 0" }}>
          候选 {e.candidates?.length ?? 0} · 健康预筛淘汰 {e.filtered?.length ?? 0}
          {e.filtered?.length ? `（${e.filtered.map((f) => f.device).join(", ")}）` : ""}
        </div>
        {e.scores?.length > 0 && (
          <table className="event-table">
            <thead><tr><th>设备</th><th>传输代价(s)</th><th>负载代价</th><th>综合代价</th></tr></thead>
            <tbody>
              {e.scores.slice(0, 5).map((s) => (
                <tr key={s.device} className={s.device === e.chosen ? "is-chosen" : ""}>
                  <td>{s.device}{s.device === e.chosen ? " ★" : ""}</td>
                  <td className="mono">{s.xfer_s}</td><td className="mono">{s.load}</td><td className="mono">{s.cost}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    );
  }
  if (kind === "resources") {
    const dim = (label, d, unit) => (
      <Badge kind={d?.auto ? "info" : "neutral"}>
        {label}：{d?.final ?? "默认"}{d?.final != null ? unit : ""}{d?.declared != null ? "（声明）" : d?.auto ? "（自动）" : ""}
      </Badge>
    );
    return (
      <div className="event-row">
        <div className="row between wrap">
          <strong>节点 {e.node_id} · {e.algorithm}</strong>
          <Badge kind="ok">{e.device}</Badge>
        </div>
        <div className="row wrap" style={{ gap: 6, marginTop: 6 }}>
          {dim("核", e.cores, "")}
          {dim("内存", e.mem_mb, " MB")}
          {dim("存储", e.storage_mb, " MB")}
          <Badge kind="neutral">已承诺存储 {e.committed_storage_mb ?? 0} MB</Badge>
        </div>
      </div>
    );
  }
  // transfers
  return (
    <div className="event-row">
      <div className="row between wrap">
        <strong>节点 {e.consumer_node} ← 前驱 {e.pred_id}（源 {e.src_device}）</strong>
        <span className="row" style={{ gap: 6 }}>
          {e.local_read
            ? <Badge kind="ok">本地直读</Badge>
            : <Badge kind={e.multipath ? "info" : "neutral"}>{e.multipath ? `多链路 ${e.paths?.length} 路` : "单链路"}</Badge>}
        </span>
      </div>
      <div className="t-dim" style={{ fontSize: "var(--fs-sm)", margin: "4px 0" }}>
        产物 {(e.size / 1048576).toFixed(1)} MB · 限速 {e.rate_mbps ?? "不限"}{e.rate_mbps ? " MB/s" : ""}
      </div>
      {!e.local_read && e.paths?.length > 0 && (
        <div className="row wrap" style={{ gap: 6 }}>
          {e.paths.map((p, i) => (
            <Badge key={i} kind={p.type === "relay" ? "warn" : "neutral"}>
              {p.type === "relay" ? "中继" : "直连"} {p.host}:{p.port}
            </Badge>
          ))}
        </div>
      )}
    </div>
  );
}

function TransferDemo() {
  // Illustrative multipath breakdown shown only in demo mode.
  const paths = [
    { kind: "直连", via: "edge-01 → edge-02", nic: "eth0", rate: 48 },
    { kind: "中继", via: "edge-01 → edge-03 → edge-02", nic: "eth0", rate: 31 },
    { kind: "多网卡", via: "edge-01 → edge-02", nic: "eth1", rate: 22 },
  ];
  const agg = paths.reduce((a, p) => a + p.rate, 0);
  return (
    <Card title="多链路传输示例" sub="22.5 MB 产物 · edge-01 → edge-02（演示）">
      <div className="grid" style={{ gap: "var(--sp-3)" }}>
        {paths.map((p, i) => (
          <Meter key={i} kind={["bw", "store", "mem"][i]} label={`${p.kind} · ${p.via} · ${p.nic}`}
            used={p.rate} total={agg} valueText={`${p.rate} MB/s`} />
        ))}
        <div className="row between" style={{ marginTop: 4 }}>
          <span className="t-dim">聚合（3 条并行路径）</span>
          <span className="mono t-hi" style={{ fontSize: "var(--fs-lg)" }}>{agg} MB/s</span>
        </div>
      </div>
    </Card>
  );
}
