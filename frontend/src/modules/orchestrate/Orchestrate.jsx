import "../modules.css";
import { useMemo, useState } from "react";
import { useStore } from "../../state/store";
import { Card, Button, Icon, Field, Input, Select, Badge, IconButton, Tag } from "../../components/ui";
import { TEMPLATES, computeLayers, hasCycle, buildDagData, newNodeId } from "./templates";

export default function Orchestrate({ navigate }) {
  const algorithms = useStore((s) => s.algorithms);
  const policies = useStore((s) => s.policies);
  const submitDag = useStore((s) => s.submitDag);
  const toast = useStore((s) => s.toast);
  const paramMode = useStore((s) => s.params.placementMode);

  const algoOptions = algorithms.length ? algorithms : ["Algorithm_Pi_Calculation"];
  const [taskName, setTaskName] = useState("task-" + Math.floor(Date.now() / 1000));
  const [policy, setPolicy] = useState(paramMode);
  const [nodes, setNodes] = useState(TEMPLATES[3].nodes.map((n) => ({ ...n, res: { ...n.res } })));
  const [selId, setSelId] = useState(nodes[0]?.id ?? null);
  const [submitting, setSubmitting] = useState(false);

  const sel = nodes.find((n) => n.id === selId) || null;
  const cyclic = useMemo(() => hasCycle(nodes), [nodes]);

  function loadTemplate(t) {
    const copy = t.nodes.map((n) => ({ ...n, res: { ...n.res } }));
    setNodes(copy);
    setSelId(copy[0]?.id ?? null);
    setTaskName(t.key + "-" + Math.floor(Date.now() / 1000));
  }

  function addNode() {
    const id = newNodeId();
    const n = { id, name: `node-${nodes.length + 1}`, algo: algoOptions[0], deps: selId ? [selId] : [], res: {} };
    setNodes((p) => [...p, n]);
    setSelId(id);
  }
  function removeNode(id) {
    setNodes((p) => p.filter((n) => n.id !== id).map((n) => ({ ...n, deps: n.deps.filter((d) => d !== id) })));
    if (selId === id) setSelId(null);
  }
  function patchNode(id, patch) {
    setNodes((p) => p.map((n) => (n.id === id ? { ...n, ...patch } : n)));
  }
  function patchRes(id, patch) {
    setNodes((p) => p.map((n) => (n.id === id ? { ...n, res: { ...n.res, ...patch } } : n)));
  }

  async function submit() {
    if (!taskName.trim()) return toast("请填写任务名称", "err");
    if (nodes.length === 0) return toast("请至少添加一个节点", "err");
    if (cyclic) return toast("DAG 存在环 —— 依赖必须无环", "err");
    setSubmitting(true);
    const dag = buildDagData(taskName.trim(), nodes, policy);
    const r = await submitDag(dag);
    setSubmitting(false);
    if (r.ok) navigate("results");
  }

  return (
    <div className="fade-in">
      <div className="template-row">
        {TEMPLATES.map((t) => (
          <button key={t.key} className="template-chip" title={t.desc} onClick={() => loadTemplate(t)}>
            {t.name}
          </button>
        ))}
      </div>

      <div className="orch-grid">
        <Card
          title="DAG 预览"
          sub={`${nodes.length} 个节点 · ${nodes.reduce((a, n) => a + n.deps.length, 0)} 条边`}
          actions={
            <>
              {cyclic && <Badge kind="err" dot>检测到环</Badge>}
              <Button size="sm" variant="ghost" icon={Icon.plus} onClick={addNode}>节点</Button>
            </>
          }
          bodyClass=""
        >
          <DagCanvas nodes={nodes} selId={selId} onSelect={setSelId} />
        </Card>

        <div className="grid" style={{ gap: "var(--sp-4)" }}>
          <Card title="任务">
            <div className="grid" style={{ gap: "var(--sp-3)" }}>
              <Field label="任务名称">
                <Input value={taskName} onChange={(e) => setTaskName(e.target.value)} placeholder="my-task" />
              </Field>
              <Field label="放置策略" hint="plan = 整图前瞻 · smart = 逐节点贪心 · random = 对照基线">
                <Select value={policy} onChange={(e) => setPolicy(e.target.value)}
                  options={(policies.length ? policies : ["plan", "smart", "random"])} />
              </Field>
              <Button variant="primary" icon={Icon.play} disabled={submitting || cyclic} onClick={submit}>
                {submitting ? "提交中…" : "提交任务"}
              </Button>
            </div>
          </Card>

          <Card title="节点编辑器" sub={sel ? `#${sel.id}` : "请选择节点"}>
            {!sel ? (
              <p className="t-faint" style={{ fontSize: "var(--fs-sm)" }}>在预览中点选一个节点进行编辑，或新增一个节点。</p>
            ) : (
              <NodeEditor
                node={sel} nodes={nodes} algoOptions={algoOptions}
                onName={(v) => patchNode(sel.id, { name: v })}
                onAlgo={(v) => patchNode(sel.id, { algo: v })}
                onDeps={(deps) => patchNode(sel.id, { deps })}
                onRes={(patch) => patchRes(sel.id, patch)}
                onRemove={() => removeNode(sel.id)}
              />
            )}
          </Card>
        </div>
      </div>
    </div>
  );
}

function DagCanvas({ nodes, selId, onSelect }) {
  const layout = useMemo(() => {
    const depth = computeLayers(nodes);
    const layers = {};
    nodes.forEach((n) => {
      const d = depth.get(n.id) || 0;
      (layers[d] = layers[d] || []).push(n);
    });
    const maxLayer = Math.max(0, ...Object.keys(layers).map(Number));
    const pos = new Map();
    Object.entries(layers).forEach(([d, ns]) => {
      ns.forEach((n, i) => {
        const x = maxLayer === 0 ? 50 : 12 + (Number(d) / maxLayer) * 76; // %
        const y = ns.length === 1 ? 50 : 14 + (i / (ns.length - 1)) * 72;
        pos.set(n.id, { x, y });
      });
    });
    return pos;
  }, [nodes]);

  const edges = [];
  nodes.forEach((n) => n.deps.forEach((d) => {
    if (layout.has(d) && layout.has(n.id)) edges.push({ from: layout.get(d), to: layout.get(n.id), key: `${d}-${n.id}` });
  }));

  return (
    <div className="dag-canvas" style={{ minHeight: 420 }}>
      <svg style={{ position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none" }}>
        <defs>
          <marker id="arrow" markerWidth="9" markerHeight="9" refX="7" refY="3" orient="auto">
            <path d="M0,0 L7,3 L0,6 Z" fill="rgba(56,225,255,0.55)" />
          </marker>
        </defs>
        {edges.map((e) => (
          <line key={e.key}
            x1={`${e.from.x}%`} y1={`${e.from.y}%`} x2={`${e.to.x}%`} y2={`${e.to.y}%`}
            stroke="rgba(56,225,255,0.4)" strokeWidth="1.5" markerEnd="url(#arrow)" />
        ))}
      </svg>
      {nodes.map((n) => {
        const p = layout.get(n.id);
        return (
          <div key={n.id} className={`dag-node ${selId === n.id ? "selected" : ""}`}
            style={{ left: `${p.x}%`, top: `${p.y}%` }} onClick={() => onSelect(n.id)}>
            {n.res?.fixedDevice != null && n.res.fixedDevice >= 0 && <span className="pin"><Icon.pin size={14} /></span>}
            <div className="nm">{n.name}</div>
            <div className="al">{n.algo?.replace("Algorithm_", "")}</div>
          </div>
        );
      })}
    </div>
  );
}

function NodeEditor({ node, nodes, algoOptions, onName, onAlgo, onDeps, onRes, onRemove }) {
  const candidates = nodes.filter((n) => n.id !== node.id);
  function toggleDep(id) {
    onDeps(node.deps.includes(id) ? node.deps.filter((d) => d !== id) : [...node.deps, id]);
  }
  const res = node.res || {};
  return (
    <div className="grid" style={{ gap: "var(--sp-3)" }}>
      <Field label="名称"><Input value={node.name} onChange={(e) => onName(e.target.value)} /></Field>
      <Field label="算法"><Select value={node.algo} onChange={(e) => onAlgo(e.target.value)} options={algoOptions} /></Field>

      <Field label="依赖（前驱节点）">
        {candidates.length === 0 ? (
          <span className="t-faint" style={{ fontSize: "var(--fs-sm)" }}>暂无其它节点</span>
        ) : (
          <div className="row wrap" style={{ gap: 6 }}>
            {candidates.map((c) => (
              <button key={c.id} className="template-chip" style={{ padding: "4px 9px", fontSize: "var(--fs-xs)" }}
                onClick={() => toggleDep(c.id)}
                aria-pressed={node.deps.includes(c.id)}>
                {node.deps.includes(c.id) ? "✓ " : ""}{c.name}
              </button>
            ))}
          </div>
        )}
      </Field>

      <div className="divider" />
      <div className="panel-title">资源档位 <span className="t-faint" style={{ textTransform: "none", letterSpacing: 0 }}>（留空 = 自动）</span></div>
      <div className="grid" style={{ gridTemplateColumns: "1fr 1fr", gap: "var(--sp-3)" }}>
        <Field label="CPU 核心"><Input type="number" min="1" placeholder="自动" value={res.cores ?? ""} onChange={(e) => onRes({ cores: e.target.value })} /></Field>
        <Field label="内存 (MB)"><Input type="number" min="0" placeholder="自动" value={res.mem_mb ?? ""} onChange={(e) => onRes({ mem_mb: e.target.value })} /></Field>
        <Field label="带宽 (MB/s)"><Input type="number" min="0" placeholder="自动" value={res.bw_mbps ?? ""} onChange={(e) => onRes({ bw_mbps: e.target.value })} /></Field>
        <Field label="存储 (MB)"><Input type="number" min="0" placeholder="自动" value={res.storage_mb ?? ""} onChange={(e) => onRes({ storage_mb: e.target.value })} /></Field>
      </div>
      <Field label="固定设备（钉选）" hint="-1 = 交给自适应决策">
        <Input type="number" min="-1" placeholder="-1" value={res.fixedDevice ?? ""} onChange={(e) => onRes({ fixedDevice: e.target.value === "" ? null : Number(e.target.value) })} />
      </Field>

      <div className="row between">
        <Tag>id {node.id}</Tag>
        <Button size="sm" variant="danger" icon={Icon.trash} onClick={onRemove}>删除</Button>
      </div>
    </div>
  );
}
