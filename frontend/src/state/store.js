// Global app store (zustand). Owns connection lifecycle, polling, the cluster
// snapshot, the algorithm/policy catalog, submitted tasks, tunable params and
// the global view mode. UI components subscribe to slices of this.

import { create } from "zustand";
import { bridge, BridgeError, getBridgeBase, setBridgeBase } from "../api/bridge";
import { normalizeMonitor, deriveLinks } from "../api/normalize";
import { DEMO_MONITOR, DEMO_ALGORITHMS, DEMO_POLICIES, DEMO_TASKS } from "../api/demoData";
import { buildDemoDag } from "../demo/demoDag";

const POLL_MS = 2000; // spec §11/§15: ~2s incremental refresh

// Default tunable params (spec §12.1 safe-adjustable set). Persisted locally;
// pushing them to the backend is a future event-channel concern.
const DEFAULT_PARAMS = {
  placementMode: "plan", // plan | smart | random
  wXfer: 1.0,
  wLoad: 1.0,
  autoMem: false,
  autoStorage: false,
  targetXferS: 0,
  healthPrefilter: true,
  multipath: { enabled: false, relay: false, multiNic: true, minBytes: 8_000_000, gain: 1.2 },
};

const READONLY_PARAMS = [
  { key: "控制端口", value: "启动时配置", note: "需重启" },
  { key: "每设备 Worker 数", value: "设备端配置", note: "需重启" },
  { key: "中枢主机", value: "config/network_config.py", note: "需重启" },
];

export const useStore = create((set, get) => ({
  // ---- connection ----
  conn: "connecting", // connecting | online | empty | offline
  lastError: null,
  lastTs: null,
  bridgeBase: getBridgeBase(),
  demoMode: false,

  // ---- cluster snapshot ----
  devices: [],
  links: [],
  summary: null,
  algoHistory: null,

  // ---- catalog ----
  algorithms: [],
  policies: [],

  // ---- tasks ----
  tasks: [], // {taskName, status, finishedTime, nodes, submittedAt}
  selectedTaskName: null,

  // ---- decision events (§14.2: placement / resource / transfer) ----
  events: [], // [{seq, ts, kind, ...}] newest-last, deduped by seq
  demoStep: 0, // demo-mode scene step (advances each tick to animate selection/relay)

  // ---- topology editor (spec §4: scenario/what-if topology; live cluster stays read-only) ----
  topology: loadTopology(), // { devices:[{id,label,slots,cores,memGB,nics}], links:[{from,to,mbps,ms}] }

  // ---- view ----
  viewMode: "live", // live | replay | edit | compare
  params: { ...DEFAULT_PARAMS },
  readonlyParams: READONLY_PARAMS,

  // ---- toasts ----
  toasts: [],

  // ===================== actions =====================
  setBridgeBase(base) {
    setBridgeBase(base);
    set({ bridgeBase: base });
    get().refreshNow();
  },

  setDemoMode(on) {
    // Reset the dynamic demo scene on enable (so it works even after a real run);
    // clear on disable so live polling repopulates.
    set({ demoMode: on, demoStep: 0, events: on ? seedDemoEvents(0) : [] });
    get().refreshNow();
  },

  setViewMode(mode) {
    set({ viewMode: mode });
  },

  setParams(patch) {
    set((s) => ({ params: { ...s.params, ...patch } }));
  },
  setMultipath(patch) {
    set((s) => ({ params: { ...s.params, multipath: { ...s.params.multipath, ...patch } } }));
  },

  selectTask(name) {
    set({ selectedTaskName: name });
  },

  // ---- topology editor actions (persisted locally) ----
  _commitTopology(topo) {
    saveTopology(topo);
    set({ topology: topo });
  },
  addTopoDevice() {
    const t = get().topology;
    const n = t.devices.length + 1;
    const dev = { id: `dev-${Date.now()}`, label: `设备-${n}`, slots: 2, cores: 8, memGB: 16, nics: 1 };
    get()._commitTopology({ ...t, devices: [...t.devices, dev] });
  },
  updateTopoDevice(id, patch) {
    const t = get().topology;
    get()._commitTopology({ ...t, devices: t.devices.map((d) => (d.id === id ? { ...d, ...patch } : d)) });
  },
  removeTopoDevice(id) {
    const t = get().topology;
    get()._commitTopology({
      devices: t.devices.filter((d) => d.id !== id),
      links: t.links.filter((l) => l.from !== id && l.to !== id),
    });
  },
  addTopoLink(from, to, mbps = 10, ms = 50) {
    const t = get().topology;
    if (!from || !to || from === to) return;
    if (t.links.some((l) => (l.from === from && l.to === to) || (l.from === to && l.to === from))) return;
    get()._commitTopology({ ...t, links: [...t.links, { from, to, mbps: Number(mbps), ms: Number(ms) }] });
  },
  removeTopoLink(idx) {
    const t = get().topology;
    get()._commitTopology({ ...t, links: t.links.filter((_, i) => i !== idx) });
  },
  importLiveTopology() {
    const devs = get().devices;
    if (!devs.length) { get().toast("无在线设备可导入", "warn"); return; }
    const tDevs = devs.map((d) => ({
      id: d.id, label: d.id,
      slots: d.workerSlots ?? 1, cores: d.cores?.total ?? 1,
      memGB: Math.round(d.mem?.total ?? 0), nics: d.nics ?? 1,
    }));
    get()._commitTopology({ devices: tDevs, links: [] });
    get().toast(`已从实时集群导入 ${tDevs.length} 台设备`, "ok");
  },
  resetTopology() {
    get()._commitTopology({ devices: [], links: [] });
  },

  toast(message, kind = "info") {
    const id = ++toastSeq;
    set((s) => ({ toasts: [...s.toasts, { id, message, kind }] }));
    setTimeout(() => get().dismissToast(id), 4200);
    return id;
  },
  dismissToast(id) {
    set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) }));
  },

  // ---- polling lifecycle ----
  start() {
    if (pollTimer) return;
    get().refreshNow();
    pollTimer = setInterval(() => get().tick(), POLL_MS);
  },
  stop() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = null;
  },
  refreshNow() {
    get().tick();
    get().refreshCatalog();
  },

  async tick() {
    if (get().demoMode) {
      const mon = normalizeMonitor(DEMO_MONITOR);
      const step = (get().demoStep || 0) + 1; // advance the dynamic scene each tick
      set({
        conn: "online",
        devices: mon.devices,
        links: deriveLinks(mon.devices),
        summary: mon.summary,
        algoHistory: mon.algoHistory,
        lastTs: Date.now(),
        lastError: null,
        events: seedDemoEvents(step), // 场景每个 tick(~2s)换一次：选中设备每 2s 变
        demoStep: step,
      });
      if (get().tasks.length === 0) set({ tasks: seedDemoTasks() });
      return;
    }
    try {
      const raw = await bridge.devices();
      const mon = normalizeMonitor(raw || {});
      set({
        conn: mon.devices.length > 0 ? "online" : "empty",
        devices: mon.devices,
        links: deriveLinks(mon.devices),
        summary: mon.summary,
        algoHistory: mon.algoHistory,
        lastTs: Date.now(),
        lastError: null,
      });
    } catch (e) {
      set({ conn: "offline", lastError: e instanceof BridgeError ? e.message : String(e) });
    }
    // §14.2 决策事件流：与监控同频拉取，按 seq 去重合并（失败不影响连接状态）。
    try {
      const ev = await bridge.events();
      if (ev?.events?.length) set((s) => ({ events: mergeEvents(s.events, ev.events) }));
    } catch {
      /* events are best-effort; ignore */
    }
  },

  async refreshCatalog() {
    if (get().demoMode) {
      set({ algorithms: DEMO_ALGORITHMS, policies: DEMO_POLICIES });
      return;
    }
    try {
      const info = await bridge.algorithms();
      if (info?.algorithms) set({ algorithms: info.algorithms, policies: info.policies ?? get().policies });
    } catch {
      /* keep last known catalog; connection state handled by tick() */
    }
  },

  // ---- task submission ----
  async submitDag(dagData) {
    const name = dagData?.taskName || "task";
    if (get().demoMode) {
      get().toast(`已提交“${name}”（演示）`, "ok");
      const task = { taskName: name, status: "running", finishedTime: null, nodes: dagData.nodes?.map((n) => ({
        id: n.id, name: n.name, taskAlgorithm: n.form?.taskAlgorithm, deviceId: null, delay: null, status: 0,
      })) ?? [], submittedAt: Date.now() };
      set((s) => ({ tasks: [task, ...s.tasks], selectedTaskName: name }));
      return { ok: true };
    }
    try {
      const ack = await bridge.submitDag(dagData);
      const accepted = ack?.dagRspStatus === true || ack?.dagRspStatus === "true";
      const task = {
        taskName: name, status: accepted ? "queued" : "failed", finishedTime: null,
        nodes: dagData.nodes?.map((n) => ({ id: n.id, name: n.name, taskAlgorithm: n.form?.taskAlgorithm, deviceId: null, delay: null, status: 0 })) ?? [],
        submittedAt: Date.now(),
      };
      set((s) => ({ tasks: [task, ...s.tasks], selectedTaskName: name }));
      get().toast(accepted ? `任务“${name}”已排队` : `提交被拒绝`, accepted ? "ok" : "err");
      return { ok: accepted };
    } catch (e) {
      get().toast(`提交失败：${e.message}`, "err");
      return { ok: false, error: e.message };
    }
  },

  // 一键演示：构造复杂 DAG（放置/资源/多链路）并提交。
  async runDemo() {
    const dag = buildDemoDag();
    return get().submitDag(dag);
  },

  async uploadAlgorithm(file) {
    if (get().demoMode) {
      get().toast(`已上传 ${file.name}（演示）`, "ok");
      return { ok: true };
    }
    try {
      const r = await bridge.uploadAlgorithm(file);
      get().toast(`已上传：${r?.path ?? file.name}`, "ok");
      get().refreshCatalog();
      return { ok: true, path: r?.path };
    } catch (e) {
      get().toast(`上传失败：${e.message}`, "err");
      return { ok: false, error: e.message };
    }
  },
}));

let pollTimer = null;
let toastSeq = 0;

const TOPO_KEY = "opensai.topology";
function loadTopology() {
  try {
    const raw = localStorage.getItem(TOPO_KEY);
    if (raw) { const t = JSON.parse(raw); if (t && Array.isArray(t.devices)) return { devices: t.devices, links: t.links || [] }; }
  } catch { /* ignore */ }
  return { devices: [], links: [] };
}
function saveTopology(topo) {
  try { localStorage.setItem(TOPO_KEY, JSON.stringify(topo)); } catch { /* ignore */ }
}

function seedDemoTasks() {
  return DEMO_TASKS.map((t) => ({ ...t, submittedAt: Date.now() }));
}

// Merge incoming decision events into the kept list, deduped by seq, bounded.
function mergeEvents(prev, incoming) {
  const bySeq = new Map();
  for (const e of prev) bySeq.set(e.seq, e);
  for (const e of incoming) {
    if (!e || e.seq == null || bySeq.has(e.seq)) continue; // keep existing (preserves _seen)
    bySeq.set(e.seq, { ...e, _seen: Date.now() }); // stamp arrival time for the 3D "active" window
  }
  const all = Array.from(bySeq.values()).sort((a, b) => a.seq - b.seq);
  return all.slice(-250); // keep the most recent
}

// DYNAMIC demo decision events (no backend). Parameterised by `step` (advances
// every ~2s while Demo mode is on): the chosen device subset rotates each step
// (selected devices change often), and the relay path rotates every 2 steps
// (~2 changes per 10s). 8 devices, only 4 chosen -> only 4 green pulses.
const DEMO_ALL = ["edge-01", "edge-02", "edge-03", "edge-04", "edge-05", "edge-06", "sensor-A", "sensor-B"];
const DEMO_SUBSETS = [
  ["edge-02", "edge-01", "edge-03", "edge-04"],
  ["edge-01", "edge-03", "edge-05", "edge-02"],
  ["edge-04", "edge-02", "edge-06", "edge-01"],
  ["edge-01", "sensor-A", "edge-04", "edge-02"],
  ["edge-03", "edge-05", "edge-01", "edge-04"],
];
function seedDemoEvents(step = 0) {
  const chosen = DEMO_SUBSETS[step % DEMO_SUBSETS.length];
  // 中继传输的 源/目的 必须是"被选中(执行子任务)"的设备；只有中继中转节点用一台未被选中的设备
  // （它只转发、不执行，所以有流量但没绿框——这是合理的）。
  const unchosen = DEMO_ALL.filter((d) => !chosen.includes(d));
  // 中继不是每时每刻都需要：每隔一个场景(~4s)才出现一次中继传输，更真实；出现时换一台未选中的中转设备。
  const showRelay = step % 2 === 0;
  const relayDev = unchosen[Math.floor(step / 2) % unchosen.length] || chosen[3];
  // Stable seqs (events are replaced each tick, not merged): same key per logical
  // node/transfer -> the 3D overlay & decision rows update in place (no blink),
  // changing only when the chosen subset / relay path actually rotates.
  const base = 0;
  // deterministic score table: winner=0, others by index distance (+ sensors penalised)
  const sc = (winner) => DEMO_ALL.map((d, i) => {
    const w = DEMO_ALL.indexOf(winner);
    const cost = d === winner ? 0 : +(0.3 + (Math.abs(i - w) % 4) * 0.22 + (d.startsWith("sensor") ? 0.5 : 0)).toFixed(2);
    return { device: d, xfer_s: +(cost * 0.6).toFixed(2), load: d.startsWith("sensor") ? 0.8 : +(((i % 3) * 0.15)).toFixed(2), cost };
  }).sort((a, b) => a.cost - b.cost);
  const place = (off, node, algo, winner) => ({
    seq: base + off, ts: 0, kind: "placement", task_id: "demo-multilink", node_id: node, algorithm: algo,
    mode: "plan", chosen: winner, candidates: DEMO_ALL,
    filtered: [{ device: "sensor-B", reason: "过载(健康预筛)" }],
    locality_hit: node >= 6, scores: sc(winner),
  });
  const c = chosen;
  const placements = [
    place(1, 1, "Algorithm_ProduceBlob", c[0]),
    place(2, 2, "Algorithm_ProduceBlob", c[1]),
    place(3, 3, "Algorithm_ProduceBlob", c[2]),
    place(4, 6, "Algorithm_ParallelMatMul", c[3]),
    place(5, 7, "Algorithm_ParallelMatMul", c[0]),
    place(6, 8, "Algorithm_ConsumeBlob", c[1]),
  ];
  const resources = [
    { seq: base + 7, ts: 0, kind: "resource", task_id: "demo-multilink", node_id: 1, algorithm: "Algorithm_ProduceBlob", device: c[0],
      cores: { declared: null, final: 2 }, mem_mb: { declared: null, final: 536, auto: true }, storage_mb: { declared: 200, final: 200, auto: false }, committed_storage_mb: 200 },
    { seq: base + 8, ts: 0, kind: "resource", task_id: "demo-multilink", node_id: 6, algorithm: "Algorithm_ParallelMatMul", device: c[3],
      cores: { declared: 4, final: 4 }, mem_mb: { declared: null, final: 1280, auto: true }, storage_mb: { declared: null, final: null, auto: false }, committed_storage_mb: 0 },
    { seq: base + 9, ts: 0, kind: "resource", task_id: "demo-multilink", node_id: 8, algorithm: "Algorithm_ConsumeBlob", device: c[1],
      cores: { declared: null, final: 2 }, mem_mb: { declared: null, final: 512, auto: true }, storage_mb: { declared: null, final: 256, auto: true }, committed_storage_mb: 256 },
  ];
  const transfers = [
    { seq: base + 10, ts: 0, kind: "transfer", task_id: "demo-multilink", consumer_node: 6, pred_id: 2, src_device: c[2], consumer_device: c[0],
      size: 6291456, rate_mbps: 8, local_read: false, multipath: false, paths: [{ type: "direct", host: c[2], port: 7001, device: c[2] }] },
    { seq: base + 11, ts: 0, kind: "transfer", task_id: "demo-multilink", consumer_node: 4, pred_id: 3, src_device: c[1], consumer_device: c[3],
      size: 6291456, rate_mbps: 8, local_read: false, multipath: false, paths: [{ type: "direct", host: c[1], port: 7001, device: c[1] }] },
    // 48MB 大产物：每隔一个场景才走中继(direct+relay)，其余场景走单直连——中继不是每时每刻都需要。
    { seq: base + 12, ts: 0, kind: "transfer", task_id: "demo-multilink", consumer_node: 8, pred_id: 1, src_device: chosen[0], consumer_device: chosen[1],
      size: 50331648, rate_mbps: 8, local_read: false, multipath: showRelay,
      paths: showRelay
        ? [{ type: "direct", host: chosen[0], port: 7001, device: chosen[0] }, { type: "relay", host: relayDev, port: 7001, device: relayDev }]
        : [{ type: "direct", host: chosen[0], port: 7001, device: chosen[0] }] },
  ];
  return [...placements, ...resources, ...transfers];
}
