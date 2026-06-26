// Preset DAGs (spec §9 模板区). Each node: {id, name, algo, deps:[ids], params:{}}.
// These are editable after loading. ids are stable within a template.

let _uid = 1000;
export function newNodeId() {
  return ++_uid;
}

export const TEMPLATES = [
  {
    key: "minimal",
    name: "最小链路",
    desc: "单个产出 → 消费",
    nodes: [
      { id: 1, name: "compute", algo: "Algorithm_Pi_Calculation", deps: [], res: {} },
    ],
  },
  {
    key: "cross-device",
    name: "跨设备数据",
    desc: "一台设备产出，另一台消费",
    nodes: [
      { id: 1, name: "produce", algo: "Algorithm_MatrixMul", deps: [], res: {} },
      { id: 2, name: "consume", algo: "Algorithm_Reduce", deps: [1], res: {} },
    ],
  },
  {
    key: "fan-in",
    name: "扇入融合",
    desc: "多个产出 → 一个融合",
    nodes: [
      { id: 1, name: "src-a", algo: "Algorithm_FeatureExtract", deps: [], res: {} },
      { id: 2, name: "src-b", algo: "Algorithm_FeatureExtract", deps: [], res: {} },
      { id: 3, name: "src-c", algo: "Algorithm_FeatureExtract", deps: [], res: {} },
      { id: 4, name: "fuse", algo: "Algorithm_Fusion", deps: [1, 2, 3], res: {} },
    ],
  },
  {
    key: "collab",
    name: "协同感知",
    desc: "左右半图 → 两设备各提特征 → 融合分类",
    nodes: [
      { id: 1, name: "feat-left", algo: "Algorithm_FeatureExtract", deps: [], res: { fixedDevice: 0 } },
      { id: 2, name: "feat-right", algo: "Algorithm_FeatureExtract", deps: [], res: { fixedDevice: 1 } },
      { id: 3, name: "fuse-classify", algo: "Algorithm_Fusion", deps: [1, 2], res: {} },
    ],
  },
  {
    key: "reduce-tree",
    name: "归约树",
    desc: "多级归约",
    nodes: [
      { id: 1, name: "leaf-1", algo: "Algorithm_MatrixMul", deps: [], res: {} },
      { id: 2, name: "leaf-2", algo: "Algorithm_MatrixMul", deps: [], res: {} },
      { id: 3, name: "leaf-3", algo: "Algorithm_MatrixMul", deps: [], res: {} },
      { id: 4, name: "leaf-4", algo: "Algorithm_MatrixMul", deps: [], res: {} },
      { id: 5, name: "reduce-a", algo: "Algorithm_Reduce", deps: [1, 2], res: {} },
      { id: 6, name: "reduce-b", algo: "Algorithm_Reduce", deps: [3, 4], res: {} },
      { id: 7, name: "reduce-top", algo: "Algorithm_Reduce", deps: [5, 6], res: {} },
    ],
  },
  {
    key: "parallel-compute",
    name: "并行算力",
    desc: "多核矩阵乘",
    nodes: [
      { id: 1, name: "matmul", algo: "Algorithm_MatrixMul", deps: [], res: { cores: 4 } },
    ],
  },
  {
    key: "multipath",
    name: "多链路演示",
    desc: "大产物 + 多设备 → 触发中继 / 多网卡",
    nodes: [
      { id: 1, name: "big-produce", algo: "Algorithm_MatrixMul", deps: [], res: { storage_mb: 512 } },
      { id: 2, name: "pull-heavy", algo: "Algorithm_Reduce", deps: [1], res: { bw_mbps: 80 } },
    ],
  },
];

// ---- DAG model helpers ----

// Topological depth of each node (for layered layout). Returns Map id->layer.
export function computeLayers(nodes) {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const depth = new Map();
  const visiting = new Set();
  function dfs(id) {
    if (depth.has(id)) return depth.get(id);
    if (visiting.has(id)) return 0; // cycle guard
    visiting.add(id);
    const n = byId.get(id);
    const deps = (n?.deps || []).filter((d) => byId.has(d));
    const d = deps.length ? 1 + Math.max(...deps.map(dfs)) : 0;
    visiting.delete(id);
    depth.set(id, d);
    return d;
  }
  nodes.forEach((n) => dfs(n.id));
  return depth;
}

// Detect a cycle in the dependency graph.
export function hasCycle(nodes) {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const state = new Map(); // 0=unseen,1=in-stack,2=done
  function dfs(id) {
    if (state.get(id) === 1) return true;
    if (state.get(id) === 2) return false;
    state.set(id, 1);
    for (const d of byId.get(id)?.deps || []) {
      if (byId.has(d) && dfs(d)) return true;
    }
    state.set(id, 2);
    return false;
  }
  return nodes.some((n) => dfs(n.id));
}

// Build the dagData payload the bridge expects (mirrors server.py example shape).
export function buildDagData(taskName, nodes, policy) {
  const now = new Date();
  const pad = (x) => String(x).padStart(2, "0");
  const createTime = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
  const idSet = new Set(nodes.map((n) => n.id));
  return {
    taskName,
    taskId: String(Date.now()),
    taskDesc: `${nodes.length}-node DAG`,
    taskCreateTime: createTime,
    taskPolicy: policy,
    nodes: nodes.map((n) => {
      const afters = nodes.filter((m) => (m.deps || []).includes(n.id)).map((m) => m.id);
      const res = n.res || {};
      const input = {};
      ["cores", "mem_mb", "bw_mbps", "storage_mb"].forEach((k) => {
        if (res[k] != null && res[k] !== "") input[k] = Number(res[k]);
      });
      return {
        id: n.id,
        name: n.name,
        form: {
          beforeNodes: (n.deps || []).filter((d) => idSet.has(d)),
          afterNodes: afters,
          taskAlgorithm: n.algo,
          taskVersion: "1.0",
          taskInputInfo: input,
          details: n.name,
          deviceParam: res.fixedDevice != null && res.fixedDevice >= 0 ? { directAllocationId: res.fixedDevice } : {},
        },
        result: {},
      };
    }),
  };
}
