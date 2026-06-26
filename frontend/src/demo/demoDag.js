// 复杂演示 DAG —— 在三维窗口同时展示：智能放置(多设备铺开/就近)、资源分配(核/内存/存储 档位)、
// 以及中继多链路(一个大产物跨设备传输，大脑选中继开第二条路径)。
//
// 节点按 id 升序（中枢用 queue[id-1] 索引）；beforeNodes 只引用更小的 id。
// directAllocationId：1=钉到某设备 / 2=钉到另一设备（制造大产物的跨设备传输 + 留出中继候选）；
// 其余 -1 交给智能放置(plan)。

function node(id, name, algo, before, after, direct, params) {
  return {
    id, name,
    form: {
      beforeNodes: before, afterNodes: after,
      taskAlgorithm: algo, taskVersion: "1.0",
      taskInputInfo: { inDataPath: "./data/", outDataPath: "./data/", ...params },
      details: "", directAllocationId: direct, deviceParam: {},
    },
    result: { taskResultInfo: "", status: 1, delay: 0, deviceId: 0 },
  };
}

export function buildDemoDag() {
  const nodes = [
    // 大产物（48MB）→ 钉到设备1；其消费者(8)钉到设备2 → 跨设备大传输 → 触发中继多链路
    node(1, "BulkSource", "Algorithm_ProduceBlob", [], [8], 1, { size_mb: 48, compute_ms: 200, storage_mb: 200 }),
    // 两路"传感"产物（小）→ 各接一个消费者（由智能放置决定就近/铺开）
    node(2, "SensorA", "Algorithm_ProduceBlob", [], [4], -1, { size_mb: 6, compute_ms: 120 }),
    node(3, "SensorB", "Algorithm_ProduceBlob", [], [5], -1, { size_mb: 6, compute_ms: 120 }),
    node(4, "ConsumeA", "Algorithm_ConsumeBlob", [2], [9], -1, {}),
    node(5, "ConsumeB", "Algorithm_ConsumeBlob", [3], [9], -1, {}),
    // 两个并行算力任务（不同核档位）→ 展示资源分配 + 容量感知放置
    node(6, "MatMul-4c", "Algorithm_ParallelMatMul", [], [9], -1, { matrix: 1000, chunks: 32, cores: 4 }),
    node(7, "MatMul-2c", "Algorithm_ParallelMatMul", [], [9], -1, { matrix: 1000, chunks: 32, cores: 2 }),
    // 大产物的消费者 → 钉到设备2（与源不同机）→ 中继多链路在此发生
    node(8, "BulkConsume", "Algorithm_ConsumeBlob", [1], [9], 2, {}),
    // 汇聚 sink：依赖全部 → 触发整图完成
    node(9, "Sink", "Algorithm_ProduceBlob", [4, 5, 6, 7, 8], [], -1, { size_mb: 0, compute_ms: 0 }),
  ];
  return {
    taskName: "demo-multilink",
    taskId: "1",
    taskDesc: "复杂演示：智能放置 + 资源分配 + 中继多链路",
    taskCreateTime: Date.now() / 1000,
    taskPolicy: "p",
    nodes,
  };
}
