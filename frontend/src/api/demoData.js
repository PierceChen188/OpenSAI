// Demo dataset — used ONLY when the user enables Demo mode in the top bar (off by
// default). Real cluster state is never fabricated (spec §4.2); this exists so the
// console can be explored / screencast with no backend running. A richer 8-device
// scene where only a SUBSET is chosen by placement, with one relay multipath, so a
// leadership screencast clearly shows device selection + relay aggregation.
// Shapes mirror the server aggregate_monitor_view (same normalizer path).

function dev(id, slots, cores, cpu, memT, memIdle, stoT, stoFree, bwC, links, nic = 1) {
  return {
    deviceNo: id, work_status: cpu > 25 ? 2 : cpu > 10 ? 1 : 0, used_cpu: cpu,
    ip_address: "10.0.0." + (10 + parseInt(id.replace(/\D/g, "") || "0", 10)), data_port: 7001,
    worker_slots: slots, cores_total: cores, cores_committed: Math.round((cpu / 100) * cores),
    total_memory: memT, idle_memory: memIdle, used_memory: +(memT - memIdle).toFixed(1),
    mem_committed_mb: Math.round((memT - memIdle) * 1024 * 0.4),
    storage_total_mb: stoT, storage_free_mb: stoFree, storage_committed_mb: Math.round((stoT - stoFree) * 0.1),
    bw_committed_mbps: bwC, throughput: Math.round(bwC * 1.4), bandwidth: 120, delay: 8 + (cores < 4 ? 18 : 4),
    nic_count: nic, observed_links: links,
  };
}

export const DEMO_DEVICES = [
  dev("edge-01", 6, 8, 35, 16, 9.0, 256000, 180000, 24.5, { "edge-02": { bw_mbps: 94, rtt_ms: 12 }, "edge-04": { bw_mbps: 60, rtt_ms: 18 }, "sensor-A": { bw_mbps: 40, rtt_ms: 30 } }, 2),
  dev("edge-02", 4, 8, 28, 16, 10.5, 256000, 205000, 18.0, { "edge-03": { bw_mbps: 76, rtt_ms: 15 }, "edge-04": { bw_mbps: 55, rtt_ms: 20 } }),
  dev("edge-03", 3, 4, 18, 8, 6.0, 128000, 96000, 9.0, { "edge-01": { bw_mbps: 48, rtt_ms: 22 }, "edge-05": { bw_mbps: 30, rtt_ms: 26 } }),
  dev("edge-04", 2, 4, 22, 8, 5.4, 128000, 88000, 11.0, { "edge-06": { bw_mbps: 35, rtt_ms: 24 } }), // the relay
  dev("edge-05", 1, 2, 12, 4, 2.6, 64000, 44000, 4.0, { "edge-02": { bw_mbps: 28, rtt_ms: 28 } }),
  dev("edge-06", 1, 2, 8, 4, 3.0, 64000, 50000, 0, { "edge-01": { bw_mbps: 20, rtt_ms: 34 } }),
  dev("sensor-A", 1, 2, 15, 4, 2.1, 64000, 40000, 6.0, { "edge-02": { bw_mbps: 38, rtt_ms: 31 } }),
  dev("sensor-B", 1, 2, 10, 4, 3.2, 64000, 52000, 0, { "edge-03": { bw_mbps: 33, rtt_ms: 33 } }),
];

export const DEMO_MONITOR = {
  devices: DEMO_DEVICES,
  inflight_tasks: 4,
  algo_history: {
    Algorithm_FeatureExtract: { task_mem_mb: 380, avg_runtime_s: 1.8, out_storage_mb: 22, samples: 47 },
    Algorithm_Fusion: { task_mem_mb: 210, avg_runtime_s: 0.9, out_storage_mb: 8, samples: 47 },
    Algorithm_ParallelMatMul: { task_mem_mb: 640, avg_runtime_s: 3.2, out_storage_mb: 40, samples: 12 },
    Algorithm_ProduceBlob: { task_mem_mb: 268, avg_runtime_s: 1.3, out_storage_mb: 48, samples: 9 },
  },
  ts: 0,
};

export const DEMO_ALGORITHMS = [
  "Algorithm_Pi_Calculation", "Algorithm_ProduceBlob", "Algorithm_ConsumeBlob",
  "Algorithm_ParallelMatMul", "Algorithm_FeatureExtract", "Algorithm_Fusion", "Algorithm_Relay",
];

export const DEMO_POLICIES = ["plan", "smart", "random"];

export const DEMO_TASKS = [
  {
    taskName: "demo-multilink", status: "completed", finishedTime: 14.8,
    nodes: [
      { id: 1, name: "BulkSource", taskAlgorithm: "Algorithm_ProduceBlob", deviceId: "edge-02", delay: 2.1, status: 1, outBytes: 50_331_648 },
      { id: 2, name: "SensorA", taskAlgorithm: "Algorithm_ProduceBlob", deviceId: "edge-03", delay: 0.7, status: 1, outBytes: 6_291_456 },
      { id: 3, name: "SensorB", taskAlgorithm: "Algorithm_ProduceBlob", deviceId: "edge-03", delay: 0.7, status: 1, outBytes: 6_291_456 },
      { id: 6, name: "MatMul-4c", taskAlgorithm: "Algorithm_ParallelMatMul", deviceId: "edge-01", delay: 3.0, status: 1, outBytes: 0 },
      { id: 8, name: "BulkConsume", taskAlgorithm: "Algorithm_ConsumeBlob", deviceId: "edge-01", delay: 6.4, status: 1, outBytes: 0 },
    ],
  },
];
