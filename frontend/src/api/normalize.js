// Maps raw bridge payloads into clean, UI-friendly models. Centralising this
// means modules never touch backend field names (deviceNo / usedCpu / ...).
//
// Two device shapes exist in the backend:
//   • bridge `devicesInfo.devices[]`  (camelCase, what user_client.py exposes today)
//   • server `aggregate_monitor_view`  (the richer 4-dim ledger published to Redis:
//      cores_committed/available, bw_committed_mbps, storage_*, placement_reserved,
//      observed_links, ...). Not yet bridged — we read those fields when present
//      and degrade gracefully (show "—") when absent. See spec §11 / §14.

import { ratio } from "../lib/format";

export function normalizeDevice(raw) {
  // Accept either shape; prefer ledger fields, fall back to bridge fields.
  const totalCores = num(raw.cores_total ?? raw.coreNum);
  const committedCores = num(raw.cores_committed);
  const availCores = raw.cores_available != null ? num(raw.cores_available) : Math.max(0, totalCores - committedCores);

  const totalMem = num(raw.total_memory ?? raw.totalMemory); // GB in heartbeat
  const idleMem = num(raw.idle_memory ?? raw.idleMemory);
  const usedMem = raw.used_memory != null ? num(raw.used_memory) : Math.max(0, totalMem - idleMem);

  return {
    id: String(raw.deviceNo ?? raw.device_name ?? raw.device_no ?? "device"),
    online: (raw.workStatus ?? raw.work_status ?? 0) >= 0,
    ip: raw.ipAddress ?? raw.ip_address ?? null,
    port: raw.port ?? raw.data_port ?? null,

    cpuUsage: num(raw.usedCpu ?? raw.used_cpu), // percent
    workerSlots: int(raw.worker_slots ?? raw.workerSlots ?? 1),
    runningTasks: int(raw.work_status ?? raw.workStatus ?? 0),

    cores: { total: totalCores, committed: committedCores, available: availCores },
    mem: { total: totalMem, idle: idleMem, used: usedMem, committedMb: num(raw.mem_committed_mb) },
    storage: {
      totalMb: num(raw.storage_total_mb),
      freeMb: num(raw.storage_free_mb),
      committedMb: num(raw.storage_committed_mb),
    },
    bw: {
      committedMbps: num(raw.bw_committed_mbps),
      throughput: num(raw.throughput),
      bandwidth: num(raw.bandwidth),
    },
    placementReserved: int(raw.placement_reserved),
    rtt: num(raw.delay), // ms RTT to hub
    nics: int(raw.nic_count ?? (Array.isArray(raw.nics) ? raw.nics.length : 1) ?? 1),
    links: raw.observed_links ?? {},

    // derived strength label (strong/weak) from worker slots + cores
    strength: int(raw.worker_slots ?? raw.workerSlots ?? 1) >= 4 || totalCores >= 8 ? "strong" : "standard",
    _raw: raw,
  };
}

export function normalizeMonitor(payload) {
  // payload may be the bridge devicesInfo wrapper or a raw monitor view.
  const devicesArr = payload?.devices
    ? payload.devices
    : payload?.devices == null && payload?.devs
      ? Object.values(payload.devs)
      : [];
  const devices = (devicesArr || []).map(normalizeDevice);

  const summary = {
    onlineDevices: devices.filter((d) => d.online).length,
    totalCores: sum(devices, (d) => d.cores.total),
    availableCores: sum(devices, (d) => d.cores.available),
    runningTasks: sum(devices, (d) => d.runningTasks),
    inflightTasks: int(payload?.inflight_tasks),
    committedBw: sum(devices, (d) => d.bw.committedMbps),
    committedStorage: sum(devices, (d) => d.storage.committedMb),
    placementReserved: sum(devices, (d) => d.placementReserved),
  };

  return { devices, summary, algoHistory: payload?.algo_history ?? null, ts: payload?.ts ?? null };
}

// Derive cluster-wide links from each device's observed_links map.
export function deriveLinks(devices) {
  const out = [];
  const seen = new Set();
  for (const d of devices) {
    for (const [peer, info] of Object.entries(d.links || {})) {
      const key = [d.id, peer].sort().join("→");
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        a: d.id,
        b: String(peer),
        bandwidth: num(info?.bw_mbps ?? info?.bandwidth),
        delay: num(info?.rtt_ms ?? info?.delay),
      });
    }
  }
  return out;
}

export function utilization(d) {
  return {
    cores: ratio(d.cores.committed, d.cores.total),
    mem: ratio(d.mem.used, d.mem.total),
    storage: ratio(d.storage.totalMb - d.storage.freeMb, d.storage.totalMb),
    cpu: (d.cpuUsage ?? 0) / 100,
  };
}

function num(v) {
  const n = Number(v);
  return isNaN(n) ? 0 : n;
}
function int(v) {
  return Math.round(num(v));
}
function sum(arr, fn) {
  return arr.reduce((a, x) => a + (fn(x) || 0), 0);
}
