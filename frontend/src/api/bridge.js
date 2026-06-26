// ============================================================================
// Bridge interface layer — the ONLY module that talks to the backend.
// Everything the spec calls the "桥接层" (§14.3) is funnelled through here so
// the rest of the app never sees transport details and future protocol changes
// stay contained (spec §15 "与桥接/事件流的对接收敛到清晰的接口层").
//
// Backend reality (Offloading-System/dag_offload/user_client.py):
//   GET  /api/devices     -> text/event-stream, one event: {devicesInfo:{algorithms,policies,devices}}
//   GET  /api/algorithms  -> one event: {algorithmsInfo:{...}}
//   GET  /api/policies     -> one event: {policiesInfo:{...}}
//   GET  /api/status      -> one event: {dagRspStatus:true} (submit ack relay)
//   POST /api/dag/upload  -> one event: {dagRspStatus:true}
//   POST /api/upload_py   -> JSON {message,path}
// Each event-stream yields a single `data: {...}` frame then closes (the bridge's
// generate() is a one-shot). We therefore POLL these endpoints on an interval
// rather than holding an EventSource open.
// ============================================================================

const LS_KEY = "opensai.bridgeBase";

export function getBridgeBase() {
  // Empty string = same origin (production: Flask serves the SPA + /api).
  // Configurable per spec §2.5 "后端连接地址可配".
  return localStorage.getItem(LS_KEY) ?? "";
}

export function setBridgeBase(base) {
  localStorage.setItem(LS_KEY, base || "");
}

function url(path) {
  return `${getBridgeBase()}${path}`;
}

// Parse a (possibly multi-frame) SSE body and return the last non-empty JSON
// payload. The bridge sends `data: {}` on timeout, which we treat as "no data".
function parseSSE(text) {
  let last = null;
  for (const line of text.split(/\r?\n/)) {
    const t = line.trim();
    if (!t.startsWith("data:")) continue;
    const raw = t.slice(5).trim();
    if (!raw) continue;
    try {
      const obj = JSON.parse(raw);
      if (obj && Object.keys(obj).length > 0) last = obj;
    } catch {
      /* ignore unparseable frame */
    }
  }
  return last;
}

class BridgeError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "BridgeError";
    this.status = status;
  }
}

// Core fetch with timeout. Returns parsed payload (SSE or JSON) or throws.
async function call(path, { method = "GET", body, timeoutMs = 12000, json = false } = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url(path), {
      method,
      signal: ctrl.signal,
      headers: body && !(body instanceof FormData) ? { "Content-Type": "application/json" } : undefined,
      body: body instanceof FormData ? body : body ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) throw new BridgeError(`HTTP ${res.status} on ${path}`, res.status);
    const ctype = res.headers.get("content-type") || "";
    if (json || ctype.includes("application/json")) {
      return await res.json();
    }
    const text = await res.text();
    return parseSSE(text);
  } catch (e) {
    if (e.name === "AbortError") throw new BridgeError(`Timeout on ${path}`, 0);
    if (e instanceof BridgeError) throw e;
    throw new BridgeError(e.message || `Network error on ${path}`, 0);
  } finally {
    clearTimeout(timer);
  }
}

// The three "base info" endpoints all return {<key>: {algorithms, policies, devices}}.
function unwrap(payload, key) {
  if (!payload) return null;
  return payload[key] ?? null;
}

export const bridge = {
  async devices() {
    return unwrap(await call("/api/devices"), "devicesInfo");
  },
  async algorithms() {
    return unwrap(await call("/api/algorithms"), "algorithmsInfo");
  },
  async policies() {
    return unwrap(await call("/api/policies"), "policiesInfo");
  },
  // Submit a DAG. Returns the ack payload ({dagRspStatus:true}) or null.
  async submitDag(dagData) {
    return call("/api/dag/upload", { method: "POST", body: { dagData }, timeoutMs: 30000 });
  },
  // Poll the status stream for a submit ack.
  async status() {
    return call("/api/status");
  },
  // §14.2 decision event stream (placement / resource / transfer). Returns {events:[...]} or null.
  async events() {
    return unwrap(await call("/api/events"), "eventsInfo");
  },
  // Upload an algorithm plugin (.py). Returns {message, path}.
  async uploadAlgorithm(file) {
    const fd = new FormData();
    fd.append("file", file);
    return call("/api/upload_py", { method: "POST", body: fd, json: true, timeoutMs: 30000 });
  },
};

export { BridgeError };
