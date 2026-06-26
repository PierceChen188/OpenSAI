# OpenSAI Frontend — Offloading Console

A premium "dark control-room" SPA for the OpenSAI DAG computation-offloading
platform. React + Vite + Three.js (react-three-fiber). Implements the base
functional floor (P0) of `docs/FRONTEND_SPEC.md` plus the flagship 3D topology
stage scaffold.

## Build & serve

The bundle is served by the Flask bridge (`Offloading-System/dag_offload/user_client.py`).
Vite emits straight into that app's `static/` folder.

```bash
cd frontend
npm install        # once; needs network for the install only
npm run build      # emits index.html + assets/ into dag_offload/static/
```

Then start the bridge (`python user_client.py`) and open its URL (default
`http://127.0.0.1:5000`). The SPA uses hash routing, so only `/` is served by
Flask. The build is fully self-contained — no runtime CDN/network (spec §15).

## Develop

```bash
npm run dev        # http://localhost:5173 ; proxies /api -> 127.0.0.1:5000
```

## Demo mode

No cluster running? Toggle **DEMO** in the top bar to populate every view with a
synthetic cluster (never mixed with real data; off by default — spec §4.2).

## Layout

```
src/
  api/        bridge.js (only backend contact) · normalize.js · demoData.js
  state/      store.js (zustand: connection, polling, params, tasks)
  components/ ui/ (design-system primitives) · shell/ (sidebar, topbar)
  modules/    stage/ (3D) · monitor/ · orchestrate/ · results/
              algorithms/ · params/ · decisions/ (placement/resources/transfers)
  styles/     tokens.css (single source of visual truth) · global.css
  lib/        format.js (units/time/number)
```

## Status vs spec

- **Done (P0 base):** app shell + connection lifecycle (4 states) · cluster
  monitor (4-dim ledger) · DAG orchestration + submit · tasks/results · algorithm
  library + upload · system parameters · 3D topology stage scaffold.
- **Scaffolded, awaiting backend §14.2 event streams:** live placement/resource/
  transfer *process* views and the 3D flow animation tied to real transfers.
  These document the decision chain and degrade honestly until the hub emits
  structured decision events over the bridge.
- The bridge currently exposes `devicesInfo` (camelCase device list); the richer
  `aggregate_monitor_view` ledger (committed cores/bw/storage, reservations,
  inflight, algo profiles) is read when present and shown as "—" when absent.
```
