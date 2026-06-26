import { useEffect, useState, useCallback } from "react";

let autoDemoFired = false; // module-level guard so the ?demo auto-run fires at most once
let firstOnlineAt = 0;     // when the cluster first came online (for the auto-demo fallback timer)
import "./components/shell/shell.css";
import Sidebar from "./components/shell/Sidebar";
import TopBar from "./components/shell/TopBar";
import { Toaster } from "./components/ui";
import { useStore } from "./state/store";

import Overview from "./modules/overview/Overview";
import Orchestrate from "./modules/orchestrate/Orchestrate";
import Results from "./modules/results/Results";
import Algorithms from "./modules/algorithms/Algorithms";
import Params from "./modules/params/Params";
import DecisionView from "./modules/decisions/DecisionView";
import TopologyEditor from "./modules/topology/TopologyEditor";

const ROUTES = {
  overview: { c: Overview },
  topology: { c: TopologyEditor },
  orchestrate: { c: Orchestrate },
  results: { c: Results },
  placement: { c: () => <DecisionView kind="placement" /> },
  resources: { c: () => <DecisionView kind="resources" /> },
  transfers: { c: () => <DecisionView kind="transfers" /> },
  algorithms: { c: Algorithms },
  params: { c: Params },
};

// Legacy hashes from the previous two-page layout redirect to the merged page.
const ALIASES = { stage: "overview", monitor: "overview" };

function parseHash() {
  let h = (window.location.hash || "#overview").replace(/^#/, "");
  h = ALIASES[h] || h;
  return ROUTES[h] ? h : "overview";
}

export default function App() {
  const [route, setRoute] = useState(parseHash);
  const start = useStore((s) => s.start);
  const stop = useStore((s) => s.stop);
  const conn = useStore((s) => s.conn);
  const deviceCount = useStore((s) => s.devices.length);
  const runDemo = useStore((s) => s.runDemo);

  useEffect(() => {
    start();
    return stop;
  }, [start, stop]);

  // 一键演示：以 ?demo 打开时，优先等到 3 台设备（保证中继多链路可触发）再自动提交；
  // 若 3 台迟迟未齐，~18s 后用现有设备兜底提交，避免一直不演示。
  useEffect(() => {
    if (!new URLSearchParams(window.location.search).has("demo")) return;
    if (autoDemoFired) return;
    if (conn !== "online" || deviceCount < 1) return;
    if (!firstOnlineAt) firstOnlineAt = Date.now();
    const fire = () => {
      if (autoDemoFired) return;
      autoDemoFired = true;
      runDemo();
      window.location.hash = "overview";
      setRoute("overview");
    };
    if (deviceCount >= 3) { fire(); return; }
    const t = setTimeout(fire, Math.max(0, 18000 - (Date.now() - firstOnlineAt)));
    return () => clearTimeout(t);
  }, [conn, deviceCount, runDemo]);

  useEffect(() => {
    const onHash = () => setRoute(parseHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const navigate = useCallback((id) => {
    window.location.hash = id;
    setRoute(id);
  }, []);

  const entry = ROUTES[route] || ROUTES.stage;
  const Page = entry.c;

  return (
    <div className="app">
      <Sidebar route={route} onNavigate={navigate} />
      <div className="main">
        <TopBar route={route} />
        <div className={`content ${entry.flush ? "flush" : ""}`}>
          <Page navigate={navigate} />
        </div>
      </div>
      <Toaster />
    </div>
  );
}
