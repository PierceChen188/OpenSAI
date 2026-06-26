import "./stage.css";
import { useMemo, useRef, useState } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import { OrbitControls, Line, Html, Grid } from "@react-three/drei";
import * as THREE from "three";
import { useStore } from "../../state/store";
import { Card, Badge, Button, Icon, Toggle, Meter, StateView } from "../../components/ui";
import { utilization } from "../../api/normalize";
import { fmtMbps, fmtMB, fmtNum, fmtMs, fmtPct } from "../../lib/format";

const COLOR = {
  cpu: "#0ea5e9", strong: "#7c3aed", offline: "#dc2626",
  link: "#9aa9bd", flow: "#2563eb",
  body: "#dbe3ee", bodyDark: "#aab6c7", // monitor chassis (online / offline)
  direct: "#2563eb", relay: "#f97316", place: "#22c55e", // event overlay: direct flow / relay flow / placement pulse
};

// Lay devices out on a ring in the XZ plane (radius grows with device count).
function useLayout(devices) {
  return useMemo(() => {
    const pos = new Map();
    const n = devices.length;
    const radius = Math.max(3.2, n * 0.95);
    devices.forEach((d, i) => {
      if (n === 1) { pos.set(d.id, [0, 0, 0]); return; }
      const a = (i / n) * Math.PI * 2;
      pos.set(d.id, [Math.cos(a) * radius, 0, Math.sin(a) * radius]);
    });
    return pos;
  }, [devices]);
}

export default function Stage3D({ compact = false, selectedId, onSelect }) {
  const conn = useStore((s) => s.conn);
  const demoMode = useStore((s) => s.demoMode);
  const devices = useStore((s) => s.devices);
  const links = useStore((s) => s.links);
  const events = useStore((s) => s.events);
  const setDemoMode = useStore((s) => s.setDemoMode);

  // Decision overlay: keep the MOST RECENT transfers (incl. relay paths) + placed
  // devices visible persistently, so the run can be studied after it completes
  // (rotate/zoom) rather than only during a 12s flash. A new task replaces them.
  const { activeTransfers, placedIds } = useMemo(() => {
    const transfers = events.filter((e) => e.kind === "transfer" && !e.local_read && e.paths?.length);
    const placements = events.filter((e) => e.kind === "placement");
    return {
      activeTransfers: transfers.slice(-6),
      placedIds: Array.from(new Set(placements.slice(-12).map((e) => e.chosen).filter(Boolean))),
    };
  }, [events]);

  // Selection can be controlled by a parent (e.g. the Overview side panel) or
  // managed internally when used stand-alone.
  const [internalSel, setInternalSel] = useState(null);
  const sel = onSelect ? selectedId ?? null : internalSel;
  const select = (id) => (onSelect ? onSelect(sel === id ? null : id) : setInternalSel(sel === id ? null : id));

  const [layers, setLayers] = useState({ links: true, flows: true, labels: true, resources: true, transfers: true });
  const [autoRotate, setAutoRotate] = useState(true);
  const controls = useRef();

  const layout = useLayout(devices);
  const selDevice = devices.find((d) => d.id === sel) || null;
  const empty = devices.length === 0;

  return (
    <div className="stage">
      <Canvas className="stage-canvas" camera={{ position: [0, 9, 15], fov: 46 }} dpr={[1, 2]}>
        <color attach="background" args={["#eef2f8"]} />
        <fog attach="fog" args={["#eef2f8", 22, 48]} />
        <ambientLight intensity={0.95} />
        <hemisphereLight args={["#ffffff", "#cdd7e6", 0.7]} />
        <directionalLight position={[8, 15, 10]} intensity={2.4} color="#ffffff" castShadow />
        <pointLight position={[-12, 8, -8]} intensity={45} color="#c3d4ff" distance={60} />

        <Grid args={[60, 60]} cellSize={1} cellThickness={0.6} cellColor="#d3dcea" sectionSize={5}
          sectionThickness={1} sectionColor="#bcc8da" fadeDistance={46} fadeStrength={1.5}
          position={[0, -1.4, 0]} infiniteGrid />

        {layers.links && links.map((l) => {
          const a = layout.get(l.a), b = layout.get(l.b);
          if (!a || !b) return null;
          return <LinkEdge key={`${l.a}-${l.b}`} a={a} b={b} link={l} showFlow={layers.flows} />;
        })}

        {/* live decision overlay: active transfer paths (direct + relay) */}
        {layers.transfers && activeTransfers.map((tr) => (
          <TransferPaths key={tr.seq} tr={tr} layout={layout} />
        ))}
        {/* live decision overlay: recently-placed devices pulse */}
        {layers.transfers && placedIds.map((id) => {
          const p = layout.get(id);
          return p ? <PlacePulse key={`pl-${id}`} position={p} /> : null;
        })}

        {devices.map((d) => (
          <DeviceNode key={d.id} device={d} position={layout.get(d.id)} selected={sel === d.id}
            showLabel={layers.labels} showResources={layers.resources} onClick={() => select(d.id)} />
        ))}

        <OrbitControls ref={controls} autoRotate={autoRotate} autoRotateSpeed={0.5}
          enableDamping dampingFactor={0.08} minDistance={5} maxDistance={38} maxPolarAngle={Math.PI / 2.05} />
      </Canvas>

      {/* ---------- HUD ---------- */}
      {!compact && (
        <div className="hud hud-tl">
          <div className="hud-panel">
            <div className="stage-title">
              <span className="mark"><Icon.cube size={18} /></span>
              <div>
                <b>集群舞台</b>
                <div className="t-faint" style={{ fontSize: "var(--fs-xs)" }}>
                  {empty ? "暂无设备" : `${devices.length} 台设备 · ${links.length} 条链路`}
                </div>
              </div>
            </div>
          </div>
          <div className="hud-panel">
            <h4>图例</h4>
            <div className="legend-row"><span className="legend-swatch" style={{ background: COLOR.cpu, color: COLOR.cpu }} />设备 · 大小=核数</div>
            <div className="legend-row"><span className="legend-swatch" style={{ background: COLOR.strong, color: COLOR.strong }} />强算力设备</div>
            <div className="legend-row"><span className="legend-swatch" style={{ background: COLOR.flow, color: COLOR.flow }} />数据流（示意）</div>
            <div className="legend-row"><span className="legend-swatch" style={{ background: COLOR.offline, color: COLOR.offline }} />离线</div>
          </div>
        </div>
      )}

      <div className="hud hud-tr">
        <div className="hud-panel" style={{ width: 200 }}>
          <h4>图层</h4>
          {[["resources", "资源辉光"], ["links", "链路"], ["flows", "数据流"], ["transfers", "传输事件"], ["labels", "标签"]].map(([k, label]) => (
            <div className="layer-toggle" key={k}>
              <span>{label}</span>
              <Toggle checked={layers[k]} onChange={(v) => setLayers((p) => ({ ...p, [k]: v }))} />
            </div>
          ))}
          <div className="layer-toggle"><span>自动旋转</span><Toggle checked={autoRotate} onChange={setAutoRotate} /></div>
        </div>
        {!compact && selDevice && <DeviceDetail d={selDevice} onClose={() => select(sel)} />}
      </div>

      <div className="hud hud-br">
        <Button size="sm" variant="ghost" icon={Icon.refresh} onClick={() => controls.current?.reset()}>复位视角</Button>
      </div>

      <div className="hud hud-bl">
        <span className="stage-hint">拖拽旋转 · 滚轮缩放 · 点击设备查看详情</span>
      </div>

      {empty && (
        <div className="stage-empty">
          <Card pad>
            <StateView
              status={conn === "offline" ? "error" : "empty"}
              title={conn === "offline" ? "无法连接桥接服务" : "暂无在线设备"}
              message={conn === "offline"
                ? "无法连接中枢，每 2 秒重试中。可启用演示数据预览舞台。"
                : "尚未有边缘设备注册。请启动设备，或启用演示数据预览舞台。"}
              action={!demoMode && <Button variant="primary" icon={Icon.play} onClick={() => setDemoMode(true)}>启用演示数据</Button>}
            />
          </Card>
        </div>
      )}
    </div>
  );
}

// A small low-poly desktop computer (monitor + stand + base). The screen face is
// emissive and tinted by status, so CPU load reads as screen "brightness".
function ComputerModel({ scale, bodyColor, screenColor, screenIntensity, screenRef }) {
  const bodyMat = { color: bodyColor, roughness: 0.55, metalness: 0.2 };
  return (
    <group scale={scale}>
      {/* monitor chassis */}
      <mesh castShadow position={[0, 0.16, 0]}>
        <boxGeometry args={[1.12, 0.72, 0.1]} />
        <meshStandardMaterial {...bodyMat} />
      </mesh>
      {/* screen (emissive, status-tinted) */}
      <mesh ref={screenRef} position={[0, 0.16, 0.057]}>
        <planeGeometry args={[0.96, 0.56]} />
        <meshStandardMaterial color={screenColor} emissive={screenColor}
          emissiveIntensity={screenIntensity} roughness={0.3} toneMapped={false} />
      </mesh>
      {/* neck */}
      <mesh position={[0, -0.28, 0]}>
        <boxGeometry args={[0.13, 0.22, 0.08]} />
        <meshStandardMaterial {...bodyMat} />
      </mesh>
      {/* base */}
      <mesh castShadow position={[0, -0.42, 0.02]}>
        <boxGeometry args={[0.54, 0.06, 0.3]} />
        <meshStandardMaterial {...bodyMat} />
      </mesh>
    </group>
  );
}

function DeviceNode({ device, position, selected, showLabel, showResources, onClick }) {
  const ref = useRef();
  const screenRef = useRef();
  const u = utilization(device);
  const scale = 0.72 + Math.min(device.cores.total, 16) * 0.045; // cores -> size
  const screenColor = !device.online ? COLOR.offline : device.strength === "strong" ? COLOR.strong : COLOR.cpu;
  const baseIntensity = showResources ? 0.55 + u.cpu * 1.5 : 0.75;
  const ringR = 0.42 * scale + 0.16;

  useFrame((state) => {
    if (!ref.current) return;
    // gentle bob + screen "brightness" breathing keyed to CPU load
    const t = state.clock.elapsedTime;
    ref.current.position.y = Math.sin(t * 0.9 + position[0]) * 0.07;
    const m = screenRef.current?.material;
    if (m) m.emissiveIntensity = baseIntensity * (0.9 + 0.12 * Math.sin(t * 2 + position[2]));
  });

  return (
    <group position={position}>
      <group ref={ref} onClick={onClick}
        rotation={[0, Math.atan2(-position[0], -position[2]), 0]}
        onPointerOver={() => (document.body.style.cursor = "pointer")}
        onPointerOut={() => (document.body.style.cursor = "default")}>
        <ComputerModel scale={scale} screenRef={screenRef} screenColor={screenColor}
          screenIntensity={baseIntensity} bodyColor={device.online ? COLOR.body : COLOR.bodyDark} />
        {/* committed-cores ring around the base */}
        {showResources && device.cores.total > 0 && (
          <mesh position={[0, -0.46 * scale, 0.02]} rotation={[Math.PI / 2, 0, 0]}>
            <torusGeometry args={[ringR, 0.03, 8, 48, Math.PI * 2 * u.cores]} />
            <meshBasicMaterial color={COLOR.cpu} toneMapped={false} />
          </mesh>
        )}
        {/* selection halo */}
        {selected && (
          <mesh position={[0, -0.46 * scale, 0.02]} rotation={[Math.PI / 2, 0, 0]}>
            <torusGeometry args={[ringR + 0.13, 0.02, 8, 64]} />
            <meshBasicMaterial color={COLOR.flow} toneMapped={false} />
          </mesh>
        )}
      </group>

      {showLabel && (
        <Html center distanceFactor={14} position={[0, 0.6 * scale + 0.42, 0]} style={{ pointerEvents: "none" }}>
          <div style={{
            fontFamily: "var(--font-mono)", fontSize: 11, fontWeight: 600,
            color: device.online ? "#0f1b2d" : "#dc2626", whiteSpace: "nowrap",
            background: "rgba(255,255,255,0.88)", padding: "2px 7px", borderRadius: 6,
            border: "1px solid rgba(15,35,75,0.14)", backdropFilter: "blur(4px)",
            boxShadow: "0 2px 8px -3px rgba(15,30,60,0.25)",
          }}>
            {device.id} · {fmtPct(device.cpuUsage)}
          </div>
        </Html>
      )}
    </group>
  );
}

function LinkEdge({ a, b, link, showFlow }) {
  const curve = useMemo(() => {
    const mid = new THREE.Vector3((a[0] + b[0]) / 2, (a[1] + b[1]) / 2 + 0.6, (a[2] + b[2]) / 2);
    return new THREE.QuadraticBezierCurve3(new THREE.Vector3(...a), mid, new THREE.Vector3(...b));
  }, [a[0], a[1], a[2], b[0], b[1], b[2]]);
  const points = useMemo(() => curve.getPoints(32), [curve]);
  const speed = 0.15 + Math.min(link.bandwidth || 20, 120) / 200;

  return (
    <group>
      <Line points={points} color={COLOR.link} lineWidth={1} transparent opacity={0.55} />
      {showFlow && curve && <FlowParticle curve={curve} speed={speed} />}
    </group>
  );
}

function FlowParticle({ curve, speed, color = COLOR.flow, size = 0.16, offset }) {
  const ref = useRef();
  const t = useRef(offset != null ? offset : Math.random());
  useFrame((_, delta) => {
    if (!ref.current) return;
    t.current = (t.current + delta * speed) % 1;
    const p = curve.getPointAt(t.current);
    ref.current.position.set(p.x, p.y, p.z);
    const scale = 0.6 + 0.4 * Math.sin(t.current * Math.PI); // fade in/out along path
    ref.current.scale.setScalar(scale * size);
  });
  return (
    <mesh ref={ref}>
      <sphereGeometry args={[1, 12, 12]} />
      <meshBasicMaterial color={color} toneMapped={false} />
    </mesh>
  );
}

// ---- live decision overlay ----------------------------------------------

// One active transfer: draw each physical path. relay path = src->relay->consumer
// (two highlighted segments); direct/multi-NIC path = src->consumer.
function TransferPaths({ tr, layout }) {
  const src = tr.src_device, dst = tr.consumer_device;
  const segs = [];
  (tr.paths || []).forEach((p, i) => {
    if (p.type === "relay" && p.device) {
      segs.push({ from: src, to: p.device, color: COLOR.relay, key: `r1-${i}` });
      segs.push({ from: p.device, to: dst, color: COLOR.relay, key: `r2-${i}` });
    } else {
      segs.push({ from: src, to: dst, color: COLOR.direct, key: `d-${i}` });
    }
  });
  return (
    <group>
      {segs.map((s) => {
        const a = layout.get(s.from), b = layout.get(s.to);
        if (!a || !b) return null;
        return <TransferEdge key={s.key} a={a} b={b} color={s.color} />;
      })}
    </group>
  );
}

function TransferEdge({ a, b, color }) {
  const curve = useMemo(() => {
    const mid = new THREE.Vector3((a[0] + b[0]) / 2, (a[1] + b[1]) / 2 + 1.0, (a[2] + b[2]) / 2);
    return new THREE.QuadraticBezierCurve3(new THREE.Vector3(...a), mid, new THREE.Vector3(...b));
  }, [a[0], a[1], a[2], b[0], b[1], b[2]]);
  const points = useMemo(() => curve.getPoints(32), [curve]);
  return (
    <group>
      <Line points={points} color={color} lineWidth={2.5} transparent opacity={0.9} />
      {/* a few staggered particles to read as a fast stream */}
      {[0, 0.33, 0.66].map((o, i) => (
        <FlowParticle key={i} curve={curve} speed={0.5} color={color} size={0.2} offset={o} />
      ))}
    </group>
  );
}

// Pulsing green ring under a device that was just chosen by placement.
function PlacePulse({ position }) {
  const ref = useRef();
  useFrame((state) => {
    if (!ref.current) return;
    const t = state.clock.elapsedTime;
    const s = 1 + 0.25 * Math.sin(t * 3);
    ref.current.scale.setScalar(s);
    if (ref.current.material) ref.current.material.opacity = 0.5 + 0.3 * Math.sin(t * 3);
  });
  return (
    <mesh ref={ref} position={[position[0], -0.85, position[2]]} rotation={[Math.PI / 2, 0, 0]}>
      <torusGeometry args={[0.9, 0.05, 8, 56]} />
      <meshBasicMaterial color={COLOR.place} transparent opacity={0.7} toneMapped={false} />
    </mesh>
  );
}

function DeviceDetail({ d, onClose }) {
  const usedStorage = d.storage.totalMb - d.storage.freeMb;
  return (
    <div className="hud-panel device-detail">
      <div className="row between" style={{ marginBottom: "var(--sp-3)" }}>
        <div>
          <div className="id">{d.id}</div>
          <div className="t-faint mono" style={{ fontSize: "var(--fs-xs)" }}>{d.ip || "—"}</div>
        </div>
        <div className="row" style={{ gap: 6 }}>
          <Badge kind={d.online ? "ok" : "err"} dot>{d.online ? "在线" : "离线"}</Badge>
          <button className="icon-btn" style={{ width: 28, height: 28 }} onClick={onClose} aria-label="关闭"><Icon.close size={15} /></button>
        </div>
      </div>
      <div className="row wrap" style={{ gap: 6, marginBottom: "var(--sp-3)" }}>
        <Badge kind="neutral">{d.workerSlots} 槽位</Badge>
        <Badge kind="neutral">{d.runningTasks} 运行中</Badge>
        <Badge kind="neutral">{d.nics} 网卡</Badge>
        <Badge kind="neutral">RTT {fmtMs(d.rtt)}</Badge>
      </div>
      <div className="grid" style={{ gap: "var(--sp-3)" }}>
        <Meter kind="cpu" label="核心" used={d.cores.committed} total={d.cores.total}
          valueText={`${fmtNum(d.cores.committed)}/${fmtNum(d.cores.total)}`} />
        <Meter kind="mem" label="内存" used={d.mem.used} total={d.mem.total} valueText={`${d.mem.used.toFixed(1)}/${d.mem.total.toFixed(0)} GB`} />
        <Meter kind="bw" label="出口带宽" used={d.bw.committedMbps} total={Math.max(d.bw.bandwidth, d.bw.committedMbps, 1)} valueText={fmtMbps(d.bw.committedMbps)} />
        <Meter kind="store" label="存储" used={usedStorage} committed={d.storage.committedMb} total={Math.max(d.storage.totalMb, 1)} valueText={fmtMB(usedStorage)} />
      </div>
    </div>
  );
}
