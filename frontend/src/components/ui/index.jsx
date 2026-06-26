import "./ui.css";
import { Icon } from "./icons";
import { useStore } from "../../state/store";

export { Icon };

// ---------- Card / Panel ----------
export function Card({ title, sub, actions, children, className = "", pad = false, glow = false, bodyClass = "" }) {
  const cls = `card ${glow ? "glow" : ""} ${pad ? "pad" : ""} ${className}`.trim();
  if (!title && !actions) return <div className={cls}>{children}</div>;
  return (
    <div className={cls}>
      {(title || actions) && (
        <div className="card-hd">
          <div>
            {title && <h3>{title}</h3>}
            {sub && <div className="sub">{sub}</div>}
          </div>
          {actions && <div className="row">{actions}</div>}
        </div>
      )}
      <div className={`card-bd ${bodyClass}`}>{children}</div>
    </div>
  );
}

// ---------- Stat ----------
export function Stat({ label, value, unit, foot, accent }) {
  return (
    <div className="stat" data-accent={accent}>
      <div className="label">{label}</div>
      <div className="value">
        {value}
        {unit && <span className="unit">{unit}</span>}
      </div>
      {foot && <div className="foot">{foot}</div>}
    </div>
  );
}

// ---------- Badge / Tag ----------
export function Badge({ kind = "neutral", children, dot = false, live = false }) {
  return (
    <span className={`badge ${kind} ${live ? "live" : ""}`}>
      {(dot || live) && <span className="dot" />}
      {children}
    </span>
  );
}
export function Tag({ children }) {
  return <span className="tag">{children}</span>;
}

// ---------- Button ----------
export function Button({ variant, size, children, icon: IconC, ...p }) {
  const cls = `btn ${variant || ""} ${size || ""}`.trim();
  return (
    <button className={cls} {...p}>
      {IconC && <IconC size={size === "sm" ? 15 : 17} />}
      {children}
    </button>
  );
}
export function IconButton({ icon: IconC, title, ...p }) {
  return (
    <button className="icon-btn" title={title} aria-label={title} {...p}>
      <IconC size={17} />
    </button>
  );
}

// ---------- Segmented control ----------
export function Segmented({ value, onChange, options }) {
  return (
    <div className="segmented" role="tablist">
      {options.map((o) => (
        <button
          key={o.value}
          className={value === o.value ? "active" : ""}
          onClick={() => onChange(o.value)}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

// ---------- Tabs ----------
export function Tabs({ value, onChange, options }) {
  return (
    <div className="tabs">
      {options.map((o) => (
        <button key={o.value} className={value === o.value ? "active" : ""} onClick={() => onChange(o.value)}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

// ---------- Form controls ----------
export function Field({ label, children, hint }) {
  return (
    <div className="field">
      {label && <label>{label}</label>}
      {children}
      {hint && <span className="t-faint" style={{ fontSize: "var(--fs-xs)" }}>{hint}</span>}
    </div>
  );
}
export function Input(props) {
  return <input className="input" {...props} />;
}
export function Textarea(props) {
  return <textarea className="textarea" {...props} />;
}
export function Select({ options, ...p }) {
  return (
    <select className="select" {...p}>
      {options.map((o) =>
        typeof o === "string" ? (
          <option key={o} value={o}>{o}</option>
        ) : (
          <option key={o.value} value={o.value}>{o.label}</option>
        )
      )}
    </select>
  );
}
export function Toggle({ checked, onChange }) {
  return (
    <label className="toggle">
      <input type="checkbox" checked={!!checked} onChange={(e) => onChange(e.target.checked)} />
      <span className="track" />
      <span className="thumb" />
    </label>
  );
}
export function Slider({ value, onChange, min = 0, max = 1, step = 0.05 }) {
  return (
    <input
      className="slider"
      type="range"
      min={min}
      max={max}
      step={step}
      value={value}
      onChange={(e) => onChange(Number(e.target.value))}
    />
  );
}

// ---------- Meter (resource bar with committed + used segments) ----------
export function Meter({ kind, label, used = 0, committed = 0, total = 1, valueText }) {
  const pct = (n) => `${Math.max(0, Math.min(100, (n / (total || 1)) * 100))}%`;
  return (
    <div className="meter">
      <div className="meter-row">
        <span className="k">
          <span className="dot-legend" style={{ background: `var(--${kind})` }} />
          {label}
        </span>
        <span className="v">{valueText}</span>
      </div>
      <div className="meter-track">
        {committed > 0 && <div className={`meter-fill ${kind} committed`} style={{ width: pct(committed) }} />}
        <div className={`meter-fill ${kind}`} style={{ width: pct(used) }} />
      </div>
    </div>
  );
}

// ---------- Key/Value ----------
export function KV({ k, v }) {
  return (
    <div className="kv">
      <span className="k">{k}</span>
      <span className="v">{v}</span>
    </div>
  );
}

// ---------- Spinner / StateView ----------
export function Spinner() {
  return <div className="spinner" />;
}

export function StateView({ status, title, message, action, glyph }) {
  const G = glyph || (status === "error" ? Icon.warn : status === "empty" ? Icon.inbox : Icon.server);
  return (
    <div className="stateview">
      {status === "loading" ? (
        <Spinner />
      ) : (
        <div className="glyph">
          <G size={44} />
        </div>
      )}
      {title && <h4>{title}</h4>}
      {message && <p>{message}</p>}
      {action}
    </div>
  );
}

// ---------- Toasts (reads from store) ----------
export function Toaster() {
  const toasts = useStore((s) => s.toasts);
  return (
    <div className="toasts">
      {toasts.map((t) => (
        <div key={t.id} className={`toast ${t.kind}`}>
          <span className="bar" />
          <span>{t.message}</span>
        </div>
      ))}
    </div>
  );
}
