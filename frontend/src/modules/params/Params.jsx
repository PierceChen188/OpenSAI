import "../modules.css";
import { useState } from "react";
import { useStore } from "../../state/store";
import { Card, Field, Input, Select, Toggle, Slider, Badge, Button, Icon, KV } from "../../components/ui";
import { getBridgeBase } from "../../api/bridge";

export default function Params() {
  const params = useStore((s) => s.params);
  const setParams = useStore((s) => s.setParams);
  const setMultipath = useStore((s) => s.setMultipath);
  const readonly = useStore((s) => s.readonlyParams);
  const setBridgeBase = useStore((s) => s.setBridgeBase);
  const toast = useStore((s) => s.toast);
  const [base, setBase] = useState(getBridgeBase());

  return (
    <div className="fade-in detail-grid">
      <div className="grid" style={{ gap: "var(--sp-4)" }}>
        <Card title="放置策略" sub="在线可调（规格 §12.1）">
          <Row k="放置模式" d="plan = 整图前瞻 · smart = 逐节点贪心 · random = 对照基线">
            <Select value={params.placementMode} onChange={(e) => setParams({ placementMode: e.target.value })}
              options={["plan", "smart", "random"]} />
          </Row>
          <Row k="W_XFER（传输权重）" d="放置打分中传输代价的权重">
            <SliderVal value={params.wXfer} min={0} max={3} step={0.1} onChange={(v) => setParams({ wXfer: v })} />
          </Row>
          <Row k="W_LOAD（负载权重）" d="放置打分中设备负载的权重（铺开 ↔ 聚集的调节旋钮）">
            <SliderVal value={params.wLoad} min={0} max={3} step={0.1} onChange={(v) => setParams({ wLoad: v })} />
          </Row>
          <Row k="健康预筛" d="打分前先剔除过载设备">
            <Toggle checked={params.healthPrefilter} onChange={(v) => setParams({ healthPrefilter: v })} />
          </Row>
        </Card>

        <Card title="自适应分配" sub="自动资源定档">
          <Row k="内存自动定档" d="按算法历史 × 系数定内存预算，并设地板值">
            <Toggle checked={params.autoMem} onChange={(v) => setParams({ autoMem: v })} />
          </Row>
          <Row k="存储自动定档" d="按历史产出 × 系数定存储配额，并设地板值">
            <Toggle checked={params.autoStorage} onChange={(v) => setParams({ autoStorage: v })} />
          </Row>
          <Row k="目标传输时延（秒）" d="自动估算带宽需求 = 前驱产出 ÷ 该目标（0 = 关闭）">
            <Input type="number" min="0" step="0.5" style={{ maxWidth: 110 }}
              value={params.targetXferS} onChange={(e) => setParams({ targetXferS: Number(e.target.value) })} />
          </Row>
        </Card>
      </div>

      <div className="grid" style={{ gap: "var(--sp-4)" }}>
        <Card title="多链路数据面" sub="一次传输并行走多条物理路径">
          <Row k="启用多链路" d="中继 / 多网卡并行传输的总开关">
            <Toggle checked={params.multipath.enabled} onChange={(v) => setMultipath({ enabled: v })} />
          </Row>
          <Row k="中继多链路" d="直连带宽不足时，经中继设备开第二条路径">
            <Toggle checked={params.multipath.relay} onChange={(v) => setMultipath({ relay: v })} />
          </Row>
          <Row k="多网卡聚合" d="多网卡源设备上跨多块网卡分流">
            <Toggle checked={params.multipath.multiNic} onChange={(v) => setMultipath({ multiNic: v })} />
          </Row>
          <Row k="最小产物（MB）" d="可触发多链路的最小产物大小">
            <Input type="number" min="0" style={{ maxWidth: 110 }}
              value={Math.round(params.multipath.minBytes / 1e6)}
              onChange={(e) => setMultipath({ minBytes: Number(e.target.value) * 1e6 })} />
          </Row>
          <Row k="增益阈值" d="保留第二条路径所需的最低加速比">
            <SliderVal value={params.multipath.gain} min={1} max={3} step={0.1} onChange={(v) => setMultipath({ gain: v })} />
          </Row>
        </Card>

        <Card title="连接" sub="桥接地址（规格 §2.5）">
          <Field label="桥接地址" hint="留空 = 同源（由 Flask 同时提供页面与 /api）">
            <div className="row">
              <Input placeholder="http://127.0.0.1:5000" value={base} onChange={(e) => setBase(e.target.value)} />
              <Button variant="primary" size="sm" icon={Icon.check}
                onClick={() => { setBridgeBase(base.trim()); toast("桥接地址已更新", "ok"); }}>应用</Button>
            </div>
          </Field>
        </Card>

        <Card title="只读 / 加锁" sub="需重启 —— 不可在线修改">
          {readonly.map((r) => (
            <div className="param-row" key={r.key}>
              <div className="info"><div className="k">{r.key}</div><div className="d">{r.value}</div></div>
              <Badge kind="warn">{r.note}</Badge>
            </div>
          ))}
        </Card>
      </div>
    </div>
  );
}

function Row({ k, d, children }) {
  return (
    <div className="param-row">
      <div className="info"><div className="k">{k}</div><div className="d">{d}</div></div>
      <div className="param-control">{children}</div>
    </div>
  );
}
function SliderVal({ value, onChange, ...p }) {
  return (
    <div className="row" style={{ minWidth: 170 }}>
      <Slider value={value} onChange={onChange} {...p} />
      <span className="mono t-hi" style={{ minWidth: 34, textAlign: "right" }}>{value.toFixed(1)}</span>
    </div>
  );
}
