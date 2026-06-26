import "../modules.css";
import { useRef, useState } from "react";
import { useStore } from "../../state/store";
import { Card, Badge, Button, Icon, StateView } from "../../components/ui";

// Static catalog describing built-in algorithms (spec §12.2). Merged with the
// live registered list from the backend.
const CATALOG = {
  Algorithm_Pi_Calculation: { desc: "蒙特卡洛法估算 π（CPU 基准测试）。", produces: false, needsPred: false, params: "count_num" },
  Algorithm_FeatureExtract: { desc: "从输入数据提取特征张量。", produces: true, needsPred: false, params: "—" },
  Algorithm_Fusion: { desc: "融合前驱特征并分类。", produces: true, needsPred: true, params: "—" },
  Algorithm_MatrixMul: { desc: "稠密矩阵乘法（多核）。", produces: true, needsPred: false, params: "size" },
  Algorithm_Reduce: { desc: "归约 / 聚合前驱产出。", produces: true, needsPred: true, params: "—" },
};

export default function Algorithms() {
  const algorithms = useStore((s) => s.algorithms);
  const uploadAlgorithm = useStore((s) => s.uploadAlgorithm);
  const fileRef = useRef(null);
  const [drag, setDrag] = useState(false);
  const [busy, setBusy] = useState(false);

  const names = algorithms.length ? algorithms : Object.keys(CATALOG);

  async function handleFiles(files) {
    const file = files?.[0];
    if (!file) return;
    if (!file.name.endsWith(".py")) return useStore.getState().toast("仅允许上传 .py 插件", "err");
    setBusy(true);
    await uploadAlgorithm(file);
    setBusy(false);
  }

  return (
    <div className="fade-in grid" style={{ gap: "var(--sp-5)" }}>
      <Card title="上传插件" sub="向设备运行时添加 .py 算法">
        <div
          className={`dropzone ${drag ? "drag" : ""}`}
          onClick={() => fileRef.current?.click()}
          onDragOver={(e) => { e.preventDefault(); setDrag(true); }}
          onDragLeave={() => setDrag(false)}
          onDrop={(e) => { e.preventDefault(); setDrag(false); handleFiles(e.dataTransfer.files); }}
        >
          <div style={{ display: "grid", placeItems: "center", gap: "var(--sp-2)" }}>
            <Icon.upload size={28} />
            <div style={{ fontWeight: 600, color: "var(--text-hi)" }}>{busy ? "上传中…" : "拖入 .py 文件，或点击选择"}</div>
            <div className="t-faint" style={{ fontSize: "var(--fs-sm)", maxWidth: 460 }}>
              文件名以 <code>Algorithm</code> 开头，包含同名类，实现
              <code> __init__(self, param_dict, data_dict)</code> 与 <code>run()</code> 方法。
            </div>
          </div>
          <input ref={fileRef} type="file" accept=".py" hidden onChange={(e) => handleFiles(e.target.files)} />
        </div>
      </Card>

      <Card title="算法目录" sub={`已注册 ${names.length} 个`}>
        {names.length === 0 ? (
          <StateView status="empty" title="暂无已注册算法" />
        ) : (
          <div className="algo-grid">
            {names.map((name) => {
              const meta = CATALOG[name] || { desc: "用户上传的插件。", produces: true, needsPred: false, params: "—" };
              const isBuiltin = !!CATALOG[name];
              return (
                <div key={name} className="algo-card">
                  <div className="row between">
                    <span className="nm">{name.replace("Algorithm_", "")}</span>
                    <Badge kind={isBuiltin ? "neutral" : "info"}>{isBuiltin ? "内置" : "已上传"}</Badge>
                  </div>
                  <p className="t-dim" style={{ fontSize: "var(--fs-sm)" }}>{meta.desc}</p>
                  <div className="row wrap" style={{ gap: 6 }}>
                    {meta.produces && <Badge kind="ok">产出数据</Badge>}
                    {meta.needsPred && <Badge kind="warn">需要前驱</Badge>}
                    <Badge kind="neutral">参数：{meta.params}</Badge>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Card>
    </div>
  );
}
