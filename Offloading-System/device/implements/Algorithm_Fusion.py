__all__ = ['Algorithm_Fusion']

import numpy as np
import torch

from device.implements.collab_model import CLASSES, FUSION_PATH, FusionHead

_FUSION = None


class Algorithm_Fusion:
    """协同感知：把各前驱设备传来的特征拼接后分类。

    通过 _artifact_in (pred_id -> 本地特征文件) 读取上游特征（按 pred_id 排序：1=左,2=右）。
    """

    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        global _FUSION
        inputs = self.param.get('_artifact_in') or {}
        if len(inputs) < 2:
            return f"ERROR: 需要 2 份特征输入，实得 {len(inputs)}"

        if _FUSION is None:
            fh = FusionHead()
            fh.load_state_dict(torch.load(FUSION_PATH, map_location='cpu'))
            fh.eval()
            _FUSION = fh

        feats = []
        for pid in sorted(inputs.keys(), key=lambda k: int(k)):   # 1=左, 2=右
            with open(inputs[pid], 'rb') as f:
                feats.append(np.load(f))
        x = torch.tensor(np.concatenate(feats), dtype=torch.float32).unsqueeze(0)  # (1, FEAT_DIM*2)

        with torch.no_grad():
            logits = _FUSION(x)[0]
            probs = torch.softmax(logits, dim=0)
            cls = int(torch.argmax(logits))
            conf = float(probs[cls])
        return f"predicted={CLASSES[cls]} class={cls} conf={conf:.2f}"
