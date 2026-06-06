__all__ = ['Algorithm_ConsumeBlob']

import hashlib
import os
import time

# 数据面验证用的桩算法：读取系统注入的 _artifact_in（前驱产物本地路径 map），
# 返回每个输入的 sha，便于和上游 ProduceBlob 的 sha 比对，确认跨设备传输正确。


def _spin(compute_ms):
    """占用 CPU 约 compute_ms 毫秒（持有 GIL），用于放置实验体现并行收益。默认 0。"""
    ms = int(compute_ms or 0)
    if ms <= 0:
        return
    end = time.time() + ms / 1000.0
    while time.time() < end:
        for _ in range(20000):
            pass


class Algorithm_ConsumeBlob:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        _spin(self.param.get('compute_ms', 0))
        inputs = self.param.get('_artifact_in') or {}
        if not inputs:
            return "ERROR: 缺少 _artifact_in (未拉到任何前驱产物)"
        total = 0
        shas = {}
        for pred_id, path in inputs.items():
            if not os.path.exists(path):
                shas[str(pred_id)] = 'MISSING'
                continue
            with open(path, 'rb') as f:
                b = f.read()
            total += len(b)
            shas[str(pred_id)] = hashlib.sha256(b).hexdigest()[:16]
        return f"consumed {len(inputs)} inputs, {total} bytes, sha={shas}"
