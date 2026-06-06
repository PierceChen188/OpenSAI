__all__ = ['Algorithm_ProduceBlob']

import hashlib
import os
import time

# 数据面验证用的桩算法：产生一块数据写到系统注入的 _artifact_out 路径，
# 供下游(可能在另一台设备)通过 QUIC 拉取。返回内容里带 sha 便于端到端校验。


def _spin(compute_ms):
    """占用 CPU 约 compute_ms 毫秒（持有 GIL）。用于放置实验：同机的 CPU 任务会因 GIL
    串行、跨机才能真正并行，从而体现「分散放置」的并行收益。默认 0 = 不占用。"""
    ms = int(compute_ms or 0)
    if ms <= 0:
        return
    end = time.time() + ms / 1000.0
    while time.time() < end:
        for _ in range(20000):
            pass


class Algorithm_ProduceBlob:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        _spin(self.param.get('compute_ms', 0))
        size = int(self.param.get('size_mb', 3)) * 1024 * 1024
        out = self.param.get('_artifact_out')
        if not out:
            return "ERROR: 缺少 _artifact_out (系统未注入产物路径)"
        d = os.path.dirname(os.path.abspath(out))
        if d:
            os.makedirs(d, exist_ok=True)
        blob = os.urandom(size)
        with open(out, 'wb') as f:
            f.write(blob)
        sha = hashlib.sha256(blob).hexdigest()[:16]
        return f"produced {size} bytes sha={sha}"
