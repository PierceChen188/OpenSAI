__all__ = ['Algorithm_Relay']

import hashlib
import os
import time


def _spin(compute_ms):
    """占用 CPU 约 compute_ms 毫秒（持有 GIL），用于放置实验体现并行收益。默认 0。"""
    ms = int(compute_ms or 0)
    if ms <= 0:
        return
    end = time.time() + ms / 1000.0
    while time.time() < end:
        for _ in range(20000):
            pass


# 数据面/放置实验用的桩算法：读取系统注入的 _artifact_in（前驱产物），再产出一个
# size_mb 的新块到 _artifact_out。可作为多级 DAG 的中间节点（既消费上游、又供给下游），
# 用来构造扇出/扇入/多级管道来观察放置行为。返回输入数与合并 sha，便于校验数据正确。


class Algorithm_Relay:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        _spin(self.param.get('compute_ms', 0))
        inputs = self.param.get('_artifact_in') or {}
        out = self.param.get('_artifact_out')
        h = hashlib.sha256()
        n_in = 0
        for pid in sorted(inputs, key=lambda k: int(k)):
            path = inputs[pid]
            if os.path.exists(path):
                with open(path, 'rb') as f:
                    h.update(f.read())
                n_in += 1
        in_sha = h.hexdigest()[:16]
        size = int(self.param.get('size_mb', 1)) * 1024 * 1024
        if out:
            d = os.path.dirname(os.path.abspath(out))
            if d:
                os.makedirs(d, exist_ok=True)
            with open(out, 'wb') as f:
                f.write(os.urandom(size))
        return f"relayed {n_in} inputs (in_sha={in_sha}) -> {size} bytes"
