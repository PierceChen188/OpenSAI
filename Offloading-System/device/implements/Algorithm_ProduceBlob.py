__all__ = ['Algorithm_ProduceBlob']

import hashlib
import os

# 数据面验证用的桩算法：产生一块数据写到系统注入的 _artifact_out 路径，
# 供下游(可能在另一台设备)通过 QUIC 拉取。返回内容里带 sha 便于端到端校验。


class Algorithm_ProduceBlob:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
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
