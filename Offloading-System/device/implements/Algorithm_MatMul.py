__all__ = ['Algorithm_MatMul']

import time

# 演示/测试桩：numpy 矩阵乘(BLAS 多线程, 随可用核数加速)。用于演示「每任务 CPU 核分配」——
# 同样的活, 分到的核越多越快。返回里带上实际绑到的核数与耗时。


class Algorithm_MatMul:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        import numpy as np
        n = int(self.param.get('matrix', 1000))
        iters = int(self.param.get('iters', 12))
        cores = '?'
        try:
            import psutil
            cores = len(psutil.Process().cpu_affinity())
        except Exception:
            pass
        t0 = time.time()
        a = np.random.rand(n, n)
        for _ in range(iters):
            a = a @ a
            a /= (np.linalg.norm(a) + 1e-9)
        return f"matmul n={n} iters={iters} on {cores} cores took {time.time() - t0:.2f}s"
