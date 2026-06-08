__all__ = ['Algorithm_ParallelMatMul']

import time

# 演示/测试桩：把 chunks 个【相互独立】的小矩阵乘，分摊到「本任务实际分到的 CPU 核」上并行执行
# （每个 chunk 限单线程 + numpy 矩阵乘释放 GIL -> 线程真并行）。分到的核越多 -> 越快。
#
# 与 Algorithm_MatMul（单个大矩阵乘）的区别很关键：单个大矩阵乘受【内存带宽】限制，在本机上并不
# 随核数加速（实测 1 核 4.1s vs 8 核 5.8s，甚至更慢）；其在旧实验里“1 核 105s / 16 核 0.2s”的
# 巨大差异其实是【绑核但 BLAS 仍起满线程】的过订阅抖动假象，不是真实算力收益。
# 这里改成 embarrassingly-parallel 的 CPU 负载，随核数近线性加速（本机实测 1->8 核约 2.8x），
# 才能真实体现 B2「每任务 CPU 核分配」的收益。返回里带上实际绑到的核数与耗时。


class Algorithm_ParallelMatMul:
    def __init__(self, param_dict, data_dict):
        self.param = param_dict
        self.data = data_dict

    def run(self):
        import numpy as np
        from concurrent.futures import ThreadPoolExecutor
        try:
            from threadpoolctl import threadpool_limits
        except ImportError:
            threadpool_limits = None
        try:
            import psutil
            ncores = max(1, len(psutil.Process().cpu_affinity()))
        except Exception:
            ncores = 1
        n = int(self.param.get('matrix', 1200))
        chunks = int(self.param.get('chunks', 48))

        def unit(_):
            # 每个 chunk 单线程，避免几十个 chunk 各起满 BLAS 线程互抢；并行性来自跨 chunk 的多线程。
            if threadpool_limits is not None:
                with threadpool_limits(limits=1):
                    a = np.random.rand(n, n)
                    b = np.random.rand(n, n)
                    return float((a @ b).sum())
            a = np.random.rand(n, n)
            b = np.random.rand(n, n)
            return float((a @ b).sum())

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=ncores) as ex:
            list(ex.map(unit, range(chunks)))
        return f"parallel {chunks} matmuls(n={n}) on {ncores} cores took {time.time() - t0:.2f}s"
