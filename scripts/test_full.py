"""OpenSAI 综合功能测试执行器（单机部署）。

对应 docs/TEST_PLAN.md 的 A/B/C/D/E 组可程序化用例：在单进程内
  (1) 跑一批"无栈"单元断言（健康预筛 / 多链路源 / 中继选择 / 产物 GC）；
  (2) 起一套栈(server[plan,多链路,自动档位] + 3 设备)跑活体断言（提交 DAG→校验结果/事件/监控/日志/清理）；
  (3) 起一套 random 栈做放置对照。
每条断言映射到 TEST_PLAN 用例编号，输出 PASS/FAIL + 证据，并写 .run/test_full_results.json。

Run:  .venv/Scripts/python.exe scripts/test_full.py
退出码非 0 表示有断言失败，可直接接 CI。
"""
import json
import os
import socket
import sys
import threading
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(ROOT, 'Offloading-System'))
sys.path.insert(0, os.path.join(ROOT, 'Offloading-System', 'dag_offload'))  # 供单元断言直接 import server
import test_suite as ts        # noqa: E402  起栈/收栈/清理 helpers
import run_tests as rt         # noqa: E402  submit_dag/_node/_dag/_get/_sha

import redis                   # noqa: E402

RESULTS = []
SERVER_ERR = os.path.join(ROOT, '.run', 'server.err.log')


def rec(cid, name, passed, detail=''):
    RESULTS.append({'case': cid, 'name': name, 'pass': bool(passed), 'detail': str(detail)})
    print(f"  [{'PASS' if passed else 'FAIL'}] {cid} {name}  ({detail})", flush=True)


def submit(nodes, name='t', timeout=60):
    return rt.submit_dag(rt._dag(name, nodes), timeout)


def errs_of(res, n):
    nodes = res.get('nodes', []) if res else []
    got = sum(1 for nd in nodes if str(nd.get('ResultInfo', '')))
    bad = sum(1 for nd in nodes if 'ERROR' in str(nd.get('ResultInfo', ''))
              or 'MISSING' in str(nd.get('ResultInfo', '')))
    return bad + (n - got)


def query_events(timeout=10):
    """连 :8888 取决策事件快照(NDJSON)。返回 events 列表。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    out = []
    try:
        s.connect(('127.0.0.1', 8888))
        s.sendall((json.dumps({'user_beat': {'name': 'tf'}}) + '\n').encode())
        s.sendall((json.dumps({'eventsInfo': 'data'}) + '\n').encode())
        buf = bytearray()
        t0 = time.time()
        while time.time() - t0 < timeout:
            nl = buf.find(b'\n')
            if nl < 0:
                try:
                    chunk = s.recv(65536)
                except socket.timeout:
                    break
                if not chunk:
                    break
                buf.extend(chunk)
                continue
            line = bytes(buf[:nl]); del buf[:nl + 1]
            if not line.strip():
                continue
            try:
                obj = json.loads(line.decode('utf-8', 'replace'))
            except Exception:
                continue
            if 'eventsInfo' in obj:
                out = obj['eventsInfo'].get('events', [])
                break
    finally:
        try:
            s.close()
        except Exception:
            pass
    return out


def read_log():
    # 日志由子进程以本地编码(Windows 多为 GBK)写出，含中文标记(如"多链路源")。
    # 先按 GBK 解码才能正确匹配中文；否则用 utf-8 读会把 GBK 字节读成乱码。
    for enc in ('gbk', 'utf-8'):
        try:
            return open(SERVER_ERR, encoding=enc, errors='replace').read()
        except OSError:
            return ''
    return ''


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        return ip if ip and not ip.startswith('127.') else None
    except Exception:
        return None
    finally:
        s.close()


# ======================= (1) 无栈单元断言 =======================
def unit_checks():
    print('\n--- 单元断言（无需起栈）---', flush=True)
    # B2-06 健康预筛：一台健康 + 一台过载 → 只留健康；全过载 → 兜底全量。
    try:
        os.environ['OPENSAI_HEALTH_FILTER'] = '1'
        from server import UserServer
        # 健康阈值(device/constant/configuration)：work_status<10 且 used_cpu<90% 且 used_memory/total<0.99。
        # used_cpu 须为 "NN%" 字符串；set_device_from_dict 需完整心跳字段(否则 KeyError 被兜底保留)。
        def _dev(used_cpu, used_mem, work_status):
            return {'device_no': 'x', 'server_no': '#', 'used_cpu': used_cpu, 'core_num': 8,
                    'idle_memory': 4000, 'used_memory': used_mem, 'total_memory': 8000,
                    'work_status': work_status, 'lng': 0.0, 'lat': 0.0, 'order_value': -1,
                    'ip_address': '127.0.0.1', 'port': 0, 'throughput': 0, 'bandwidth': 0,
                    'network_usage': 0, 'delay': 0.0, 'worker_slots': 4}
        healthy = _dev('5%', 1000, 0)
        busy = _dev('99%', 7990, 50)
        out = UserServer._healthy_devices({'A': dict(healthy), 'B': dict(busy)})
        keep_one = ('A' in out and 'B' not in out)
        allbusy = UserServer._healthy_devices({'A': dict(busy), 'B': dict(busy)})
        fallback = (len(allbusy) == 2)
        rec('B2-06', '健康预筛剔除过载/全过载兜底', keep_one and fallback,
            f'健康集={list(out.keys())}, 全过载兜底={len(allbusy)}台')
    except Exception as e:
        rec('B2-06', '健康预筛', False, f'异常: {e}')

    # C8-03 多网卡：开 MULTINIC + 多 NIC + 够大 → ≥2 源；关 → 1 源。
    try:
        from server import UserServer
        dev = {'nic_ips': ['10.0.0.2', '10.0.1.2']}
        os.environ['OPENSAI_MULTINIC'] = '1'; os.environ['OPENSAI_MULTINIC_MIN_MB'] = '1'
        srcs = UserServer._multinic_sources('10.0.0.2', 9990, dev, 8 * 1024 * 1024)
        os.environ['OPENSAI_MULTINIC'] = '0'
        one = UserServer._multinic_sources('10.0.0.2', 9990, dev, 8 * 1024 * 1024)
        rec('C8-03', '多网卡聚合源(开≥2/关=1)', len(srcs) >= 2 and len(one) == 1,
            f'开={len(srcs)}源 关={len(one)}源')
    except Exception as e:
        rec('C8-03', '多网卡聚合源', False, f'异常: {e}')

    # C8-02 中继选择：构造一条经中继明显更快的链路 → _pick_relay 返回该中继。
    try:
        from server import UserServer
        os.environ['OPENSAI_MULTIPATH'] = '1'; os.environ['OPENSAI_MULTIPATH_MIN_MB'] = '1'
        os.environ['OPENSAI_MULTIPATH_GAIN'] = '1.3'
        # consumer=C, src=S 直连慢(5)，经中继 R 两段都快(50) → 应选 R。
        dil = {
            'S': {'ip_address': '1.1.1.1', 'data_port': 9990, 'observed_links': {}},
            'R': {'ip_address': '2.2.2.2', 'data_port': 9990,
                  'observed_links': {'1.1.1.1:9990': {'mbps': 50}}},   # R 实测从 S 拉=50
            'C': {'ip_address': '3.3.3.3', 'data_port': 9990,
                  'observed_links': {'1.1.1.1:9990': {'mbps': 5},      # C 直连 S=5(慢)
                                     '2.2.2.2:9990': {'mbps': 50}}},   # C 从 R 拉=50(快)
        }
        relay = UserServer._pick_relay('C', 'S', '1.1.1.1', 9990, 8 * 1024 * 1024, dil)
        rec('C8-02', '中继选择(绕行更快时选中继)', relay == ('2.2.2.2', 9990), f'选中继={relay}')
    except Exception as e:
        rec('C8-02', '中继选择', False, f'异常: {e}')

    # C5-02/03 产物 GC：造超龄 + 超水位 blob → TTL 删超龄、水位 LRU 删到上限下、探测文件保留。
    try:
        from network.dataplane.quic_server import ArtifactServer, ARTIFACTS_DIR
        os.makedirs(ARTIFACTS_DIR, exist_ok=True)
        srv = ArtifactServer()
        old = os.path.join(ARTIFACTS_DIR, 'gctest_old.blob')
        fresh = os.path.join(ARTIFACTS_DIR, 'gctest_fresh.blob')
        probe = os.path.join(ARTIFACTS_DIR, '__probe_gctest.blob')
        for p, mb in ((old, 1), (fresh, 1), (probe, 1)):
            with open(p, 'wb') as f:
                f.write(b'0' * (mb * 1024 * 1024))
        old_t = time.time() - 99999
        os.utime(old, (old_t, old_t))
        n_ttl = srv._gc_once(ttl_s=3600, max_mb=0)
        ttl_ok = (not os.path.exists(old)) and os.path.exists(fresh) and os.path.exists(probe)
        # 水位：再造 3 个各 2MB，max=3MB → 应删到 <=3MB
        wm = []
        for i in range(3):
            p = os.path.join(ARTIFACTS_DIR, f'gctest_wm{i}.blob')
            with open(p, 'wb') as f:
                f.write(b'0' * (2 * 1024 * 1024))
            t = time.time() - (100 - i)        # 不同 mtime，最旧先删
            os.utime(p, (t, t)); wm.append(p)
        srv._gc_once(ttl_s=0, max_mb=3)
        alive_mb = sum(os.path.getsize(p) for p in wm + [fresh] if os.path.exists(p)) / 1024 / 1024
        wm_ok = alive_mb <= 3.01
        for p in wm + [fresh, probe]:
            try:
                os.remove(p)
            except OSError:
                pass
        rec('C5-02', '产物GC·TTL删超龄/留探测', ttl_ok, f'删超龄={n_ttl}, 留fresh/probe={ttl_ok}')
        rec('C5-03', '产物GC·水位LRU删到上限下', wm_ok, f'存活≈{alive_mb:.1f}MB<=3MB')
    except Exception as e:
        rec('C5-02', '产物GC·TTL', False, f'异常: {e}')
        rec('C5-03', '产物GC·水位', False, f'异常: {e}')


# ======================= (2) 活体断言（plan 栈）=======================
def live_plan():
    print('\n--- 活体断言（plan + 多链路 + 自动档位，3 设备）---', flush=True)
    nic = ','.join(x for x in ['127.0.0.1', lan_ip()] if x)
    senv = {'OPENSAI_PLACEMENT': 'plan', 'OPENSAI_MULTINIC': '1', 'OPENSAI_MULTINIC_MIN_MB': '1',
            'OPENSAI_MULTIPATH': '1', 'OPENSAI_MULTIPATH_MIN_MB': '1',
            'OPENSAI_AUTO_MEM': '1', 'OPENSAI_AUTO_STORAGE': '1', 'OPENSAI_HEALTH_FILTER': '1'}
    denv = {'OPENSAI_PROBE_INTERVAL': '600', 'OPENSAI_TASK_WORKERS': '2',
            'OPENSAI_POOL_WARMUP_S': '45', 'OPENSAI_NIC_IPS': nic}
    procs = _start_stack(senv, denv, ports=(9990, 9991, 9992))
    if procs is None:
        rec('SETUP', 'plan 栈启动', False, '设备未上线')
        return
    try:
        r = redis.Redis(host='127.0.0.1', port=6379, db=0)

        # D1-01/A1-01 基础 Pi->Sleep
        res = submit([rt._node(1, 'Pi', 'Algorithm_Pi_Calculation', [], [2], -1, {'count_num': 100000}),
                      rt._node(2, 'Sleep', 'Algorithm_Sleep_1', [1], [], -1, {'sleep_time': 1})], 'tf_basic')
        pi = rt._get(res, 'Pi').get('ResultInfo') if res else None
        rec('A1-01', '单/链式 DAG 提交执行', bool(res) and isinstance(pi, (int, float)) and 2.5 < float(pi) < 3.6,
            f'pi={pi}')

        # A1-02 多形态：扇出(1->2,3 排序) + 扇入(2,3->4)。2/3 须产出产物供 4 拉取，故用 ProduceBlob。
        res = submit([
            rt._node(1, 'Src', 'Algorithm_ProduceBlob', [], [2, 3], 1, {'size_mb': 1}),
            rt._node(2, 'A', 'Algorithm_ProduceBlob', [1], [4], 2, {'size_mb': 1}),
            rt._node(3, 'B', 'Algorithm_ProduceBlob', [1], [4], 3, {'size_mb': 1}),
            rt._node(4, 'Fuse', 'Algorithm_ConsumeBlob', [2, 3], [], 1, {}),
        ], 'tf_mixed')
        rec('A1-02', '多形态DAG(扇出+扇入)', bool(res) and errs_of(res, 4) == 0, f'errs={errs_of(res, 4) if res else "NA"}')

        # A1-04/cross-device：钉选拆分 producer/consumer 到不同设备 + C6 sha
        res = submit([rt._node(1, 'P', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': 1}),
                      rt._node(2, 'C', 'Algorithm_ConsumeBlob', [1], [], 2, {})], 'tf_cross')
        p, c = (rt._get(res, 'P'), rt._get(res, 'C')) if res else ({}, {})
        psha = rt._sha(p.get('ResultInfo')); cinfo = str(c.get('ResultInfo'))
        split = p.get('result', {}).get('deviceId') != c.get('result', {}).get('deviceId')
        rec('A1-04', 'directAllocationId 钉选拆分', bool(res) and split, '已分到不同设备' if split else '未拆分')
        rec('C6-01', '数据面取产物 sha 一致', bool(psha) and psha in cinfo, f'sha={psha}')

        # B2-04 就近：不钉选依赖链 → 同机
        res = submit([rt._node(1, 'P', 'Algorithm_ProduceBlob', [], [2], -1, {'size_mb': 1}),
                      rt._node(2, 'C', 'Algorithm_ConsumeBlob', [1], [], -1, {})], 'tf_loc')
        pd = rt._get(res, 'P').get('result', {}).get('deviceId') if res else 1
        cd = rt._get(res, 'C').get('result', {}).get('deviceId') if res else 2
        rec('B2-04', '就近放置(依赖链同机)', bool(res) and pd == cd, f'P@{pd} C@{cd}')

        # B2-07 directAllocationId 字符串容错
        res = submit([rt._node(1, 'Pi', 'Algorithm_Pi_Calculation', [], [], '-1', {'count_num': 50000})], 'tf_str')
        rec('B2-07', 'directAllocationId 字符串容错', bool(res) and errs_of(res, 1) == 0, '完成无报错')

        # A1-03/B5 资源档位：声明四维 → 资源事件回显
        res = submit([rt._node(1, 'Prod', 'Algorithm_ProduceBlob', [], [2], 1,
                               {'size_mb': 2, 'cores': 2, 'mem_mb': 256, 'storage_mb': 300}),
                      rt._node(2, 'Cons', 'Algorithm_ConsumeBlob', [1], [], 2, {})], 'tf_alloc')
        evs = query_events()
        re_evs = [e for e in evs if e.get('kind') == 'resource']
        decl = next((e for e in re_evs if (e.get('storage_mb') or {}).get('declared') == 300), None)
        rec('A1-03', '声明资源档位被接收', decl is not None,
            f'storage.declared/final={decl.get("storage_mb") if decl else "NA"}')
        rec('B5-04', '存储档位记账(声明)', decl is not None and (decl.get('storage_mb') or {}).get('final'),
            f'final={decl.get("storage_mb") if decl else "NA"}')

        # B9 决策事件三类齐全
        kinds = set(e.get('kind') for e in evs)
        pe = next((e for e in evs if e.get('kind') == 'placement'), {})
        tr = next((e for e in evs if e.get('kind') == 'transfer'), {})
        rec('B9-01', '放置事件(候选/打分/选中)', bool(pe.get('chosen')) and 'candidates' in pe,
            f'chosen={pe.get("chosen")}, 候选数={len(pe.get("candidates", []))}')
        rec('B9-02', '资源事件(四维档位)', any(e.get('kind') == 'resource' and 'cores' in e for e in evs),
            f'kinds={sorted(kinds)}')
        rec('B9-03', '传输事件(路径/源目的)', bool(tr) and 'paths' in tr and 'consumer_device' in tr,
            f'paths={len(tr.get("paths", []))}, local_read={tr.get("local_read")}')

        # C9 本地直读：同设备上下游 → 该传输 local_read=true
        submit([rt._node(1, 'P', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': 1}),
                rt._node(2, 'C', 'Algorithm_ConsumeBlob', [1], [], 1, {})], 'tf_local')
        evs2 = query_events()
        lr = any(e.get('kind') == 'transfer' and e.get('task_id') == '1' and e.get('local_read') for e in evs2)
        # task_id 都是 '1'，改用最近一批：找 local_read True 的 transfer
        lr = any(e.get('kind') == 'transfer' and e.get('local_read') is True for e in evs2)
        rec('C9-01', '本地直读(同机跳过传输)', lr, f'存在 local_read=true 传输={lr}')

        # C8-03 多网卡(活体)：大产物 → 日志多链路源>=2
        submit([rt._node(1, 'P', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': 4}),
                rt._node(2, 'C', 'Algorithm_ConsumeBlob', [1], [], 2, {})], 'tf_mnic')
        time.sleep(1)
        import re as _re
        srcs = [int(m) for m in _re.findall(r'多链路源=(\d+)', read_log())]
        rec('C8-03L', '多链路源活体>=2', (max(srcs) if srcs else 0) >= 2, f'max源={max(srcs) if srcs else 0}')

        # C11-01 内置算法：MatMul / ParallelMatMul 可执行
        res = submit([rt._node(1, 'MM', 'Algorithm_MatMul', [], [2], 1, {'n': 128}),
                      rt._node(2, 'PMM', 'Algorithm_ParallelMatMul', [1], [], 2, {'n': 128, 'cores': 2})], 'tf_mm')
        rec('C11-01', '内置算力算法可执行', bool(res) and errs_of(res, 2) == 0, f'errs={errs_of(res, 2) if res else "NA"}')

        # E2-01 算法执行失败被收敛：Fusion 只给 1 个输入 → ERROR，但整图汇总不崩
        res = submit([rt._node(1, 'P', 'Algorithm_ProduceBlob', [], [2], 1, {'size_mb': 1}),
                      rt._node(2, 'Fu', 'Algorithm_Fusion', [1], [], 2, {})], 'tf_fail')
        fu = str(rt._get(res, 'Fu').get('ResultInfo')) if res else ''
        rec('E2-01', '算法失败被收敛(整图不崩)', bool(res) and 'ERROR' in fu, f'fusion={fu[:40]}')

        # E2-03/A1-05 未知算法被收敛
        res = submit([rt._node(1, 'X', 'Algorithm_DoesNotExist', [], [], 1, {})], 'tf_unknown')
        xi = str(rt._get(res, 'X').get('ResultInfo')) if res else ''
        rec('A1-05', '非法/未知算法被收敛', bool(res) and ('ERROR' in xi or 'MISSING' in xi or xi == ''),
            f'result={xi[:40]!r}')

        # B7/C1 监控视图发布 + 设备四维字段
        time.sleep(2)
        view = r.get('OPENSAI_MONITOR_VIEW')
        v = json.loads(view) if view else {}
        devs = v.get('devices', {})
        d0 = next(iter(devs.values()), {})
        c1_fields = all(k in d0 for k in ('cores_total', 'total_memory', 'storage_total_mb',
                                          'worker_slots', 'placement_reserved'))
        rec('B7-01', '聚合监控视图发布', bool(devs) and 'algo_history' in v and 'inflight_tasks' in v,
            f'设备数={len(devs)}, 画像={len(v.get("algo_history", {}))}')
        rec('C1-01', '设备心跳四维字段齐全', c1_fields, f'字段ok={c1_fields}')

        # B6/B3 账本：所有任务跑完后预留应归零。监控视图周期发布，预留释放与发布间有窗口，
        # 故轮询至多 ~8s 等其settle（仍 >0 则为真实泄漏）。
        reserved = 9
        for _ in range(8):
            view = r.get('OPENSAI_MONITOR_VIEW')
            vv = json.loads(view).get('devices', {}) if view else {}
            reserved = sum(int(dd.get('placement_reserved', 0)) for dd in vv.values())
            infl = json.loads(view).get('inflight_tasks', 0) if view else 0
            if reserved == 0:
                break
            time.sleep(1)
        rec('B3-02', '放置预留完成后归还', reserved == 0, f'残留预留={reserved}, 在飞={infl}')

        # B4 画像：算法历史已积累
        rec('B4-01', '算法画像EWMA积累', len(v.get('algo_history', {})) >= 1,
            f'画像算法数={len(v.get("algo_history", {}))}')

        # B8 产物生命周期清理（日志标记）
        log = read_log()
        rec('B8-01', '整图完成产物清理', '[cleanup]' in log or '[dataplane] 清理' in log,
            "日志含 [cleanup]")
        # E1-01 结构化日志标记
        marks = [m for m in ('[alloc]', '[multipath]', '[health]') if m in log]
        rec('E1-01', '结构化日志可定位决策', len(marks) >= 2, f'标记={marks}')

        # D1-05 协同感知(冒烟)：CIFAR 左右半 → 跨设备 → 融合分类
        import re as _re2
        res = submit([
            rt._node(1, 'FE_l', 'Algorithm_FeatureExtract', [], [3], 1, {'image_index': 0, 'half': 'left'}),
            rt._node(2, 'FE_r', 'Algorithm_FeatureExtract', [], [3], 2, {'image_index': 0, 'half': 'right'}),
            rt._node(3, 'Fusion', 'Algorithm_Fusion', [1, 2], [], 1, {}),
        ], 'tf_collab', timeout=120)
        finfo = str(rt._get(res, 'Fusion').get('ResultInfo')) if res else ''
        m = _re2.search(r'class=(\d+)', finfo)
        ld = rt._get(res, 'FE_l').get('result', {}).get('deviceId') if res else None
        rd = rt._get(res, 'FE_r').get('result', {}).get('deviceId') if res else None
        rec('D1-05', '协同感知端到端(跨设备融合)', bool(m) and ld != rd,
            f'class={m.group(1) if m else "NA"}, 左@{ld}右@{rd}')

        # D2-01 复杂混合大图：两路产出(跨设备)→融合消费(扇入传输) + 并行算力分支 → 汇聚。
        # 注：ConsumeBlob 须从「会落盘产物」的 ProduceBlob 拉取(MatMul 不产出产物)。
        res = submit([
            rt._node(1, 'P1', 'Algorithm_ProduceBlob', [], [3], 1, {'size_mb': 2}),
            rt._node(2, 'P2', 'Algorithm_ProduceBlob', [], [3], 2, {'size_mb': 2}),
            rt._node(3, 'F', 'Algorithm_ConsumeBlob', [1, 2], [5], 3, {}),
            rt._node(4, 'M', 'Algorithm_ParallelMatMul', [], [5], 1, {'n': 128, 'cores': 2}),
            rt._node(5, 'Sink', 'Algorithm_Sleep_1', [3, 4], [], -1, {'sleep_time': 1}),
        ], 'tf_complex', timeout=120)
        rec('D2-01', '复杂混合大图全完成', bool(res) and errs_of(res, 5) == 0,
            f'errs={errs_of(res, 5) if res else "NA"}')
    finally:
        _stop_stack(procs)


# ======================= (3) random 放置对照 =======================
def live_random():
    print('\n--- 放置对照（random，2 设备）---', flush=True)
    procs = _start_stack({'OPENSAI_PLACEMENT': 'random'},
                         {'OPENSAI_TASK_WORKERS': '2', 'OPENSAI_POOL_WARMUP_S': '45'},
                         ports=(9990, 9991), tag='rndserver')
    if procs is None:
        rec('B2-03', 'random 栈启动', False, '设备未上线')
        return
    try:
        res = submit([rt._node(1, 'Pi', 'Algorithm_Pi_Calculation', [], [2], -1, {'count_num': 100000}),
                      rt._node(2, 'Sleep', 'Algorithm_Sleep_1', [1], [], -1, {'sleep_time': 1})], 'rnd')
        rec('B2-03', 'random 放置可完成(对照基线)', bool(res) and errs_of(res, 2) == 0,
            f'errs={errs_of(res, 2) if res else "NA"}')
    finally:
        _stop_stack(procs)


# ======================= 起栈/收栈 =======================
def _start_stack(senv, denv, ports, tag='server'):
    ts.kill_stale()
    t0 = time.time()
    while ts.port_in_use() and time.time() - t0 < 12:
        ts.kill_stale(); time.sleep(1)
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    r.flushall()
    procs = [ts.start('server.py', senv, tag)]
    time.sleep(3)
    for i, p in enumerate(ports):
        e = dict(denv); e['OPENSAI_DATA_PORT'] = str(p)
        procs.append(ts.start('device_client.py', e, f'{tag}_dev{p}'))
    t0 = time.time()
    while int(r.scard('DEVICE_ONLINE_SET')) < len(ports) and time.time() - t0 < 90:
        time.sleep(2)
    if int(r.scard('DEVICE_ONLINE_SET')) < len(ports):
        _stop_stack(procs)
        return None
    time.sleep(5)
    return procs


def _stop_stack(procs):
    for p in procs or []:
        ts.kill_tree(p)
    ts.kill_stale()


def main():
    try:
        redis.Redis(host='127.0.0.1', port=6379, db=0).ping()
    except Exception as e:
        print(f'[setup] FAIL: redis 不可用 ({e})')
        return 1
    print('==== OpenSAI 综合功能测试 ====', flush=True)
    unit_checks()
    live_plan()
    live_random()

    passed = sum(1 for x in RESULTS if x['pass'])
    total = len(RESULTS)
    out = os.path.join(ROOT, '.run', 'test_full_results.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump({'passed': passed, 'total': total, 'results': RESULTS},
              open(out, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    print(f'\n=== 综合功能：{passed}/{total} 断言通过 ===', flush=True)
    print(f'(结果写入 {out})', flush=True)
    fails = [x for x in RESULTS if not x['pass']]
    if fails:
        print('未通过：' + ', '.join(f"{x['case']}({x['detail']})" for x in fails), flush=True)
    return 0 if passed == total else 1


if __name__ == '__main__':
    sys.exit(main())
