"""通信收发·数据面传输规划：多网卡聚合源（P0-1c）+ 中继多链路（P0-1b）的选择逻辑。

从 `UserServer` 抽出（P3）。这两个选择器是**纯函数**（只读 env + 入参），无 server 状态依赖，
故先行抽出；产物拉取清单的编排（`_build_input_artifacts`）依赖调度态，随 Scheduler（P6）一并迁出。
行为与原 `_multinic_sources`/`_pick_relay` 完全一致。
"""
import os


class TransferPlanner:
    @staticmethod
    def multinic_sources(src_ip, src_port, dev, size_bytes):
        """P0-1c 多网卡聚合：源设备若上报了多张可用 NIC（nic_ips）且产物够大，返回该设备的多个
        (host,port) 直连源（主 IP + 其余 NIC，去重保序）—— 消费者会对不同目的 IP 建独立连接，
        由路由表把各连接从匹配网卡发出，带宽叠加。否则返回 [主直连]。受 OPENSAI_MULTINIC 控制。
        默认关闭 → 单网卡/同构环境只回 1 个源，不影响现有直连路径与回归。"""
        primary = {'host': src_ip, 'port': src_port}
        if os.environ.get('OPENSAI_MULTINIC', '0') in ('0', '', 'false', 'False'):
            return [primary]
        min_mb = float(os.environ.get('OPENSAI_MULTINIC_MIN_MB', '8') or 8)
        if not size_bytes or size_bytes < min_mb * 1024 * 1024:
            return [primary]
        hosts, seen = [src_ip], {src_ip}
        for ip in (dev.get('nic_ips') or []):
            if ip and ip not in seen:
                seen.add(ip); hosts.append(ip)
        return [{'host': h, 'port': src_port} for h in hosts]

    @staticmethod
    def pick_relay(consumer_dno, src_dno, src_ip, src_port, size_bytes, device_info_list):
        """P0-1b 多链路中继选择：仅当 OPENSAI_MULTIPATH 开启、产物够大、且存在某中继 R 使
        min(src→R, R→consumer) 明显快于直连 consumer→src 时，返回 (relay_ip, relay_port)，否则 None。
        带宽取自各设备上报的 observed_links（key='ip:dataport'，单位 MB/s），未知用默认兜底。
        默认关闭 + 阈值保守 → 同构/局域网下不会误触发，不影响现有直连路径与回归。"""
        if os.environ.get('OPENSAI_MULTIPATH', '0') in ('0', '', 'false', 'False'):
            return None
        min_mb = float(os.environ.get('OPENSAI_MULTIPATH_MIN_MB', '8') or 8)
        if not size_bytes or size_bytes < min_mb * 1024 * 1024:
            return None
        gain = float(os.environ.get('OPENSAI_MULTIPATH_GAIN', '1.3') or 1.3)
        default_bw = float(os.environ.get('OPENSAI_DEFAULT_BW_MBPS', '10') or 10)

        def bw(dev, peer_ip, peer_port):
            rec = ((dev or {}).get('observed_links') or {}).get(f"{peer_ip}:{int(peer_port)}")
            return float(rec['mbps']) if (rec and rec.get('mbps')) else default_bw

        consumer_dev = device_info_list.get(consumer_dno) or {}
        threshold = bw(consumer_dev, src_ip, src_port) * gain   # 绕行须超过直连×gain 才值得
        best, best_bw = None, threshold
        for r_dno, r in device_info_list.items():
            if r_dno in (consumer_dno, src_dno) or not r.get('data_port'):
                continue
            r_ip, r_port = r.get('ip_address'), r.get('data_port')
            path_bw = min(bw(r, src_ip, src_port),            # src→R（用 R 实测从 src 拉的带宽近似）
                          bw(consumer_dev, r_ip, r_port))     # R→consumer（用 consumer 实测从 R 拉近似）
            if path_bw > best_bw:
                best, best_bw = (r_ip, r_port), path_bw
        return best
