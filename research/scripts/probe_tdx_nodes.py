#!/usr/bin/env python3
"""TDX 行情节点探测（D53 遗留修复：mootdx bestip 替代硬编码 117.34.114.x）

问题背景：
  - 项目 quant_engine/data/sources/tdx.py 硬编码的 TDX 节点以 117.34.114.x 为主，
    从生产 ECS 实测 **No route to host**（分钟线通道自 2026-10 起长期降级）
  - mootdx 内置 38 个 HQ_HOSTS（含深圳/上海/武汉/广州双线主站），
    调研实测本机存活 14 个 —— 用"探测可达节点"替代"硬编码 IP"才是正解

两级探测：
  1) TCP connect 快速筛（socket，2s 超时）→ 候选可达集
  2) pytdx 实取数验证（连接 + 取 1 根日K）→ 真正可用集
     （TCP 通 ≠ TDX 协议可用：部分节点会接受连接但不响应协议）

用法：
  python3 research/scripts/probe_tdx_nodes.py                 # 两级探测（默认）
  python3 research/scripts/probe_tdx_nodes.py --tcp-only       # 只做 TCP 筛
  python3 research/scripts/probe_tdx_nodes.py --out /tmp/x.json # 输出 JSON
  python3 research/scripts/probe_tdx_nodes.py --emit-config    # 打印可粘贴的 TDX_SERVERS

说明：在**生产 ECS 上跑**才有意义（出口 IP/网络路径与本机不同）。
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

MOOTDX_HOSTS_JSON = os.path.join(ROOT, 'research', 'raw', 'mootdx_hq_hosts.json')


def load_candidates() -> list[tuple[str, str, int]]:
    """候选节点 = 项目现有 TDX_SERVERS + mootdx 38 节点（去重）"""
    cands: list[tuple[str, str, int]] = []
    seen: set[str] = set()

    try:
        from quant_engine.data.sources.tdx import TDX_SERVERS
        for ip, port in TDX_SERVERS:
            if ip not in seen:
                seen.add(ip)
                cands.append((f'项目现有:{ip}', ip, int(port)))
    except Exception as e:  # noqa: BLE001 导入失败不阻断（探测脚本可独立跑）
        print(f'（读取项目 TDX_SERVERS 失败：{e}）')

    if os.path.exists(MOOTDX_HOSTS_JSON):
        with open(MOOTDX_HOSTS_JSON, encoding='utf-8') as f:
            for h in json.load(f):
                if h['ip'] not in seen:
                    seen.add(h['ip'])
                    cands.append((h['name'], h['ip'], int(h['port'])))
    return cands


def tcp_probe(host: tuple[str, str, int], timeout: float = 2.0) -> bool:
    """TCP 连接探测（只验证端口可达）"""
    _, ip, port = host
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((ip, port))
        return True
    except OSError:
        return False
    finally:
        try:
            s.close()
        except OSError:
            pass


def pytdx_probe(host: tuple[str, str, int], timeout: float = 5.0) -> dict:
    """pytdx 实取数验证（连接 + 取 1 根日K）"""
    _, ip, port = host
    out = {'ok': False, 'bars': 0, 'err': ''}
    api = None
    try:
        from pytdx.hq import TdxHq_API
        api = TdxHq_API(heartbeat=True, auto_retry=False)
        with api.connect(ip, port, time_out=timeout):
            # 与项目 TdxSource._try_server 同口径：先做协议级探活
            # （get_security_quotes 能返回正价格），再试取 K 线
            q = api.get_security_quotes([(1, '600000')])
            if not q or not q[0].get('price', 0) > 0:
                out['err'] = '协议探活失败（quotes 无有效价格）'
                return out
            for market, code in ((1, '600519'), (0, '000001')):
                bars = api.get_security_bars(9, market, code, 0, 1)
                if bars:
                    out['bars'] = len(bars)
                    out['ok'] = True
                    out['sample'] = f'{market}.{code}'
                    out['last_close'] = bars[-1].get('close')
                    break
            if not out['ok']:
                out['err'] = 'quotes 通但日K 为空'
    except Exception as e:  # noqa: BLE001
        out['err'] = f'{type(e).__name__}: {e}'
    finally:
        if api is not None:
            with contextlib.suppress(Exception):
                api.disconnect()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tcp-only', action='store_true', help='只做 TCP 筛（跳过 pytdx 取数验证）')
    ap.add_argument('--timeout', type=float, default=2.0, help='TCP 探测超时（秒）')
    ap.add_argument('--workers', type=int, default=16, help='并发数')
    ap.add_argument('--out', default='')
    ap.add_argument('--emit-config', action='store_true', help='打印可粘贴的 TDX_SERVERS 代码')
    args = ap.parse_args()

    cands = load_candidates()
    print(f'候选节点 {len(cands)} 个（项目现有 + mootdx 38）\n')
    t0 = time.time()

    # ---- 一级：TCP 筛 ----
    tcp_ok = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(tcp_probe, c, args.timeout): c for c in cands}
        for fut in as_completed(futs):
            c = futs[fut]
            try:
                if fut.result():
                    tcp_ok.append(c)
            except (OSError, ValueError, RuntimeError):
                pass
    tcp_ok.sort(key=lambda c: c[1])
    print(f'① TCP 可达 {len(tcp_ok)}/{len(cands)}（{time.time() - t0:.1f}s）')
    for name, ip, port in tcp_ok:
        print(f'   ✅ {name:22s} {ip}:{port}')

    final = tcp_ok
    if not args.tcp_only and tcp_ok:
        # ---- 二级：pytdx 取数验证 ----
        print('\n② pytdx 取数验证…')
        t1 = time.time()
        verified = []
        with ThreadPoolExecutor(max_workers=min(8, len(tcp_ok))) as ex:
            futs = {ex.submit(pytdx_probe, c): c for c in tcp_ok}
            for fut in as_completed(futs):
                c = futs[fut]
                try:
                    res = fut.result()
                except Exception as e:  # noqa: BLE001
                    res = {'ok': False, 'bars': 0, 'err': str(e)}
                if res.get('ok'):
                    verified.append((c, res))
                    print(f'   ✅ {c[1]}:{c[2]} {c[0]}  取到 {res["bars"]} 根日K')
                else:
                    print(f'   ❌ {c[1]}:{c[2]} {c[0]}  {res.get("err", "")[:50]}')
        final = [c for c, _ in verified]
        print(f'\n② 最终可用 {len(final)}/{len(tcp_ok)}（{time.time() - t1:.1f}s）')

    print('\n=== 建议 TDX_SERVERS（按实测可用）===')
    for _, ip, port in final:
        print(f"    ('{ip}', {port}),")

    if args.emit_config and final:
        print('\n# 可直接替换 quant_engine/data/sources/tdx.py 的 TDX_SERVERS：')
        print('TDX_SERVERS = [')
        for _, ip, port in final:
            print(f"    ('{ip}', {port}),")
        print(']')

    if args.out:
        payload = {
            'probed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'candidates': len(cands),
            'tcp_ok': [{'name': n, 'ip': ip, 'port': p} for n, ip, p in tcp_ok],
            'usable': [{'name': n, 'ip': ip, 'port': p} for n, ip, p in final],
        }
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
        print(f'\n已写出: {args.out}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
