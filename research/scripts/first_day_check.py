"""首交易日三日志点验收（prd/09 §6-1 M1 出口条件 / 08 §13 F-A6）

ECS 部署后首个完整交易日，验收三个日志点：
  ① 9:00-9:15  CodeGuard 静态校验（worker 日志 + Redis check_date_static）
  ② 9:15      早间新闻推送（worker 日志 "Morning news pushed" + Redis news:check_date）
  ③ 交易时段  /api/market total ≥5000 且 /api/sectors 非空（覆盖数）

用法（部署完成后，首个交易日 15:10 后执行）：
  python research/scripts/first_day_check.py --date 2026-10-28
  python research/scripts/first_day_check.py --date 2026-10-28 --skip-ssh   # 只查 API

依赖：本机 ~/.ssh/id_ed25519 可直连 ECS（deploy/update_ecs.sh 同款密钥）。
输出：PASS/FAIL 清单 + 证据行；任一 FAIL 退出码为 1。
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

ECS = 'root@8.134.89.55'
SSH_KEY = '~/.ssh/id_ed25519'
API_BASE = 'http://8.134.89.55:8000'
MARKET_MIN = 5000


def ssh(cmd: str, timeout: int = 30) -> tuple[int, str]:
    full = ['ssh', '-i', SSH_KEY, '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
            ECS, cmd]
    try:
        r = subprocess.run(full, capture_output=True, text=True, timeout=timeout, check=False)
        return r.returncode, (r.stdout + r.stderr).strip()
    except subprocess.TimeoutExpired:
        return 124, 'ssh timeout'


def curl_json(path: str, timeout: int = 20) -> dict | None:
    full = ['curl', '-s', '-m', str(timeout), f'{API_BASE}{path}']
    try:
        r = subprocess.run(full, capture_output=True, text=True, timeout=timeout + 5, check=False)
        return json.loads(r.stdout)
    except Exception:  # noqa: BLE001 外部边界：curl/解析失败按无响应处理
        return None


def check_codeguard(date: str) -> tuple[bool, str]:
    """① 9:00-9:15 CodeGuard 静态校验报告"""
    _, out = ssh(
        f"journalctl -u quant-worker --since '{date} 08:55' --until '{date} 09:20' "
        f"--no-pager 2>/dev/null | grep -i -E 'code.?guard|代码一致性' | tail -5; "
        f"redis-cli get code_guard:check_date_static 2>/dev/null")
    lines = [l for l in out.splitlines() if l.strip()]
    has_log = any('code' in l.lower() or '代码' in l for l in lines)
    has_date = date in out
    ok = has_log or has_date
    evidence = lines[-3:] if lines else ['(无日志/无 Redis 记录)']
    return ok, ' | '.join(evidence)


def check_news_push(date: str) -> tuple[bool, str]:
    """② 9:15 早间新闻推送"""
    _, out = ssh(
        f"journalctl -u quant-worker --since '{date} 09:00' --until '{date} 09:45' "
        f"--no-pager 2>/dev/null | grep -i -E 'morning news|news pushed|早间' | tail -5; "
        f"redis-cli get news:check_date 2>/dev/null")
    pushed = 'pushed' in out.lower() or ('items' in out and ' Morning' in out)
    done = date in out
    ok = pushed or done
    lines = [l for l in out.splitlines() if l.strip()]
    return ok, ' | '.join(lines[-3:]) if lines else '(无推送记录)'


def check_market_coverage() -> tuple[bool, str]:
    """③ /api/market 覆盖数 + /api/sectors 非空（盘前缓存刷新有瞬态 0，重试 3 次）"""
    import time
    market = None
    for attempt in range(3):
        market = curl_json('/api/market')
        total = (market or {}).get('total') or (market or {}).get('count') or 0
        if total >= MARKET_MIN:
            break
        if attempt < 2:
            time.sleep(5)
    sectors = curl_json('/api/sectors')
    if market is None:
        return False, '/api/market 无响应'
    total = market.get('total') or market.get('count') or 0
    sec_n = len(sectors) if isinstance(sectors, list) else (
        len(sectors.get('sectors', [])) if isinstance(sectors, dict) else 0)
    ok = total >= MARKET_MIN and sec_n > 0
    return ok, f'/api/market total={total}（≥{MARKET_MIN}）, /api/sectors={sec_n} 个板块'


def main():
    ap = argparse.ArgumentParser(description='首交易日三日志点验收')
    ap.add_argument('--date', required=True, help='验收日期 YYYY-MM-DD（首个完整交易日）')
    ap.add_argument('--skip-ssh', action='store_true', help='跳过 SSH 项（只查 API）')
    args = ap.parse_args()

    results = []
    if not args.skip_ssh:
        for name, fn in (('① CodeGuard 静态校验 9:00-9:15', lambda: check_codeguard(args.date)),
                         ('② 早间新闻推送 9:15', lambda: check_news_push(args.date))):
            ok, ev = fn()
            results.append((name, ok, ev))
    ok3, ev3 = check_market_coverage()
    results.append(('③ 行情覆盖 /api/market+/api/sectors', ok3, ev3))

    print(f'== 首交易日三日志点验收（{args.date}）==')
    all_ok = True
    for name, ok, ev in results:
        print(f"  {'✅ PASS' if ok else '❌ FAIL'}  {name}")
        print(f"         证据: {ev}")
        all_ok = all_ok and ok
    print(f"== 结论: {'全部通过 ✅' if all_ok else '存在未通过项 ❌'} ==")
    sys.exit(0 if all_ok else 1)


if __name__ == '__main__':
    main()
