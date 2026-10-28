"""月度复验（prd/07 §15 节奏门禁 / 14 §4：每月 1 日滚动复验）

门禁语义（14 §4）：**连续两月均盈亏 >0 才恢复标准仓位**；否则维持当前
仓位折减（Kelly v1.6 口径）。本脚本把该复验从"人工跑分析"固化为可重复
执行的标准动作：

  1. 读取生产 signals 归档（ECS SQLite history.db，2026-10-25 起全量落库）
  2. 按 signal_date 归月，逐信号用腾讯日K计算 T+5 / T+10 前向收益
  3. 汇总月度胜率/均盈亏，输出门禁判定（连续两月 >0 → 可恢复标准仓位）
  4. 产出 markdown 报告（research/docs/ 归档，供 rules JSON 调整决策）

用法：
  # ① 从 ECS 拉取 signals 归档（本地无 history.db 时）
  ssh -i ~/.ssh/id_ed25519 root@8.134.89.55 \
    "sqlite3 -header -csv /stock_monitor/data/history.db \
     'SELECT signal_date,code,name,action,level,score,strategy,price \
      FROM signals ORDER BY signal_date'" \
    > research/data/signals_dump.csv

  # ② 本地复验（默认 3 个月窗口；T+N 用腾讯日K，本地可直连）
  python research/scripts/monthly_recheck.py --csv research/data/signals_dump.csv

  # 或直接指向本地/挂载的 history.db
  python research/scripts/monthly_recheck.py --db /path/to/history.db

口径说明（与 docs/22-23 复验保持一致）：
- 收益 = (T+N 收盘价 - 信号价) / 信号价；信号价为信号生成日收盘价
  （df['close'].iloc[-1]，与 D47"信号价"语义一致）
- 未成熟信号（T+N 交易日尚未走完）计入 n 但不计入对应horizon统计
- BUY/WATCH 分列；门禁判定以 BUY 为主口径（WATCH 为观察池无执行含义）
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

HORIZONS = (5, 10)  # T+N 交易日


def load_signals_csv(path: str) -> list[dict]:
    with open(path, newline='', encoding='utf-8-sig') as f:
        rows = [r for r in csv.DictReader(f) if r.get('code')]
    for r in rows:
        r['price'] = float(r['price']) if r.get('price') else 0.0
    return rows


def load_signals_db(path: str) -> list[dict]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT signal_date,code,name,action,level,score,strategy,price "
            "FROM signals ORDER BY signal_date")]
    finally:
        conn.close()
    for r in rows:
        r['price'] = float(r['price'] or 0)
    return rows


def fetch_daily(code: str, days: int = 420) -> pd.DataFrame | None:
    """腾讯日K（本地可直连；失败返 None 由调用方按缺失处理）"""
    from quant_engine.data.sources.tencent import TencentSource
    try:
        df = TencentSource.get_daily_data(code, days=days)
    except Exception as e:  # noqa: BLE001 单票失败不中断整轮复验
        print(f'  [warn] {code} 日K获取失败: {e}')
        return None
    if df is None or df.empty:
        return None
    if 'date' not in df.columns:
        return None
    return df.sort_values('date').reset_index(drop=True)


def forward_returns(df: pd.DataFrame, signal_date: str, entry_price: float) -> dict:
    """计算 T+5/T+10 前向收益（交易日口径，按数据自身日期推进）"""
    dates = df['date'].astype(str).tolist()
    if signal_date not in dates:
        # 信号日非交易日/数据缺口：找信号日后第一根
        after = [i for i, d in enumerate(dates) if d > signal_date]
        if not after:
            return {}
        idx = after[0] - 1  # 以信号日之前最后一根收盘为锚（近似）
    else:
        idx = dates.index(signal_date)
    out = {}
    closes = df['close']
    for n in HORIZONS:
        j = idx + n
        if j < len(closes) and entry_price > 0:
            out[f't{n}_pnl'] = (float(closes.iloc[j]) - entry_price) / entry_price
            out[f't{n}_ready'] = True
        else:
            out[f't{n}_pnl'] = None
            out[f't{n}_ready'] = False
    return out


def run_recheck(signals: list[dict], months: int) -> dict:
    # 按月分组的信号
    by_month: dict[str, list[dict]] = defaultdict(list)
    for s in signals:
        d = str(s.get('signal_date') or '')
        if len(d) >= 7 and s.get('price', 0) > 0:
            by_month[d[:7]].append(s)

    recent_months = sorted(by_month)[-months:]
    daily_cache: dict[str, pd.DataFrame | None] = {}
    monthly = []

    for m in recent_months:
        rows = by_month[m]
        stats = {'month': m, 'n_total': len(rows), 'n_buy': 0, 'n_watch': 0,
                 'fetch_fail': 0}
        for h in HORIZONS:
            stats[f't{h}_n'] = 0
            stats[f't{h}_win'] = 0
            stats[f't{h}_pnl_sum'] = 0.0

        for s in rows:
            action = (s.get('action') or '').upper()
            if action == 'BUY':
                stats['n_buy'] += 1
            else:
                stats['n_watch'] += 1
            code = s['code']
            if code not in daily_cache:
                daily_cache[code] = fetch_daily(code)
            df = daily_cache[code]
            if df is None:
                stats['fetch_fail'] += 1
                continue
            fw = forward_returns(df, str(s['signal_date']), s['price'])
            for h in HORIZONS:
                if fw.get(f't{h}_ready'):
                    pnl = fw[f't{h}_pnl']
                    stats[f't{h}_n'] += 1
                    stats[f't{h}_pnl_sum'] += pnl
                    if pnl > 0:
                        stats[f't{h}_win'] += 1
        for h in HORIZONS:
            n = stats[f't{h}_n']
            stats[f't{h}_winrate'] = stats[f't{h}_win'] / n if n else None
            stats[f't{h}_avg'] = stats[f't{h}_pnl_sum'] / n if n else None
        monthly.append(stats)

    # 门禁判定：连续两月（主口径 T+5 BUY 全量）均盈亏 >0
    def _gate(key_avg: str) -> tuple[bool, str]:
        vals = [(m['month'], m[key_avg]) for m in monthly if m.get(key_avg) is not None]
        if len(vals) < 2:
            return False, f'成熟月份不足 2 个月（{len(vals)}），无法判定'
        last2 = vals[-2:]
        if all(v > 0 for _, v in last2):
            return True, f"{last2[0][0]}({last2[0][1]:+.2%}) 与 {last2[1][0]}({last2[1][1]:+.2%}) 连续为正 → 可恢复标准仓位"
        detail = ' / '.join(f'{mo}({v:+.2%})' for mo, v in vals)
        return False, f'未连续为正：{detail} → 维持仓位折减'

    gate5, reason5 = _gate('t5_avg')
    return {'months': monthly, 'gate_t5_pass': gate5, 'gate_t5_reason': reason5,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M')}


def render_report(result: dict) -> str:
    lines = [
        '# 月度复验报告（自动生成）',
        '',
        f"- 生成时间：{result['generated_at']}",
        "- 口径：T+5/T+10 交易日前向收益 vs 信号价；门禁以 T+5 为主口径",
        '',
        '| 月份 | 信号数 | BUY | WATCH | 取数失败 | T+5 成熟 | T+5 胜率 | T+5 均盈亏 | T+10 成熟 | T+10 胜率 | T+10 均盈亏 |',
        '|---|---|---|---|---|---|---|---|---|---|---|',
    ]
    for m in result['months']:
        def _pct(v):
            return f'{v:.1%}' if v is not None else '-'
        lines.append(
            f"| {m['month']} | {m['n_total']} | {m['n_buy']} | {m['n_watch']} | "
            f"{m['fetch_fail']} | {m['t5_n']} | {_pct(m['t5_winrate'])} | {_pct(m['t5_avg'])} | "
            f"{m['t10_n']} | {_pct(m['t10_winrate'])} | {_pct(m['t10_avg'])} |")
    lines += [
        '',
        f"**门禁判定（连续两月均盈亏 >0 → 恢复标准仓位）**：{'✅ 通过' if result['gate_t5_pass'] else '❌ 未通过'}",
        '',
        result['gate_t5_reason'],
        '',
        ('> 报告为历史统计映射，不构成投资建议；rules JSON 调整仍须用户确认'
         '（D28 纪律：文档裁定与配置合入之间有显式中间态）。'),
    ]
    return '\n'.join(lines)


def main():
    ap = argparse.ArgumentParser(description='月度复验：生产信号 T+N 前向收益归月汇总')
    ap.add_argument('--csv', help='signals CSV（ECS sqlite3 -header -csv 导出）')
    ap.add_argument('--db', help='本地 history.db 路径（与 --csv 二选一）')
    ap.add_argument('--months', type=int, default=3, help='回溯月数（默认 3）')
    ap.add_argument('--out', help='报告输出路径（默认打印到 stdout）')
    args = ap.parse_args()

    if args.csv:
        signals = load_signals_csv(args.csv)
    elif args.db:
        signals = load_signals_db(args.db)
    else:
        ap.error('必须提供 --csv 或 --db')

    if not signals:
        print('无信号数据，退出')
        return
    print(f'加载信号 {len(signals)} 条，回溯 {args.months} 个月…')
    result = run_recheck(signals, args.months)
    report = render_report(result)
    if args.out:
        Path(args.out).write_text(report, encoding='utf-8')
        print(f'报告已写入 {args.out}')
    else:
        print(report)


if __name__ == '__main__':
    main()
