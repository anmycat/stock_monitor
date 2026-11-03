"""baostock 日频离线校验基准（D53，2026-11-01）

用途：把项目主链数据（腾讯/同花顺）与**独立第三方源** baostock 做日频
交叉校验，验证 close / volume 单位 / amount / 换手率口径是否一致。

为什么是"离线校验"而不是主链数据源：
  - 调研实测：baostock 单 IP 单连接、间隔 ≥800ms~1s、单日约 3000 次后熔断，
    高频静默封 IP 1~24h → 绝不能进盘中主循环
  - baostock 免注册免 Key（login 匿名），字段比腾讯更全：
    tradestatus(1正常/0停牌) / isST / turn(换手率) / pctChg
  - 实测与腾讯/同花顺三方逐字段一致（600519 2026-10-30）：
      close   1392.05（腾讯 1392.050 / 同花顺 1392.05）
      volume  5,992,729 股（腾讯 59,927 手 ×100 / 同花顺 5,992,729）
      amount  8,257,381,777.60（同花顺 8,257,381,800.00）
      turn    0.4794（同花顺 0.479）

用法：
  python3 research/scripts/baostock_verify.py --codes 600519.SH,000001.SZ \
      [--days 30] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--source tencent]

依赖：pip install baostock（未安装则提示并退出，不影响主链）
"""
from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)


def _to_bs_code(code: str) -> str | None:
    """标准代码 → baostock 代码（sh.600519 / sz.000001 / bj.920819）"""
    sym = str(code or '').split('.')[0]
    if not sym.isdigit() or len(sym) != 6:
        return None
    if sym[0] == '6':
        return f'sh.{sym}'
    if sym[0] in ('4', '8') or sym.startswith('92'):
        return f'bj.{sym}'
    return f'sz.{sym}'


def fetch_baostock(code: str, start: str, end: str) -> pd.DataFrame:
    """拉取 baostock 日K（不复权 adjustflag=3，与项目口径一致）"""
    import baostock as bs
    lg = bs.login()
    if lg.error_code != '0':
        raise RuntimeError(f'baostock login failed: {lg.error_msg}')
    try:
        rs = bs.query_history_k_data_plus(
            _to_bs_code(code),
            'date,code,open,high,low,close,volume,amount,pctChg,turn,tradestatus,isST',
            start_date=start, end_date=end, frequency='d', adjustflag='3')
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=[
            'date', 'bs_code', 'open', 'high', 'low', 'close',
            'volume', 'amount', 'pct_chg', 'turnover_rate',
            'trade_status', 'is_st'])
        for col in ('open', 'high', 'low', 'close', 'volume', 'amount',
                    'pct_chg', 'turnover_rate'):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    finally:
        bs.logout()


def fetch_project(code: str, start: str, end: str, source: str) -> pd.DataFrame:
    """拉取项目主链日K（默认腾讯 qfq 前复权）"""
    if source == 'ths_web':
        from quant_engine.data.sources.ths_web import ThsWebSource
        df = ThsWebSource.get_daily_data(code, days=250)
    else:
        from quant_engine.data.sources.tencent import TencentSource
        df = TencentSource.get_daily_data(code, days=250)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    df = df[df['date'] >= start].reset_index(drop=True)
    return df[df['date'] <= end].reset_index(drop=True)


def compare(bs_df: pd.DataFrame, prj_df: pd.DataFrame, tolerance: float = 0.005) -> dict:
    """逐日对比 close/volume/amount（volume 按「股」口径）"""
    if bs_df.empty or prj_df.empty:
        return {'ok': False, 'reason': 'empty data', 'rows': 0}
    merged = bs_df.merge(prj_df, on='date', how='inner', suffixes=('_bs', '_prj'))
    if merged.empty:
        return {'ok': False, 'reason': 'no overlapping dates', 'rows': 0}
    close_diff = (merged['close_bs'] - merged['close_prj']).abs()
    close_ok = bool((close_diff <= merged['close_bs'].abs() * tolerance + 0.001).all())
    vol_ratio = merged['volume_prj'] / merged['volume_bs'].replace(0, pd.NA)
    vol_ok = bool(vol_ratio.dropna().apply(
        lambda r: 0.99 <= float(r) <= 1.01).all()) if vol_ratio.notna().any() else True
    amt_ok = True
    # amount 列可能只有一侧有（腾讯 fqkline 不返回成交额）→ 按实际列名取
    amt_bs = 'amount_bs' if 'amount_bs' in merged.columns else (
        'amount' if 'amount' in merged.columns else None)
    amt_prj = 'amount_prj' if 'amount_prj' in merged.columns else (
        'amount' if 'amount' in merged.columns else None)
    if amt_bs and amt_prj and merged[amt_bs].notna().any():
        amt_ratio = merged[amt_prj] / merged[amt_bs].replace(0, pd.NA)
        amt_ok = bool(amt_ratio.dropna().apply(
            lambda r: 0.99 <= float(r) <= 1.01).all()) if amt_ratio.notna().any() else True
    return {
        'ok': close_ok and vol_ok and amt_ok,
        'rows': len(merged),
        'close_ok': close_ok,
        'volume_ok': vol_ok,
        'amount_ok': amt_ok,
        'max_close_diff': round(float(close_diff.max()), 6),
        'volume_ratio_median': (None if vol_ratio.dropna().empty
                                 else round(float(vol_ratio.dropna().median()), 4)),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--codes', required=True, help='逗号分隔的标准代码，如 600519.SH,000001.SZ')
    ap.add_argument('--start', default='')
    ap.add_argument('--end', default='')
    ap.add_argument('--days', type=int, default=30)
    ap.add_argument('--source', default='tencent', choices=['tencent', 'ths_web'])
    ap.add_argument('--tolerance', type=float, default=0.005)
    args = ap.parse_args()

    try:
        import baostock  # noqa: F401
    except ImportError:
        print('baostock 未安装：pip install baostock（仅离线校验用，不影响主链）')
        return 2

    end = args.end or pd.Timestamp.today().strftime('%Y-%m-%d')
    start = args.start or (pd.Timestamp(end) - pd.Timedelta(days=args.days)).strftime('%Y-%m-%d')
    codes = [c.strip() for c in args.codes.split(',') if c.strip()]

    all_ok = True
    for code in codes:
        try:
            bs_df = fetch_baostock(code, start, end)
            prj_df = fetch_project(code, start, end, args.source)
            res = compare(bs_df, prj_df, args.tolerance)
        except Exception as e:  # noqa: BLE001 单票失败不影响其余
            res = {'ok': False, 'reason': f'{type(e).__name__}: {e}', 'rows': 0}
        all_ok = all_ok and res.get('ok', False)
        flag = '✅' if res.get('ok') else '❌'
        print(f'{flag} {code}: {res}')
    print(f"\n结论：{'全部一致' if all_ok else '存在差异（见上）'}（源={args.source}，窗口 {start}~{end}）")
    return 0 if all_ok else 1


if __name__ == '__main__':
    sys.exit(main())
