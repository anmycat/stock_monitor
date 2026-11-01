"""数据源字段对齐层（D53，2026-11-01）

把各源的原始列名映射到 `spec.STANDARD_COLUMNS` 定义的标准列：
  - 源列名 → 标准列名（按能力逐源声明，见 registry.COLUMN_MAPPINGS）
  - 缺失的标准列填 None（不猜不造）
  - 多余的源列直接丢弃
  - 单位换算只在「volume」一列发生，且换算依据 registry 里声明的
    volume_unit 口径（腾讯 688/689=股 / 其他板=手 → 统一落库为股）

对齐层**不解决复权口径差异**（腾讯 qfq vs 东财不复权约 0.5% 价差）——
那属于"源口径声明 + 契约测试"范畴，见 registry.SourceSpec.adjust。
"""
from __future__ import annotations

import pandas as pd

from .spec import Capability

#: 腾讯成交量单位分裂：688/689（科创板）返回「股」，其余板返回「手」
TENCENT_SHARES_CODES = ('688', '689')


def _tencent_volume_factor(code: str) -> float:
    """腾讯成交量单位换算系数（手 → 股 的 100 倍；科创板为 1）"""
    sym = str(code or '').split('.')[0]
    if sym.startswith(TENCENT_SHARES_CODES):
        return 1.0
    return 100.0


def align(capability: Capability | str,
          raw: pd.DataFrame | None,
          mapping: dict[str, str],
          *,
          volume_factor: float = 1.0) -> pd.DataFrame:
    """把原始 DataFrame 对齐到标准列

    Args:
        capability: 目标能力（决定标准列集合）
        raw: 源返回的 DataFrame（None/空 → 返回空标准列 DataFrame）
        mapping: 源列名 → 标准列名
        volume_factor: 成交量换算系数（1.0 = 不换算）

    Returns:
        含全部标准列的 DataFrame（缺失列填 None）
    """
    from .spec import STANDARD_COLUMNS

    cap = Capability(capability) if not isinstance(capability, Capability) else capability
    cols = STANDARD_COLUMNS.get(cap, ())
    if raw is None or len(raw) == 0:
        return pd.DataFrame(columns=list(cols))

    out = pd.DataFrame(index=raw.index)
    for src_col, dst_col in mapping.items():
        if dst_col not in cols:
            continue
        if src_col in raw.columns:
            out[dst_col] = raw[src_col].values
    for col in cols:
        if col not in out.columns:
            out[col] = None
    out = out[list(cols)]
    if volume_factor != 1.0 and 'volume' in out.columns and 'volume' in mapping.values():
        out['volume'] = pd.to_numeric(out['volume'], errors='coerce') * volume_factor
    return out


def align_daily_tencent(raw: pd.DataFrame | None, code: str) -> pd.DataFrame:
    """腾讯日 K 专用对齐（含成交量单位换算）"""
    mapping = {
        'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
        'close': 'close', 'volume': 'volume',
    }
    return align(Capability.DAILY, raw, mapping,
                 volume_factor=_tencent_volume_factor(code))


def align_daily_default(raw: pd.DataFrame | None) -> pd.DataFrame:
    """标准日 K 对齐（成交量已是「股」的源：新浪/同花顺/baostock）"""
    mapping = {
        'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
        'close': 'close', 'volume': 'volume', 'amount': 'amount',
        'turnover_rate': 'turnover_rate', 'turnoverRate': 'turnover_rate',
    }
    return align(Capability.DAILY, raw, mapping)


def align_realtime_default(raw: pd.DataFrame | None) -> pd.DataFrame:
    """标准实时报价对齐"""
    mapping = {
        'code': 'code', 'name': 'name', 'price': 'price',
        'prev_close': 'prev_close', 'change_pct': 'change_pct',
        'volume': 'volume', 'amount': 'amount',
        'open': 'open', 'high': 'high', 'low': 'low',
    }
    return align(Capability.REALTIME, raw, mapping)


def align_pool_default(raw: pd.DataFrame | None) -> pd.DataFrame:
    """盘面池对齐（涨停/炸板/跌停）"""
    mapping = {
        'code': 'code', 'name': 'name', 'price': 'price',
        'change_pct': 'change_pct', 'amount': 'amount',
        'limit_up_times': 'limit_up_times', 'seal_fund': 'seal_fund',
        'broken_times': 'broken_times', 'industry': 'industry',
    }
    return align(Capability.POOL, raw, mapping)
