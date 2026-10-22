"""股票代码工具 - 全项目代码格式的唯一真相（single source of truth）

背景（事故驱动）：
原项目出现过「代码与实际股票不匹配」——同一只股票在不同数据源被解析成
不同的标准代码（典型：510300 沪深300ETF 在东财源为 510300.SH，在腾讯源
因 startswith('6') 推断被错判为 510300.SZ），导致：
- Redis quote:{code} 缓存同股双键分裂，行情对不上；
- ETF 监控按 code 匹配 target_codes 失败，持续显示"数据获取中"；
- 北交所股票（8/4/920 开头）在东财/新浪/腾讯源全部错判市场。

设计原则：
1. 行情源一律使用数据源自带的市场标识解析（腾讯行首 v_sh/sz/bj、
   东财 f13 市场字段、新浪行首前缀），本模块的静态前缀规则仅作为
   「无市场字段场景」（天天基金 F10 HTML、手写配置）的兜底；
2. 标准代码格式统一为 "XXXXXX.SH/.SZ/.BJ"（6位数字+.市场后缀）；
3. 各数据源适配函数（to_secid/to_tencent_code/to_sina_code）集中在此，
   避免散落各处的隐式转换。

市场规则（实测口径 2026-10-18）：
- 沪市 SH：6 开头（主板 600/601/603/605、科创 688）、5 开头（ETF/LOF 基金
  51xx/515xx/58xx）、9 开头（B 股 900xxx）
- 深市 SZ：0 开头（主板 000/001/003）、3 开头（创业板 300/301）、
  2 开头（B 股 200xxx）、1 开头（可转债 11/12/15...）
- 北交所 BJ：4 开头（430/8xx 老三板转板）、8 开头（83/87/88）、
  92 开头（920xxx 新代码段，2024 年起启用）
- 东财 secid：1.=沪市，0.=深市/北交所（920819 实测 f13=0，secid 用 0. 有效）
"""
from __future__ import annotations
import re
from typing import Optional

__all__ = [
    'MARKET_SH', 'MARKET_SZ', 'MARKET_BJ',
    'MARKET_RULES', 'market_by_symbol', 'market_of_code',
    'normalize_code', 'to_secid', 'to_tencent_code', 'to_sina_code',
    'is_valid_code', 'code_key', 'split_code',
]

MARKET_SH = 'SH'
MARKET_SZ = 'SZ'
MARKET_BJ = 'BJ'
VALID_MARKETS = (MARKET_SH, MARKET_SZ, MARKET_BJ)

_CODE_RE = re.compile(r'^(\d{6})(?:\.(SH|SZ|BJ))?$', re.IGNORECASE)

# 代码段 → 市场 规则表（顺序即优先级，92 必须先于 9 判断）
# (前缀, 市场) — 精确到前缀匹配
MARKET_RULES = [
    ('92', MARKET_BJ),   # 920xxx 北交所新段
    ('6', MARKET_SH),    # 沪市主板/科创
    ('5', MARKET_SH),    # 沪市 ETF/LOF 基金
    ('9', MARKET_SH),    # 沪市 B 股
    ('0', MARKET_SZ),    # 深市主板
    ('3', MARKET_SZ),    # 创业板
    ('2', MARKET_SZ),    # 深市 B 股
    ('1', MARKET_SZ),    # 深市可转债
    ('4', MARKET_BJ),    # 北交所（老三版/新三板转板）
    ('8', MARKET_BJ),    # 北交所
]


def market_by_symbol(symbol: str) -> str | None:
    """由 6 位数字代码推断市场（静态兜底规则，仅用于无市场字段的源）

    Args:
        symbol: 6 位数字代码，如 "510300"、"920819"；可带后缀（自动剥离）

    Returns:
        'SH' / 'SZ' / 'BJ'；无法识别返回 None
    """
    if not symbol:
        return None
    s = str(symbol).strip().upper()
    if '.' in s:
        s = s.split('.')[0]
    if not re.match(r'^\d{6}$', s):
        return None
    for prefix, market in MARKET_RULES:
        if s.startswith(prefix):
            return market
    return None


def market_of_code(code: str) -> str | None:
    """取标准代码的市场后缀；非法代码返回 None"""
    parts = split_code(code)
    return parts[1] if parts else None


def split_code(code: str) -> tuple | None:
    """拆分标准代码为 (6位数字, 市场后缀)；非法返回 None

    先经 normalize_code 归一化（兼容裸码/前缀/secid 等任意输入格式），
    保证全项目对「标准代码」的判定只有一套逻辑。
    """
    normalized = normalize_code(code)
    if not normalized:
        return None
    symbol, market = normalized.split('.')
    return symbol, market


def normalize_code(code: str) -> str | None:
    """归一化任意格式代码为标准格式 "510300.SH"

    接受："510300" / "510300.SH" / "sh510300" / "SH510300" /
          "1.510300"（东财 secid）/ "510300.sh" / " 510300 "
    不合法返回 None（调用方应据此拒绝，切勿静默通过）
    """
    if code is None:
        return None
    s = str(code).strip().upper()
    if not s:
        return None

    # 东财 secid 形式: 1.510300 / 0.920819
    if re.match(r'^[01]\.\d{6}$', s):
        secid_market, symbol = s.split('.')
        market = MARKET_SH if secid_market == '1' else None
        if market is None:
            # 0. 前缀下深市与北交所共存，按前缀规则细分
            market = market_by_symbol(symbol)
        if not market:
            return None
        return f"{symbol}.{market}"

    # 腾讯/新浪形式: sh510300 / sz000001 / bj920819
    m = re.match(r'^(SH|SZ|BJ)(\d{6})$', s)
    if m:
        return f"{m.group(2)}.{m.group(1)}"

    # 标准形式: 510300 / 510300.SH（大小写不敏感）
    m = _CODE_RE.match(s)
    if m:
        symbol, market = m.group(1), m.group(2)
        if not market:
            market = market_by_symbol(symbol)
            if not market:
                return None
        return f"{symbol}.{market.upper()}"

    return None


def to_secid(code: str) -> str | None:
    """标准代码 → 东财 secid（"1.510300"）

    实测口径：沪市 secid 前缀 1.；深市与北交所共用 0. 前缀
    （920819 用 0.920819 查询有效，东财内部 f13=0 归深市场组）。
    """
    parts = split_code(code)
    if not parts:
        return None
    symbol, market = parts
    return f"1.{symbol}" if market == MARKET_SH else f"0.{symbol}"


def to_tencent_code(code: str) -> str | None:
    """标准代码 → 腾讯代码（"sh510300" / "bj920819"）"""
    parts = split_code(code)
    if not parts:
        return None
    symbol, market = parts
    return f"{market.lower()}{symbol}"


def to_sina_code(code: str) -> str | None:
    """标准代码 → 新浪代码（"sh510300" / "bj920819"，实测新浪支持 bj 前缀）"""
    return to_tencent_code(code)


def is_valid_code(code: str) -> bool:
    """标准代码合法性：6 位数字 + 已知市场后缀（或可由前缀规则推断）"""
    return normalize_code(code) is not None


def code_key(code: str) -> str | None:
    """提取 6 位数字 key（快照/diff/Redis 键用，与市场后缀解耦）

    ETF 持仓快照的 diff 以 6 位 code 为匹配键，跨源统一。
    """
    parts = split_code(code)
    return parts[0] if parts else None
