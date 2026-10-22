"""行情数据卫生断言（B5，2026-10-21）

背景（13 报告 §4.3 + 14 §3 B5）：东财冷却期实时行情 change_pct 脏数据曾导致
"/api/market 100 只全涨 11.35%、文案 >100%"——计算出口无数据卫生断言。
本模块是出口断言的单一事实源，被 MarketDataRepository.get_market_overview
（P1 重构后，原 DataCenter）与 MarketAnalyzer._calculate_breadth 共用。

设计决策（与验收标准"夹紧"的差异说明）：超限行**剔除**而非夹紧——
脏值（如 change_pct=1135）夹紧成边界合法值（10.5）仍会参与统计并扭曲
up/down/avg_change；剔除只丢该行、保留其余样本的统计纯净。
夹紧仅用于出口数值的最终防御（avg_change/breadth 的理论界）。

新股豁免（2026-10-21 M2' 实测补充）：证券简称带 N/C 前缀（N=上市首日、
C=第 2-5 日）的新股不设涨跌幅限制（注册制全市场适用，含北交所），
其涨跌幅可远超号段上限——实测 920296.BJ "N文峰" 首日 +217.03%。
若按号段上限剔除会误丢合法行，故对 ^[NC][汉字] 的行豁免上限判定
（仍受 price>0 与非有限值约束）。
"""
import re
from typing import Dict, Tuple

import pandas as pd
from loguru import logger

# 新股标识：N（上市首日）/ C（第 2-5 日）+ 汉字简称，如 "N文峰"
_NEW_LISTING_RE = re.compile(r"^[NC][\u4e00-\u9fff]")


def change_pct_limit(code: str) -> float:
    """按代码段返回涨跌幅物理上限（含 0.5 绝对容差，防源端四舍五入误差）

    规则与 utils/code_utils.MARKET_RULES 同源：SH=6/5/9、SZ=0/1/2/3、BJ=4/8/92。
    - 科创板 688/689（CDR）与创业板 300/301/302：±20.5%
    - 北交所（.BJ 后缀，或 4/8/92 数字段兜底）：±30.5%
    - 其余（主板 60x/00x/001/003/605 等）：±10.5%

    段口径依据 2026-10-21 全市场实测（5575 只）：301 段 474 只 max 20.00、
    300 段 max 20.04（涨停价四舍五入超 20.0，需容差否则误剔）、302 段已启用；
    主板各段 max ≤10.19。新股上市首五日不设涨跌幅（N/C 前缀）由
    sanitize_quotes 豁免，不走本函数。
    """
    code = str(code)
    num = code.split(".")[0]
    if num.startswith(("688", "689", "300", "301", "302")):
        return 20.5
    if code.endswith(".BJ") or num.startswith(("4", "8", "92")):
        return 30.5
    return 10.5


def sanitize_quotes(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """净化实时行情 DataFrame：剔除 change_pct 超限 / price<=0 / 非有限值行

    返回 (clean_df, stats)。stats: {total, dropped, drop_ratio, reasons}
    """
    if df is None or df.empty:
        return pd.DataFrame(), {"total": 0, "dropped": 0, "drop_ratio": 0.0, "reasons": {}}

    total = len(df)
    reasons: Dict[str, int] = {}
    mask_keep = pd.Series(True, index=df.index)

    if "change_pct" in df.columns:
        pct = pd.to_numeric(df["change_pct"], errors="coerce")
        # 非有限值（NaN/±inf）剔除
        finite = pct.notna() & (pct.abs() != float("inf"))
        mask_keep &= finite
        reasons["non_finite"] = int((~finite).sum())
        # 逐行按代码段判定超限（N/C 前缀新股首五日不设涨跌幅，豁免）
        if "code" in df.columns:
            limits = df["code"].map(change_pct_limit)
            over = pct.notna() & (pct.abs() > limits)
            if "name" in df.columns:
                new_listing = df["name"].astype(str).str.match(_NEW_LISTING_RE)
                exempt = new_listing & over
                if exempt.any():
                    reasons["new_listing_exempt"] = int(exempt.sum())
                over &= ~new_listing
        else:  # 无 code 列时按最宽主板限兜底
            over = pct.notna() & (pct.abs() > 10.5)
        mask_keep &= ~over
        reasons["over_limit"] = int(over.sum())
    else:
        reasons["no_change_pct_column"] = total
        return pd.DataFrame(), {
            "total": total, "dropped": total, "drop_ratio": 1.0,
            "reasons": {"no_change_pct_column": total},
        }

    if "price" in df.columns:
        price = pd.to_numeric(df["price"], errors="coerce")
        bad_price = ~(price > 0)
        mask_keep &= ~bad_price
        reasons["non_positive_price"] = int(bad_price.sum())

    clean = df[mask_keep].copy()
    # 数值型写回，便于下游直接比较（替代原 get_market_overview 的 to_numeric+fillna(0)）
    if "change_pct" in clean.columns:
        clean["change_pct"] = pd.to_numeric(clean["change_pct"], errors="coerce").fillna(0)
    if "price" in clean.columns:
        clean["price"] = pd.to_numeric(clean["price"], errors="coerce")
    dropped = total - len(clean)
    if dropped:
        logger.warning(f"[market_sanity] dropped {dropped}/{total} quote rows: {reasons}")
    return clean, {
        "total": total,
        "dropped": dropped,
        "drop_ratio": round(dropped / total, 4) if total else 0.0,
        "reasons": reasons,
    }


def validate_dataframe(df: pd.DataFrame, name: str) -> bool:
    """入库/出库前的基础校验：空值与空值计数告警（P1 从 DataCenter 迁入）

    只做"空则否 + 有空值记 warning"的轻校验，与 sanitize_quotes 的
    物理上限净化分工：本函数管结构性缺失，sanitize_quotes 管脏值。
    空 DataFrame 返回 False；有空值不阻断（记 warning 后仍返回 True）。
    """
    if df is None or df.empty:
        logger.warning(f"[market_sanity] {name}: 数据为空")
        return False
    if df.isnull().any().any():
        null_count = int(df.isnull().sum().sum())
        logger.warning(f"[market_sanity] {name}: 存在 {null_count} 个空值")
    return True


# 剔除率超过该值视为数据源系统性损坏（而非零星脏行），触发 suspect 回退
DROP_RATIO_SUSPECT = 0.20


def is_overview_sane(overview: Dict) -> bool:
    """出口断言：结构一致性 + 数值理论界 + 分布矛盾（全部通过才可出厂）

    注意：全涨且均涨大（如 100 只全涨 11.35%）结构上自洽（涨停潮理论可能），
    不在此层拦截——防脏值（11.35% 对主板超物理上限）是 sanitize_quotes 的职责。
    本层只抓**分布矛盾**这类零误报的硬错误。
    """
    if not isinstance(overview, dict):
        return False
    total = overview.get("total", 0)
    if not isinstance(total, int) or total <= 0:
        return False
    up, down, flat = overview.get("up", -1), overview.get("down", -1), overview.get("flat", -1)
    if min(up, down, flat) < 0:
        return False
    if up + down + flat != total:
        return False
    avg = overview.get("avg_change", 999)
    if not isinstance(avg, (int, float)) or not (-20.0 <= avg <= 20.0):
        return False
    if not (0.0 <= up / total <= 1.0):
        return False
    # 分布矛盾（零误报硬错误）：全涨均跌 / 全跌均涨 / 全平均涨却无上涨家数
    if up == total and avg < 0:
        return False
    if down == total and avg > 0:
        return False
    if up == 0 and down == 0 and avg != 0:
        return False
    return True
