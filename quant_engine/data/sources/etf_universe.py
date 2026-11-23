"""ETF 全市场榜单数据源（D79，旁路功能：不参与任何趋势信号计算）。

数据：东财 clist ETF 板块列表（b:MK0021..24），字段 f12/f14/f2/f3/f6/f8/f20。
能力：按主题关键词（红利/股息/低波/央企…）筛选 + 规模/成交额排序，产出「优势 ETF 榜」。
纪律：
  - 所有 HTTP 走 em_get（D68 全局限速 + RST 恢复统一出口），不得直连 requests
  - 分页失败即返回已抓部分并标注 partial（绝不返回看似完整的半量榜）
  - 缓存 30 分钟，只经调用方注入的 cache（绝不自建连接池/缓存）
  - 不修改任何 df，不写 df.attrs —— 纯旁路，趋势信号零影响
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass

from loguru import logger

from .eastmoney import em_get

CLIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
FS_ETF = "b:MK0021,b:MK0022,b:MK0023,b:MK0024"
FIELDS = "f12,f14,f2,f3,f5,f6,f8,f20,f21"
CACHE_KEY = "etf_universe:leaders"
CACHE_TTL = 1800

# 主题关键词 → 展示名（名称匹配，大小写不敏感；红利/股息/低波等优势策略族）
THEMES: dict[str, tuple[str, ...]] = {
    "dividend": ("红利", "股息", "分红"),
    "lowvol": ("低波", "低波动"),
    "central": ("央企", "国企", "中特估"),
    "broad": ("沪深300", "中证500", "上证50", "中证1000", "A500"),
}


@dataclass
class ETFLeader:
    code: str          # 6 位代码
    name: str
    theme: str
    price: float
    change_pct: float
    amount: float      # 成交额（元）
    turnover_pct: float
    scale: float       # 规模（元，f20）
    rank: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def _parse_rows(rows: dict) -> list[dict]:
    out = []
    for v in rows.values():
        code = str(v.get("f12", "")).zfill(6) if v.get("f12") is not None else ""
        name = str(v.get("f14", ""))
        if not code or not name or code.startswith("-"):
            continue
        out.append({
            "code": code,
            "name": name,
            "price": _num(v.get("f2")) / 1000.0,
            "change_pct": _num(v.get("f3")) / 100.0,
            "amount": _num(v.get("f6")),
            "turnover_pct": _num(v.get("f8")) / 100.0,
            "scale": _num(v.get("f20")),
        })
    return out


def _num(v) -> float:
    try:
        return float(v) if v not in ("-", None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def fetch_etf_universe(max_pages: int = 17, page_size: int = 100) -> tuple[list[dict], bool]:
    """抓全市场 ETF 快照。返回 (rows, partial)；partial=True 表示分页提前失败。"""
    allrows: dict[str, dict] = {}
    partial = False
    for pn in range(1, max_pages + 1):
        params = {"pn": pn, "pz": page_size, "po": 1, "fid": "f6",
                  "fs": FS_ETF, "fields": FIELDS}
        try:
            resp = em_get(CLIST_URL, params=params, timeout=15)
            data = resp.json().get("data")
            if not data or not data.get("diff"):
                break  # 正常到尾
            for row in _parse_rows(data["diff"]):
                allrows[row["code"]] = row
        except Exception as exc:
            logger.warning(f"[ETF] 全市场列表第 {pn}/{max_pages} 页抓取失败：{type(exc).__name__}")
            partial = True
            break
        time.sleep(0.2)  # 温和分页，全局限速已守，此处只做二次保护
    return list(allrows.values()), partial


def select_leaders(rows: list[dict], theme: str = "dividend",
                   limit: int = 10, min_scale: float = 5e8) -> list[dict]:
    """按主题筛「优势 ETF」：名称含关键词 + 规模门槛，按规模降序取前 N。

    min_scale 默认 5 亿元——低于此的 ETF 多为同质化迷你产品，流动性差。
    """
    keywords = THEMES.get(theme)
    if keywords is None:
        raise ValueError(f"未知主题: {theme}（可选 {list(THEMES)}）")
    matched = [r for r in rows
               if r["scale"] >= min_scale and any(k in r["name"] for k in keywords)]
    matched.sort(key=lambda r: (-r["scale"], -r["amount"]))
    out = []
    for i, r in enumerate(matched[:limit], 1):
        item = ETFLeader(theme=theme, rank=i, **r)
        out.append(item.to_dict())
    return out


def get_leaders(cache, theme: str = "dividend", limit: int = 10,
                min_scale: float = 5e8) -> dict:
    """带缓存的榜单入口。cache 需提供 get/set（注入，绝不自建）。"""
    key = f"{CACHE_KEY}:{theme}:{limit}:{int(min_scale)}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    rows, partial = fetch_etf_universe()
    leaders = select_leaders(rows, theme=theme, limit=limit, min_scale=min_scale)
    result = {"theme": theme, "as_of": time.strftime("%Y-%m-%d %H:%M:%S"),
              "total_universe": len(rows), "partial": partial, "leaders": leaders}
    cache.set(key, result, ttl=CACHE_TTL)
    return result
