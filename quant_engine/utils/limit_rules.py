"""涨跌停口径单一事实源（D63）

**为什么单独成模块**：涨停/跌停家数是市场异动告警与日报的输入，口径错了会直接
误导判断（2026-11-06 实战：83% 下跌的普跌日被判「市场情绪极度亢奋」，与同屏的
「市场普跌」自相矛盾）。根因有二——① 统一用 `>= 9.9` 判涨跌停，创业板/科创板
（20%）、北交所（30%）全被误算；② 情绪结论只看家数、不看市场宽度。

**两个函数必须分开，不可混用**：

| 函数 | 用途 | ST 敏感 |
|---|---|---|
| `limit_pct_nominal(code, name)` | **判定是否涨跌停**（名义幅度 5/10/20/30） | 是（ST 名义 5%） |
| `limit_pct_physical(code)` | **脏行剔除**（名义 + 0.5 容差） | **否** |

为什么物理上限要对 ST 不敏感：若把 ST 的上限收紧到 5.5，`sanitize_quotes` 会剔除
ST 股 +6% 的合法行情 → `market.breadth` 变化 → 门控判定变化 → **盘中信号漂移**
（违反 D41 纪律）。故 ST 判定只用于涨停家数统计，绝不进入数据卫生层。

**容差来源（解析推导，非拍脑袋）**：涨停价 = round(前收 × (1 + 名义), 2)，四舍五入
误差 ≤ 0.01 元，相对误差随股价递减。实测全价位段（0.01~1999.99 元）：
- 名义 10%：真实涨停 change_pct 落在 **[7.14%, 20.00%]**（下界由 1 元以下低价股贡献）
- 按价位细分：3~10 元 [9.868%, 10.164%]、10~30 元 [9.960%, 10.050%]、100 元+ [9.996%, 10.005%]
因此固定 0.5pp 容差对 3 元以上股票安全；低于 3 元的低价股按
`100 × 0.011 / 前收` 自适应放宽（上限 3pp，且不得吃满名义幅度）。
"""
from __future__ import annotations

# ST / *ST / SST / S*ST 前缀 → 名义 5%
_ST_PREFIXES = ('ST', '*ST', 'SST', 'S*ST')

# 创业板 / 科创板 → 20%
_GROWTH_BOARD = ('688', '689', '300', '301', '302')

# 北交所 → 30%
_BJ_BOARD = ('4', '8', '92')

# 物理上限容差（绝对百分点）
_PHYSICAL_TOLERANCE = 0.5


def limit_pct_nominal(code: str, name: str | None = None) -> float:
    """该股当日**名义**涨跌停幅度（%）

    - 创业板(300/301/302) / 科创板(688/689)：**20%**
    - 北交所(4/8/92 段 或 .BJ)：**30%**
    - 主板 ST / *ST / SST 前缀：**5%**
    - 其余主板（沪 60x/605、深 00x/001/002/003）：**10%**

    **ST 判定只对主板生效**（易错点）：注册制板块（创业板/科创板）的 ST/*ST 股
    涨跌幅限制与普通股同为 **20%**；北交所 ST 股同为 **30%**。只有主板 ST 才是 5%。
    实测印证（2026-11-06 全市场实测）：300831「ST派瑞」+4.68% —— 若按 5% 判定会
    误认为涨停，实际其名义幅度为 20%，+4.68% 只是普通涨幅。
    """
    num = str(code).split('.')[0]
    # 注册制板块优先（ST 同幅度）
    if num.startswith(_GROWTH_BOARD):
        return 20.0
    if str(code).endswith('.BJ') or num.startswith(_BJ_BOARD):
        return 30.0
    # 主板 ST → 5%
    if name and str(name).upper().startswith(_ST_PREFIXES):
        return 5.0
    return 10.0


def limit_pct_physical(code: str) -> float:
    """该股涨跌幅**物理上限**（名义 + 0.5 容差），用于脏行剔除

    **ST 不敏感**（见模块 docstring）：本函数进入 `sanitize_quotes` → 影响
    `market.breadth` → 影响门控 → 影响盘中信号，必须保持历史行为不变。
    """
    num = str(code).split('.')[0]
    if num.startswith(_GROWTH_BOARD):
        return 20.0 + _PHYSICAL_TOLERANCE
    if str(code).endswith('.BJ') or num.startswith(_BJ_BOARD):
        return 30.0 + _PHYSICAL_TOLERANCE
    return 10.0 + _PHYSICAL_TOLERANCE


def _tolerance(pct: float, price: float | None, nominal: float) -> float:
    """涨停判定容差（绝对百分点）

    **按股价自适应**：涨停价 = round(前收 × (1 + 名义), 2)，舍入误差 ≤ 0.005 元，
    相对误差随股价递减；再叠加数据源 change_pct 自身的舍入噪声（±0.05pp 兜底）：

    | 前收 | 舍入相对误差 | 实际容差 | 判定阈值 |
    |---|---|---|---|
    | 100 元 | 0.005% | 0.05pp | 9.95% |
    | 10 元 | 0.05% | 0.10pp | 9.90% |
    | 3 元 | 0.167% | 0.22pp | 9.78% |
    | 1.5 元 | 0.333% | 0.38pp | 9.62% |

    实测校验（2026-11-06 全市场 5563 只）：主板真涨停 600278 +9.74%（前收约 1.49 元，
    四舍五入到 9.74% 而非 10%）→ 阈值 9.62% 可捕获；主板 +9.9% 的非涨停不被误计。
    上限封顶 3pp（1 元以下 penny 股因最小报价单位 0.01 元，涨停幅度失真）。
    **无价格信息时退化为 0.5pp 保守容差**（覆盖前收 ≥1 元的绝大多数标的）。
    """
    tol = 0.5 if not price else 0.05
    try:
        if price and price > 0 and pct is not None and pct > -100:
            prev = float(price) / (1 + float(pct) / 100)
            if 0 < prev < 20:
                err = 100 * 0.005 / prev + 0.05
                tol = max(tol, err)
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    tol = min(tol, 3.0)
    # 容差不得吃满名义幅度（否则 0% 也会被判涨停）
    return min(tol, max(0.05, nominal - 1.0))


def is_at_limit_up(pct: float | None, code: str, name: str | None = None,
                   price: float | None = None) -> bool:
    """是否处于涨停（含四舍五入容差）"""
    if pct is None:
        return False
    try:
        pct = float(pct)
    except (TypeError, ValueError):
        return False
    nominal = limit_pct_nominal(code, name)
    tol = _tolerance(pct, price, nominal)
    return pct >= nominal - tol


def is_at_limit_down(pct: float | None, code: str, name: str | None = None,
                     price: float | None = None) -> bool:
    """是否处于跌停（含四舍五入容差）"""
    if pct is None:
        return False
    try:
        pct = float(pct)
    except (TypeError, ValueError):
        return False
    nominal = limit_pct_nominal(code, name)
    tol = _tolerance(pct, price, nominal)
    return pct <= -(nominal - tol)
