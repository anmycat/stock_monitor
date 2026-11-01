"""数据源契约层（MCP 风格，D53，2026-11-01）

借鉴 MCP（Model Context Protocol）的设计原则，为项目多数据源建立**显式契约**：

  1. 能力声明（Capability）—— 每个源明确声明支持哪些能力，机器可判
  2. 统一信封（DataResult）—— 失败不是异常，而是结果的一部分
  3. 字段对齐（STANDARD_COLUMNS）—— 源列 → 标准列的唯一真源

设计依据（调研：research/docs/30 架构调研_TradingAgents-CN与MCP风格数据源
接口层.md）：TradingAgents-CN 因缺契约而并存两套互不通气的抽象，最终
"不敢重构"；本项目先立契约层（纯新增文件、零调用方改动），再逐源迁移。

字段口径声明（unit / adjust）是契约层的关键收益：腾讯 qfq 前复权与东财
不复权之间存在约 0.5% 的历史价差，靠"源口径声明 + 契约测试"兜住，
对齐层本身解决不了。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Capability(str, Enum):
    """数据源能力枚举（机器可判，不靠自由文本 features 列表）"""

    DAILY = 'daily'            # 日 K
    REALTIME = 'realtime'      # 实时报价
    LIST = 'list'              # 全市场标的列表
    MINUTE = 'minute'          # 分钟 K
    BOARD = 'board'            # 板块行情
    POOL = 'pool'              # 涨停/炸板/跌停池
    FUNDAMENTAL = 'fundamental'  # 财务/基本面

    def __str__(self) -> str:
        return self.value


#: 各能力的标准列（唯一真源）。对齐层只做「源列 → 标准列」的映射，
#: 缺失列填 None、多余列丢弃——不猜不造。
STANDARD_COLUMNS: dict[Capability, tuple[str, ...]] = {
    Capability.DAILY: (
        'date', 'open', 'high', 'low', 'close',
        'volume', 'amount', 'turnover_rate',
    ),
    Capability.REALTIME: (
        'code', 'name', 'price', 'prev_close', 'change_pct',
        'volume', 'amount', 'open', 'high', 'low',
    ),
    Capability.LIST: ('code', 'name', 'market', 'is_st'),
    Capability.MINUTE: ('date', 'time', 'open', 'high', 'low', 'close', 'volume'),
    Capability.BOARD: ('code', 'name', 'change_pct', 'amount', 'leader'),
    Capability.POOL: (
        'code', 'name', 'price', 'change_pct', 'amount',
        'limit_up_times', 'seal_fund', 'broken_times', 'industry',
    ),
    Capability.FUNDAMENTAL: ('code', 'name', 'report_date', 'roe', 'net_profit'),
}

#: 成交量单位口径（项目落库统一「股」）
UNIT_SHARES = 'shares'
UNIT_LOTS = 'lots'          # 手（×100 才是股）
UNIT_UNKNOWN = 'unknown'

#: 复权口径
ADJUST_NONE = 'none'
ADJUST_FORWARD = 'forward'  # 前复权
ADJUST_BACKWARD = 'backward'


@dataclass
class DataResult:
    """统一数据结果信封——失败是数据的一部分，不是异常

    字段与 MCP 的 tool result 语义对齐：调用方永远拿到一个可判定的对象，
    不需要 try/except 包裹每个源调用。
    """

    ok: bool
    data: Any = None
    source: str = ''
    capability: str = ''
    stale: bool = False          # 是否来自过期快照/降级路径
    elapsed_ms: int = 0
    error_code: str = ''
    error_msg: str = ''
    fetched_at: float = field(default_factory=time.time)
    meta: dict = field(default_factory=dict)

    @classmethod
    def success(cls, data: Any, source: str, capability: str,
                elapsed_ms: int = 0, stale: bool = False, **meta) -> DataResult:
        return cls(ok=True, data=data, source=source, capability=str(capability),
                   elapsed_ms=elapsed_ms, stale=stale, meta=meta)

    @classmethod
    def failure(cls, source: str, capability: str, error_code: str,
                error_msg: str, elapsed_ms: int = 0, **meta) -> DataResult:
        return cls(ok=False, source=source, capability=str(capability),
                   elapsed_ms=elapsed_ms, error_code=error_code,
                   error_msg=error_msg, meta=meta)

    @property
    def row_count(self) -> int:
        """结果行数（DataFrame/列表均可）"""
        if self.data is None:
            return 0
        try:
            return len(self.data)
        except TypeError:
            return 0

    def to_dict(self) -> dict:
        """元数据摘要（不含 data 本体，便于日志/接口输出）"""
        return {
            'ok': self.ok,
            'source': self.source,
            'capability': self.capability,
            'stale': self.stale,
            'elapsed_ms': self.elapsed_ms,
            'error_code': self.error_code,
            'error_msg': self.error_msg[:200],
            'rows': self.row_count,
            'meta': self.meta,
        }


class DataSourceError(Exception):
    """契约层异常基类（调用方一般不需要捕获——DataResult 已承载错误）"""


@dataclass
class SourceSpec:
    """数据源规格声明（registry 的元素）"""

    name: str
    module: str
    class_name: str
    capabilities: frozenset[Capability]
    priority: dict[str, int] = field(default_factory=dict)
    needs_key: bool = False
    key_env: str = ''
    volume_unit: str = UNIT_UNKNOWN
    adjust: str = ADJUST_NONE
    daily_update_time: str = ''
    notes: str = ''

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities

    def rank(self, capability: Capability) -> int:
        """该源在某能力上的优先级序号（越小越靠前；未声明则末尾）"""
        return self.priority.get(str(capability), 99)
