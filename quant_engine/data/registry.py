"""数据源能力注册表（D53，2026-11-01）

一张表列清每个源的：能力集合 / 各能力优先级 / 是否需要 Key / 成交量单位口径
/ 复权口径 / 已知限制。加源 = registry 一行 + 一个新源文件；字段对齐 =
COLUMN_MAPPINGS 一行 + 契约测试。

调研来源：research/docs/17（免费数据源降级备路调研）与 docs/30
（TradingAgents-CN 与 MCP 风格接口层）。
"""
from __future__ import annotations

from .spec import (
    ADJUST_FORWARD,
    ADJUST_NONE,
    UNIT_LOTS,
    UNIT_SHARES,
    UNIT_UNKNOWN,
    Capability,
    SourceSpec,
)

#: 源能力声明表
SOURCES: dict[str, SourceSpec] = {
    'tencent': SourceSpec(
        name='tencent',
        module='quant_engine.data.sources.tencent',
        class_name='TencentSource',
        capabilities=frozenset({
            Capability.DAILY, Capability.REALTIME, Capability.LIST,
        }),
        priority={'daily': 0, 'realtime': 0, 'list': 2},
        needs_key=False,
        volume_unit=UNIT_SHARES,   # ⚠️ 688/689=股，其余板=手（分裂，见 align）
        adjust=ADJUST_FORWARD,      # fqkline qfq 前复权
        notes='主路；代码池动态化（新浪 hs_a 列表主路/东财 clist 备路）；'
               '腾讯无复权日线时返回 HTTP 501',
    ),
    'sina': SourceSpec(
        name='sina',
        module='quant_engine.data.sources.sina',
        class_name='SinaSource',
        capabilities=frozenset({
            Capability.DAILY, Capability.REALTIME, Capability.LIST,
            Capability.MINUTE,
        }),
        priority={'daily': 1, 'realtime': 1, 'list': 0, 'minute': 0},
        needs_key=False,
        volume_unit=UNIT_SHARES,
        adjust=ADJUST_NONE,
        notes='股票池主路（hs_a 实测 5574 只含北交所）；'
               '新浪 qfq.js 提供原始复权因子（零鉴权）；'
               'D53：新增分钟 K（getKLineData scale=5/15/30/60），'
               '接替已停供行情的 TDX 分钟线通道',
    ),
    'ths_web': SourceSpec(
        name='ths_web',
        module='quant_engine.data.sources.ths_web',
        class_name='ThsWebSource',
        capabilities=frozenset({Capability.DAILY, Capability.REALTIME}),
        priority={'daily': 2, 'realtime': 3},
        needs_key=False,
        volume_unit=UNIT_SHARES,
        adjust=ADJUST_NONE,
        notes='D53 新增：同花顺网页端免登录备源；volume 直接是「股」，'
               '与腾讯逐字段对齐（600519 2026-10-30 交叉校验通过）；'
               '单标的单请求 → 不接 realtime 全市场链路',
    ),
    'eastmoney': SourceSpec(
        name='eastmoney',
        module='quant_engine.data.sources.eastmoney',
        class_name='EastMoneySource',
        capabilities=frozenset({
            Capability.DAILY, Capability.REALTIME, Capability.LIST,
        }),
        priority={'daily': 3, 'realtime': 2, 'list': 1},
        needs_key=False,
        volume_unit=UNIT_LOTS,      # 东财 f5 为手
        adjust=ADJUST_NONE,
        notes='同源 IP 风控高发区（生产 capital_flow 仅 3/33 非空）；'
               '已实施三招：限速≥1s + 随机抖动 + 连接复用',
    ),
    'em_pool': SourceSpec(
        name='em_pool',
        module='quant_engine.data.sources.em_pool',
        class_name='EastMoneyPoolSource',
        capabilities=frozenset({Capability.POOL}),
        priority={'pool': 0},
        needs_key=False,
        volume_unit=UNIT_UNKNOWN,
        adjust=ADJUST_NONE,
        notes='D53 新增：push2ex 同源换 host 救回涨停/炸板/跌停池'
               '（push2/push2his 被风控时仍可用）；连板数/封板资金/炸板次数',
    ),
    'tdx': SourceSpec(
        name='tdx',
        module='quant_engine.data.sources.tdx',
        class_name='TdxSource',
        capabilities=frozenset({Capability.MINUTE}),
        priority={'minute': 0},
        needs_key=False,
        volume_unit=UNIT_LOTS,
        adjust=ADJUST_NONE,
        notes='pytdx TCP 7709；生产 ECS→117.34.114.x 不可达（自动降级）；'
               'mootdx bestip 探测可解决（待接入）',
    ),
    'akshare': SourceSpec(
        name='akshare',
        module='quant_engine.data.sources.akshare_source',
        class_name='AkshareSource',
        capabilities=frozenset({Capability.POOL, Capability.LIST}),
        priority={'pool': 2, 'list': 4},
        needs_key=False,
        volume_unit=UNIT_UNKNOWN,
        adjust=ADJUST_NONE,
        notes='D53：可选依赖（生产 ECS 已装 1.18.64）；按调研纪律**不进盘中主循环**，'
               '只做夜间批量与补充能力（涨停/跌停池、指数成分、行业分类、交易日历）；'
               '底层多为爬虫接口（_em 与现有源同源、_sina/_ths 封 IP 风险高）',
    ),
    'cninfo': SourceSpec(
        name='cninfo',
        module='quant_engine.data.sources.cninfo_source',
        class_name='CninfoSource',
        capabilities=frozenset({Capability.FUNDAMENTAL}),
        priority={'fundamental': 0},
        needs_key=False,
        volume_unit=UNIT_UNKNOWN,
        adjust=ADJUST_NONE,
        notes='D53：巨潮公告/事件源（非行情）；stock 参数必须 "代码,orgId"'
               '（只传代码返回 0 条）；限速 1.6s + 抖动 + 连接复用；用于 ST/退市/风险警示扫描',
    ),
    'iwencai': SourceSpec(
        name='iwencai',
        module='quant_engine.data.sources.iwencai_source',
        class_name='IwencaiSource',
        capabilities=frozenset({Capability.LIST}),
        priority={'list': 5},
        needs_key=True,
        key_env='HEXIN_V',
        volume_unit=UNIT_UNKNOWN,
        adjust=ADJUST_NONE,
        notes='D53：问财自然语言选股；需 HEXIN_V + IWENCAI_COOKIE 两个凭据'
               '（pywencai 的 hexin-v.js 生成 + 浏览器 Cookie）；'
               '定位盘后一次性选股，限速 2s，不进盘中主循环',
    ),
    'tushare': SourceSpec(
        name='tushare',
        module='quant_engine.data.sources.tushare',
        class_name='TushareSource',
        capabilities=frozenset({
            Capability.DAILY, Capability.REALTIME, Capability.LIST,
        }),
        priority={'daily': 4, 'realtime': 4, 'list': 3},
        needs_key=True,
        key_env='TUSHARE_TOKEN',
        volume_unit=UNIT_SHARES,
        adjust=ADJUST_NONE,
        notes='积分制兜底线；未装包/未配 token 时各方法恒返 None',
    ),
}

#: 各源各能力的列映射（源列名 → 标准列名）
COLUMN_MAPPINGS: dict[str, dict[Capability, dict[str, str]]] = {
    'tencent': {
        Capability.DAILY: {
            'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume',
        },
    },
    'sina': {
        Capability.DAILY: {
            'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume',
        },
    },
    'ths_web': {
        Capability.DAILY: {
            'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume', 'amount': 'amount',
            'turnover_rate': 'turnover_rate',
        },
    },
    'em_pool': {
        Capability.POOL: {
            'code': 'code', 'name': 'name', 'price': 'price',
            'change_pct': 'change_pct', 'amount': 'amount',
            'limit_up_times': 'limit_up_times', 'seal_fund': 'seal_fund',
            'broken_times': 'broken_times', 'industry': 'industry',
        },
    },
}


def sources_for(capability: Capability) -> list[SourceSpec]:
    """按优先级返回支持某能力的源列表"""
    return sorted(
        (s for s in SOURCES.values() if s.supports(capability)),
        key=lambda s: (s.rank(capability), s.name),
    )


def get_spec(name: str) -> SourceSpec | None:
    """取源规格声明"""
    return SOURCES.get(name)


def describe() -> list[dict]:
    """注册表摘要（便于 /api 或巡检脚本输出）"""
    return [
        {
            'name': s.name,
            'capabilities': sorted(str(c) for c in s.capabilities),
            'needs_key': s.needs_key,
            'volume_unit': s.volume_unit,
            'adjust': s.adjust,
            'notes': s.notes,
        }
        for s in SOURCES.values()
    ]
