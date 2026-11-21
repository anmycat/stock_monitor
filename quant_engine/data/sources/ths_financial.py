"""同花顺官方金融数据服务适配层（D71 / N-10，2026-11-21）

来源：`Hithink-Tech-Org/Financial-API`（同花顺官方，MIT 仓库），
REST `https://fuyao.aicubes.cn`，API Key 头 `X-api-key`。

**能力边界（官方文档明确）**：
- 可用：涨跌停/炸板/连板池、涨停统计（封板率/打开家数/晋级率）、个股异动原因、
  热榜、龙虎榜、交易日历、日K（日/周/月）、复权事件、财务、指数板块等。
- 不提供：分钟K、tick、Level-2、港美股行情、宏观、新闻/研报原文。
- 因此**不替换**本项目任何行情源；只给 `market_alert` 补「封板率/炸板/异动原因」
  这类原本算不出来的盘面证据。

**零信号影响**：本模块不返回 DataFrame、不改 `df.attrs`、不参与评分/门控/风控/
Kelly/回测；输出只作为告警的证据字段。开关 `THS_MARKET_ALERT_ENRICH` 默认关闭，
无 Key 时工厂直接返回 `None`（零请求、零行为变化）。

**失败语义**：统一 `ApiResponse` 信封（HTTP 恒 200，业务错误在 `code`），
任何异常/鉴权失败/限流都返回 `None` 让上层静默降级，绝不抛到主循环。
"""
from __future__ import annotations

import threading
import time

import requests
from loguru import logger

from ...config import Settings, get_settings

_MIN_INTERVAL = 0.5  # 官方未给固定 QPS；串行 + 半秒间隔，避免短时集中请求


def to_thscode(code: str) -> str | None:
    """项目内码（600519 / 600519.SH）→ 同花顺码（600519.SH）。"""
    sym = str(code or '').strip().upper()
    if '.' in sym:
        head, suffix = sym.split('.', 1)
        if suffix in {'SH', 'SZ', 'BJ'} and head.isdigit() and len(head) == 6:
            return f'{head}.{suffix}'
        sym = head
    if not sym.isdigit() or len(sym) != 6:
        return None
    if sym[0] == '6':
        return f'{sym}.SH'
    if sym[0] in {'4', '8'} or sym.startswith('92'):
        return f'{sym}.BJ'
    return f'{sym}.SZ'


class ThsFinancialSource:
    """同花顺官方 REST 的只读薄客户端（仅 market_alert 需要的端点）。"""

    def __init__(self, api_key: str | None = None, base_url: str | None = None,
                 timeout: float | None = None, session: requests.Session | None = None):
        settings = get_settings()
        self.api_key = api_key if api_key is not None else settings.THS_FINANCIAL_API_KEY
        self.base_url = (base_url or settings.THS_FINANCIAL_BASE_URL).rstrip('/')
        self.timeout = timeout if timeout is not None else settings.THS_FINANCIAL_TIMEOUT
        self._session = session or requests.Session()
        self._lock = threading.Lock()
        self._last = 0.0

    def _get(self, path: str, params: dict | None = None) -> dict | None:
        """GET + 统一信封校验；失败一律 None（不抛异常、不造模拟数据）。"""
        if not self.api_key:
            return None
        with self._lock:
            wait = _MIN_INTERVAL - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
        try:
            resp = self._session.get(
                f'{self.base_url}{path}',
                params=params,
                headers={'X-api-key': self.api_key, 'Accept': 'application/json'},
                timeout=self.timeout,
            )
            if resp.status_code != 200:
                logger.warning(f'[THS-FIN] HTTP {resp.status_code}: {path}')
                return None
            payload = resp.json()
            if not isinstance(payload, dict) or payload.get('code') != 0:
                code = payload.get('code') if isinstance(payload, dict) else '?'
                msg = payload.get('message') if isinstance(payload, dict) else '?'
                logger.warning(f'[THS-FIN] code={code} message={msg}: {path}')
                return None
            data = payload.get('data')
            return data if isinstance(data, dict) else None
        except Exception as e:
            logger.warning(f'[THS-FIN] request failed {path}: {type(e).__name__}: {e}')
            return None

    def limit_up_statistics(self) -> dict | None:
        """封板率/涨停打开家数/连板档位；省略 date 由服务端取当日或最近交易日。"""
        return self._get('/api/a-share/special-data/limit-up-statistics')

    def limit_break_pool_size(self) -> int | None:
        """炸板家数（只取 1 条，读 pagination.total，不拉全量）。"""
        data = self._get('/api/a-share/special-data/limit-break-pool',
                         {'page': 1, 'size': 1})
        if not data:
            return None
        pagination = data.get('pagination')
        total = pagination.get('total') if isinstance(pagination, dict) else None
        return int(total) if isinstance(total, int) else None

    def anomaly_reasons(self, limit: int = 5) -> list[dict]:
        """当日涨停/大涨异动原因（只取前 N 条做告警佐证）。"""
        data = self._get('/api/a-share/special-data/anomaly-analysis-list',
                         {'tag_codes': 'LIMIT_UP,SHARP_RISE'})
        if not data:
            return []
        items = data.get('item')
        if not isinstance(items, list):
            return []
        return [
            {
                'stock_name': it.get('stock_name'),
                'tag_name': it.get('tag_name'),
                'keywords': it.get('keyword_list') or [],
                'content': it.get('analysis_content'),
            }
            for it in items[:limit] if isinstance(it, dict)
        ]

    def market_alert_context(self) -> dict | None:
        """给 MarketAlert 的只读证据包；统计接口失败即整体降级。

        字段全部带 `source/as_of` 语义，便于日后解释「谁的数据、什么时刻」。
        """
        stats = self.limit_up_statistics()
        if not stats:
            return None
        exact = stats.get('exact_boards')
        top_boards = [
            {'board_num': b.get('board_num'), 'stock_count': b.get('stock_count')}
            for b in exact[:3] if isinstance(b, dict)
        ] if isinstance(exact, list) else []
        return {
            'source': 'ths_financial_api',
            'as_of': stats.get('timestamp'),
            'trade_date': stats.get('trade_date'),
            'seal_rate': stats.get('seal_rate'),
            'open_stock_count': stats.get('open_stock_count'),
            'limit_up_stock_count': stats.get('limit_up_stock_count'),
            'touched_limit_up_stock_count': stats.get('touched_limit_up_stock_count'),
            'top_boards': top_boards,
            'break_stock_count': self.limit_break_pool_size(),
            'anomaly_reasons': self.anomaly_reasons(),
        }


def build_ths_context_provider(settings: Settings | None = None):
    """按开关与 Key 构造 provider；默认关闭/无 Key 返回 None（零请求）。"""
    cfg = settings or get_settings()
    if not cfg.THS_MARKET_ALERT_ENRICH or not cfg.THS_FINANCIAL_API_KEY:
        return None
    source = ThsFinancialSource(
        api_key=cfg.THS_FINANCIAL_API_KEY,
        base_url=cfg.THS_FINANCIAL_BASE_URL,
        timeout=cfg.THS_FINANCIAL_TIMEOUT,
    )
    return source.market_alert_context
