"""D71 / N-10 同花顺官方数据源 market_alert 旁路增强测试

纪律：只验证「证据注入 + 降级 + 默认关闭」，不触碰任何信号/评分/回测口径。
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quant_engine.data.sources.ths_financial import (
    ThsFinancialSource,
    build_ths_context_provider,
    to_thscode,
)
from quant_engine.models.market import MarketState, MarketType
from quant_engine.services.market.market_alert import MarketAlert


class _Resp:
    def __init__(self, payload=None, status=200):
        self.status_code = status
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _Session:
    """按 URL 尾段返回固定响应，并记录请求。"""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params, headers))
        key = next((k for k in self.routes if url.endswith(k)), None)
        return self.routes.get(key, _Resp({'code': 3002, 'message': 'no fixture'}))


class TestThsFinancialSource(unittest.TestCase):
    def test_thscode_conversion(self):
        self.assertEqual(to_thscode('600519'), '600519.SH')
        self.assertEqual(to_thscode('000001.SZ'), '000001.SZ')
        self.assertEqual(to_thscode('430001'), '430001.BJ')
        self.assertEqual(to_thscode('300750'), '300750.SZ')
        self.assertIsNone(to_thscode('abc'))

    def test_no_key_makes_zero_requests(self):
        session = _Session({})
        src = ThsFinancialSource(api_key='', session=session)
        self.assertIsNone(src.limit_up_statistics())
        self.assertEqual(session.calls, [])

    def test_envelope_error_and_http_error_degrade(self):
        session = _Session({
            '/limit-up-statistics': _Resp({'code': 2001, 'message': 'bad key'}),
            '/limit-break-pool': _Resp({'code': 0, 'data': {}}, status=500),
        })
        src = ThsFinancialSource(api_key='k', session=session)
        self.assertIsNone(src.limit_up_statistics())
        self.assertIsNone(src.limit_break_pool_size())

    def test_network_exception_degrades(self):
        session = _Session({'/limit-up-statistics': _Resp(ValueError('bad json'))})
        src = ThsFinancialSource(api_key='k', session=session)
        self.assertIsNone(src.market_alert_context())

    def test_context_fields_and_request_contract(self):
        session = _Session({
            '/limit-up-statistics': _Resp({'code': 0, 'data': {
                'timestamp': 1791957420015, 'trade_date': '2026-11-21',
                'seal_rate': 0.64, 'open_stock_count': 25,
                'limit_up_stock_count': 46, 'touched_limit_up_stock_count': 71,
                'exact_boards': [{'board_num': 6, 'stock_count': 2},
                                 {'board_num': 3, 'stock_count': 1}],
            }}),
            '/limit-break-pool': _Resp({'code': 0, 'data': {
                'pagination': {'total': 25}}}),
            '/anomaly-analysis-list': _Resp({'code': 0, 'data': {'item': [
                {'stock_name': '贵州茅台', 'tag_name': '大涨',
                 'keyword_list': ['白酒'], 'analysis_content': '原因'},
            ]}}),
        })
        src = ThsFinancialSource(api_key='secret-not-logged', session=session)
        ctx = src.market_alert_context()
        self.assertEqual(ctx['source'], 'ths_financial_api')
        self.assertEqual(ctx['seal_rate'], 0.64)
        self.assertEqual(ctx['break_stock_count'], 25)
        self.assertEqual(ctx['top_boards'][0]['board_num'], 6)
        self.assertEqual(ctx['anomaly_reasons'][0]['tag_name'], '大涨')
        self.assertEqual(len(session.calls), 3)
        for _, params, headers in session.calls:
            self.assertEqual(headers['X-api-key'], 'secret-not-logged')
        self.assertEqual(session.calls[1][1], {'page': 1, 'size': 1})

    def test_factory_default_off(self):
        cfg = SimpleNamespace(THS_MARKET_ALERT_ENRICH=False,
                              THS_FINANCIAL_API_KEY='k',
                              THS_FINANCIAL_BASE_URL='https://fuyao.aicubes.cn',
                              THS_FINANCIAL_TIMEOUT=10.0)
        self.assertIsNone(build_ths_context_provider(cfg))
        cfg.THS_MARKET_ALERT_ENRICH = True
        cfg.THS_FINANCIAL_API_KEY = ''
        self.assertIsNone(build_ths_context_provider(cfg))
        cfg.THS_FINANCIAL_API_KEY = 'k'
        self.assertTrue(callable(build_ths_context_provider(cfg)))


class _Cache:
    def get(self, key):
        return None

    def set(self, key, value, ttl=None):
        return None


class _DC:
    cache = _Cache()

    def __init__(self, quotes):
        self.quotes = quotes

    def get_stock_list(self):
        return pd.DataFrame({'code': ['600519']})

    def get_realtime_quote(self, codes):
        return self.quotes.copy()


_CTX = {
    'source': 'ths_financial_api', 'trade_date': '2026-11-21',
    'as_of': 1791957420015, 'seal_rate': 0.42, 'open_stock_count': 31,
    'limit_up_stock_count': 20, 'touched_limit_up_stock_count': 51,
    'top_boards': [], 'break_stock_count': 31, 'anomaly_reasons': [],
}


def _market():
    return MarketState(type=MarketType.SIDEWAYS, score=50, trend=0,
                       volume_ratio=1, breadth=0.6, timestamp=datetime(2026, 11, 21))


class TestMarketAlertEnrichment(unittest.TestCase):
    def test_evidence_only_attached_when_provider_present(self):
        quotes = pd.DataFrame({
            'code': ['600519', '600036', '601318', '600030', '600887'],
            'name': ['贵州茅台', '招商银行', '中国平安', '中信证券', '伊利股份'],
            'price': [1600.0, 40.0, 50.0, 25.0, 30.0],
            'change_pct': [10.0] * 5,
            'volume': [1000] * 5,
        })
        alert_obj = MarketAlert(_DC(quotes))
        plain = alert_obj._check_limit_alerts(_market())
        self.assertTrue(plain)
        self.assertNotIn('evidence', plain[0])
        enriched = alert_obj._check_limit_alerts(_market(), _CTX)
        self.assertEqual(enriched[0]['title'], plain[0]['title'])
        self.assertEqual(enriched[0]['detail'], plain[0]['detail'])
        self.assertEqual(enriched[0]['evidence']['seal_rate'], 0.42)

    def test_default_off_no_new_alert(self):
        alert_obj = MarketAlert(_DC(pd.DataFrame()), ths_context_provider=lambda: _CTX)
        for name in ('_check_limit_alerts', '_check_volume_alerts',
                     '_check_index_alerts', '_check_breadth_alerts',
                     '_check_sector_alerts'):
            setattr(alert_obj, name, lambda *a, **k: [])
        cfg = SimpleNamespace(THS_MARKET_ALERT_ENRICH=False,
                              THS_SEAL_RATE_WEAK=0.6, THS_LIMIT_BREAK_MIN=20)
        with patch('quant_engine.config.get_settings', return_value=cfg):
            self.assertEqual(alert_obj.detect_alerts(_market()), [])

    def test_seal_weak_alert_and_cooldown(self):
        alert_obj = MarketAlert(_DC(pd.DataFrame()), ths_context_provider=lambda: _CTX)
        for name in ('_check_limit_alerts', '_check_volume_alerts',
                     '_check_index_alerts', '_check_breadth_alerts',
                     '_check_sector_alerts'):
            setattr(alert_obj, name, lambda *a, **k: [])
        cfg = SimpleNamespace(THS_MARKET_ALERT_ENRICH=True,
                              THS_SEAL_RATE_WEAK=0.6, THS_LIMIT_BREAK_MIN=20)
        with patch('quant_engine.config.get_settings', return_value=cfg):
            first = alert_obj.detect_alerts(_market())
            second = alert_obj.detect_alerts(_market())
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]['type'], 'SEAL_RATE_WEAK')
        self.assertEqual(second, [])  # 300s 冷却沿用既有机制

    def test_format_message_unchanged_without_evidence(self):
        alert_obj = MarketAlert(_DC(pd.DataFrame()))
        alert = {'icon': 'x', 'title': 't', 'detail': 'd'}
        self.assertEqual(alert_obj.format_alert_message(alert), 'x **t**\n\nd')
        alert['evidence'] = _CTX
        msg = alert_obj.format_alert_message(alert)
        self.assertIn('同花顺官方', msg)
        self.assertIn('封板率42%', msg)


if __name__ == '__main__':
    unittest.main(verbosity=2)
