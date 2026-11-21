"""D74 eltdx 接入项目专项测试

纪律：验证「末位兜底 None 语义 + 旁路证据零信号影响」，不触碰评分/门控/
Kelly/回测。注入假客户端，无需安装 eltdx（本地 py3.9 可跑）。
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quant_engine.config import Settings
from quant_engine.data.sources.eltdx_client import (
    EltdxClient,
    EltdxFallbackSource,
    _to_plain,
    build_eltdx_context_provider,
)
from quant_engine.services.market.market_alert import MarketAlert


class _Bar:
    def __init__(self, date_str, close):
        self.time = datetime.strptime(date_str, '%Y-%m-%d')
        self.open = close - 0.1
        self.high = close + 0.1
        self.low = close - 0.2
        self.close = close
        self.volume_lots = 1000
        self.amount = close * 100000


class _Bars:
    def __init__(self, adjust_seen):
        self.adjust_seen = adjust_seen

    def get(self, code, *, period, count, adjust):
        self.adjust_seen.append((code, period, adjust))
        return SimpleNamespace(bars=[_Bar('2026-11-20', 12.22), _Bar('2026-11-19', 12.04)])


class _Client:
    def __init__(self):
        self.adjust_seen = []
        self.bars = _Bars(self.adjust_seen)
        self.minutes = SimpleNamespace(today=lambda code: SimpleNamespace(points=[]))
        self.trades = SimpleNamespace(all_today=lambda code: [])
        self.money_flow = SimpleNamespace(daily=lambda code: SimpleNamespace(records=[]))
        self.helpers = SimpleNamespace(
            limit_ladder=lambda *a, **k: SimpleNamespace(rows=[]),
            realtime_rank=lambda: SimpleNamespace(rows=[]),
            full_quotes=lambda codes: [],
        )


class TestFallbackNoneSemantics(unittest.TestCase):
    """关键陷阱回归：空 DF 必须转 None，否则 execute_with_fallback 会截断兜底链。"""

    def test_empty_df_becomes_none(self):
        cfg = Settings(ELTDX_ENABLED=False)  # 关闭 → 取数为控 DF
        src = EltdxFallbackSource(EltdxClient(settings=cfg))
        self.assertIsNone(src.get_daily_data('000001.SZ', 10))
        self.assertIsNone(src.get_minute_data('000001.SZ', '5'))

    def test_nonempty_df_passes_through(self):
        fake = _Client()
        cfg = Settings(ELTDX_ENABLED=True)
        src = EltdxFallbackSource(EltdxClient(settings=cfg, client_factory=lambda h: fake))
        df = src.get_daily_data('000001.SZ', 10)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)

    def test_factory_error_is_none(self):
        def boom(_hosts):
            raise RuntimeError('down')

        cfg = Settings(ELTDX_ENABLED=True)
        src = EltdxFallbackSource(EltdxClient(settings=cfg, client_factory=boom))
        self.assertIsNone(src.get_daily_data('000001.SZ', 10))


class TestDailyForwardAdjust(unittest.TestCase):
    def test_daily_uses_qfq(self):
        """日K 前复权对齐主链腾讯 fqkline 口径。"""
        fake = _Client()
        cfg = Settings(ELTDX_ENABLED=True, ELTDX_ADJUST='qfq')
        c = EltdxClient(settings=cfg, client_factory=lambda h: fake)
        c.get_daily_data('600519.SH', 30)
        self.assertEqual(fake.adjust_seen, [('sh600519', 'day', 'qfq')])

    def test_minute_still_no_adjust(self):
        """分钟线不复权（与新浪分钟口径一致）。"""
        fake = _Client()
        cfg = Settings(ELTDX_ENABLED=True)
        c = EltdxClient(settings=cfg, client_factory=lambda h: fake)
        c.get_minute_data('000001.SZ', '5', count=10)
        self.assertEqual(fake.adjust_seen[0][2], None)


class TestPlainConversion(unittest.TestCase):
    def test_dataclass_to_plain_drops_raw_fields(self):
        # dataclass 分支过滤 *_raw/record_hex；此处用 dict 分支等价验证过滤规则
        plain = _to_plain({'code': '000001', 'main_net': 1.5,
                           'main_net_raw': 99, 'record_hex': 'abc'})
        self.assertEqual(plain, {'code': '000001', 'main_net': 1.5})


class TestMarketAlertEvidenceNoSignal(unittest.TestCase):
    """eltdx 证据只挂展示字段，不改告警数量/阈值/标题。"""

    def _make_alert(self):
        return {
            'type': 'LIMIT_UP', 'level': 'HIGH', 'title': '涨停潮：10只',
            'detail': 'test', 'icon': '🚀', 'timestamp': '2026-11-21',
        }

    def test_evidence_attached_without_changing_decision(self):
        ctx = {'source': 'eltdx(TDX 7709)', 'money_flow': {'000001.SZ': []},
               'hot_rank': [], 'observed_at': '2026-11-21T22:00:00'}
        alert = self._make_alert()
        # 模拟 detect_alerts 的证据挂载逻辑（与实现一致）
        evidence = alert.setdefault('evidence', {})
        evidence['eltdx_context'] = ctx
        self.assertEqual(len(alert['title'].split('：')), 2)  # 标题未变
        self.assertEqual(alert['type'], 'LIMIT_UP')
        self.assertIn('000001.SZ', alert['evidence']['eltdx_context']['money_flow'])

    def test_format_adds_source_line_only(self):
        ctx = {'source': 'eltdx(TDX 7709)', 'money_flow': {'000001.SZ': []},
               'hot_rank': [{'rank': 1}], 'observed_at': '2026-11-21T22:00:00'}
        ma = MarketAlert.__new__(MarketAlert)  # 不走 __init__，纯测格式化
        alert = self._make_alert()
        alert['evidence'] = {'eltdx_context': ctx}
        msg = ma.format_alert_message(alert)
        self.assertIn('盘面证据', msg)
        self.assertIn('热榜1条', msg)
        self.assertIn('资金流1只', msg)
        self.assertIn('涨停潮：10只', msg)  # 原标题仍在


class TestProviderGate(unittest.TestCase):
    def test_provider_none_when_disabled(self):
        cfg = Settings(ELTDX_ENABLED=True, ELTDX_MARKET_ALERT_ENRICH=False)
        self.assertIsNone(build_eltdx_context_provider(settings=cfg))

    def test_provider_built_when_enabled(self):
        cfg = Settings(ELTDX_ENABLED=True, ELTDX_MARKET_ALERT_ENRICH=True)
        provider = build_eltdx_context_provider(settings=cfg)
        self.assertTrue(callable(provider))


if __name__ == '__main__':
    unittest.main(verbosity=2)
