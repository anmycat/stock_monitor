"""I-01 ~ I-12 改进点专项回归（2026-11-07）

覆盖：整帧校验（I-04）、失败路径效率（I-05）、能力中心（I-02）、
血缘透出（I-06/I-09）、鉴权自检（I-01）、部分结果通知（I-11）、
provider 注入与缓存失效（I-05/I-04）。全部离线，不依赖 Redis / 网络。
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.api.auth import audit_auth_coverage
from quant_engine.data.capability import build_capability_report
from quant_engine.data.source_manager import (
    SourceManager,
    SourceStatus,
    classify_failure,
)
from quant_engine.models.report import ReportSection
from quant_engine.models.signal import Signal, SignalAction
from quant_engine.services.ai.ai_summary import AISummary
from quant_engine.services.ai.daily_report import DailyReportBuilder
from quant_engine.services.signals.signal_generator import (
    SignalGenerator,
    signal_to_api_dict,
)
from quant_engine.utils import kline_frame
from quant_engine.utils import kline_provenance as prov

SESSION = '2026-11-09'   # 周一
GOOD_DATES = ['2026-11-02', '2026-11-03', '2026-11-04',
              '2026-11-05', '2026-11-06']


def make_df(dates=None, *, close=10.0) -> pd.DataFrame:
    dates = list(dates or GOOD_DATES)
    n = len(dates)
    return pd.DataFrame({
        'date': dates,
        'open': [close] * n,
        'high': [close * 1.01] * n,
        'low': [close * 0.99] * n,
        'close': [close] * n,
        'volume': [1000.0] * n,
    })


def make_signal(**kw) -> Signal:
    base = {
        'code': '000001.SZ', 'name': '平安银行', 'action': SignalAction.BUY,
        'score': 80.0, 'position': 0.2, 'reason': ['测试'],
        'timestamp': datetime(2026, 11, 9, 10, 0),
        'strategy': 'test', 'price': 10.0,
    }
    base.update(kw)
    return Signal(**base)


class TestFrameValidation(unittest.TestCase):
    """I-04：整帧交易日/单调/价格校验，只写 attrs 不改列。"""

    def test_clean_frame_ok(self):
        df = make_df()
        out = kline_frame.validate_frame(df, SESSION)
        self.assertEqual(out['status'], 'ok', out['issues'])
        self.assertEqual(out['checked_bars'], len(GOOD_DATES))

    def test_weekend_bar_invalid(self):
        df = make_df(GOOD_DATES[:-1] + ['2026-11-07'])  # 周六
        out = kline_frame.validate_frame(df, SESSION)
        self.assertEqual(out['status'], 'invalid')
        self.assertTrue(any('non-trading-day' in i for i in out['issues']))

    def test_non_monotonic_invalid(self):
        df = make_df(GOOD_DATES[:3] + ['2026-11-04', '2026-11-06'])  # 重复 11-04
        out = kline_frame.validate_frame(df, SESSION)
        self.assertEqual(out['status'], 'invalid')
        self.assertTrue(any('non-monotonic' in i for i in out['issues']))

    def test_future_bar_invalid(self):
        df = make_df(GOOD_DATES + ['2026-11-10'])
        out = kline_frame.validate_frame(df, SESSION)
        self.assertTrue(any('future bar' in i for i in out['issues']))

    def test_invalid_close_invalid(self):
        df = make_df()
        df.loc[2, 'close'] = 0.0
        out = kline_frame.validate_frame(df, SESSION)
        self.assertEqual(out['status'], 'invalid')

    def test_annotate_does_not_touch_columns_or_values(self):
        df = make_df()
        before = df.copy(deep=True)
        kline_frame.annotate(df, SESSION)
        pd.testing.assert_frame_equal(df, before)
        self.assertEqual(df.attrs['kline_frame_status'], 'ok')
        self.assertEqual(prov.read(df)['frame_status'], 'ok')

    def test_issue_persist_and_clear(self):
        class FakeCache:
            def __init__(self):
                self.data = {}

            def set(self, k, v, ttl=0):
                self.data[k] = v
                return True

            def get(self, k):
                return self.data.get(k)

            def delete(self, k):
                return self.data.pop(k, None) is not None

            def keys(self, pattern):
                prefix = pattern.split('*')[0]
                return [k for k in self.data if k.startswith(prefix)]

        cache = FakeCache()
        df = make_df(GOOD_DATES[:-1] + ['2026-11-07'])
        kline_frame.annotate(df, SESSION, cache=cache, code='000001.SZ')
        self.assertEqual(len(kline_frame.list_issues(cache, SESSION)), 1)
        kline_frame.clear_issue(cache, SESSION, '000001.SZ')
        self.assertEqual(kline_frame.list_issues(cache, SESSION), [])

    def test_reject_layer_default_off(self):
        df = make_df(GOOD_DATES[:-1] + ['2026-11-07'])
        kline_frame.annotate(df, SESSION)
        with mock.patch('quant_engine.config.get_settings') as gs:
            gs.return_value = SimpleNamespace(KLINE_REJECT_STALE_FRAME=False)
            self.assertFalse(kline_frame.should_reject(df, SESSION))
        with mock.patch('quant_engine.config.get_settings') as gs:
            gs.return_value = SimpleNamespace(KLINE_REJECT_STALE_FRAME=True)
            self.assertTrue(kline_frame.should_reject(df, SESSION))

    def test_reject_layer_never_rejects_current_data(self):
        df = make_df(GOOD_DATES + [SESSION])  # 末根 == session
        kline_frame.annotate(df, SESSION)
        with mock.patch('quant_engine.config.get_settings') as gs:
            gs.return_value = SimpleNamespace(KLINE_REJECT_STALE_FRAME=True)
            self.assertFalse(kline_frame.should_reject(df, SESSION))


class TestSourceEfficiency(unittest.TestCase):
    """I-05：超时分类、非重试、短抑制、空结果结构化。"""

    def test_classify_failure(self):
        self.assertEqual(classify_failure(TimeoutError('timed out')), ('timeout', True))
        self.assertEqual(classify_failure(ConnectionResetError('reset')), ('timeout', True))
        self.assertEqual(classify_failure(ValueError('bad')), ('error', False))

    def test_timeout_not_retried_and_suppressed(self):
        mgr = SourceManager()
        mgr.register_source('flaky')
        calls = []

        def flaky():
            calls.append(1)
            raise TimeoutError('read timed out')

        with mock.patch('quant_engine.config.get_settings') as gs:
            gs.return_value = SimpleNamespace(
                SOURCE_TIMEOUT_SUPPRESSION=True,
                SOURCE_TIMEOUT_SUPPRESS_SECONDS=20.0,
                SOURCE_TIMEOUT_SUPPRESS_STREAK=2,
            )
            mgr.execute_with_retry('flaky', flaky, max_retries=3)
            self.assertEqual(len(calls), 1, '超时不得盲重试')
            mgr.execute_with_retry('flaky', flaky, max_retries=3)
            self.assertEqual(len(calls), 2)
            status = mgr.sources['flaky']
            self.assertGreater(status.suppress_until, 0)
            # 抑制窗口内直接跳过，不再打源
            mgr.execute_with_retry('flaky', flaky, max_retries=3)
            self.assertEqual(len(calls), 2)

    def test_empty_result_has_reason_and_missing_codes(self):
        s = SourceStatus('sina')
        s.record_empty(reason='no_data', missing_codes=['600519.SH'])
        self.assertEqual(s.last_empty_reason, 'no_data')
        self.assertEqual(s.missing_codes, ['600519.SH'])
        self.assertEqual(s.error_count, 0, '空结果不得计入错误')

    def test_state_persist_and_restore(self):
        class FakeCache:
            def __init__(self):
                self.data = {}

            def set(self, k, v, ttl=0):
                self.data[k] = v
                return True

            def get(self, k):
                return self.data.get(k)

        cache = FakeCache()
        m1 = SourceManager()
        m1.register_source('tdx')
        m1._last_persist = -100.0
        m1.attach_cache(cache)
        m1.sources['tdx'].error_count = 3
        m1._persist_state_throttled()
        m2 = SourceManager()
        m2.register_source('tdx')
        m2.attach_cache(cache)
        self.assertEqual(m2.sources['tdx'].error_count, 3)

    def test_fallback_recorded(self):
        mgr = SourceManager()
        mgr.register_source('a')
        mgr.register_source('b')
        sources = {'a': SimpleNamespace(get=lambda: pd.DataFrame()),
                   'b': SimpleNamespace(get=lambda: make_df())}
        out = mgr.execute_with_fallback(['a', 'b'], sources, 'get')
        self.assertFalse(out.empty)
        ev = mgr.last_fallback.get('get')
        self.assertEqual(ev['fallbackFrom'], ['a'])
        self.assertEqual(ev['to'], 'b')


class TestCapability(unittest.TestCase):
    """I-02：四段 schema + 降级/熔断告警。"""

    def test_schema_and_warnings(self):
        sm = SimpleNamespace(
            get_status_report=lambda: {
                'a': {'available': False, 'quarantined': True,
                      'quarantined_until': 0, 'suppressed': False,
                      'error_count': 3, 'last_error_message': 'boom'},
                'b': {'available': True, 'quarantined': False,
                      'suppressed': False, 'error_count': 0},
            },
            last_fallback={'daily': {'fallbackFrom': ['a'], 'to': 'b', 'count': 2}},
        )
        with mock.patch('quant_engine.data.capability.get_history_store',
                        create=True), \
             mock.patch('quant_engine.data.store.get_history_store') as store:
            store.return_value.list_sealed_dates.return_value = []
            rep = build_capability_report(
                cache=None, session=SESSION, source_manager=sm,
                priorities={'daily': ['a', 'b']})
        for k in ('as_of', 'providers', 'datasets', 'priorities', 'warnings'):
            self.assertIn(k, rep)
        self.assertEqual(len(rep['providers']), 2)
        self.assertEqual(rep['providers'][0]['status'], 'unavailable')
        self.assertEqual(rep['datasets'][0]['fallbackFrom'], ['a'])
        self.assertTrue(any('熔断' in w for w in rep['warnings']))


class TestProvenancePassthrough(unittest.TestCase):
    """I-06/I-09：只读字段透出，不参与评分。"""

    def test_evidence_shape(self):
        df = make_df()
        prov.tag(df, 'tencent')
        ev = prov.evidence(df, SESSION)
        self.assertEqual(ev['data_as_of'], '2026-11-06')
        self.assertEqual(ev['data_source'], 'tencent')
        self.assertEqual(ev['freshness'], 'lagging')  # 上周五 vs 本周一
        self.assertNotIn('score', ev)

    def test_fill_data_provenance(self):
        gen = SignalGenerator.__new__(SignalGenerator)
        df = make_df()
        prov.tag(df, 'sina')
        sig = make_signal()
        gen._fill_data_provenance(sig, df, SESSION)
        self.assertEqual(sig.data_as_of, '2026-11-06')
        self.assertEqual(sig.data_source, 'sina')
        self.assertEqual(sig.freshness, 'lagging')
        self.assertEqual(sig.score, 80.0)  # 评分未被改动

    def test_api_dict_has_readonly_fields(self):
        sig = make_signal(current_price=10.3, price_ref=10.0,
                          data_as_of='2026-11-06', data_source='sina',
                          freshness='lagging', cross_check='confirmed_lagging',
                          frame_status='ok')
        d = signal_to_api_dict(sig)
        for k in ('current_price', 'price_ref', 'data_as_of', 'data_source',
                  'freshness', 'cross_check', 'frame_status'):
            self.assertIn(k, d)
        self.assertEqual(d['current_price'], 10.3)

    def test_report_section_evidence_default_and_builder(self):
        self.assertEqual(ReportSection(index=1, title='x').evidence, [])
        sigs = [make_signal(data_as_of='2026-11-06', data_source='sina',
                            freshness='lagging', cross_check='confirmed_lagging')]
        ev = DailyReportBuilder._build_evidence(sigs)
        self.assertEqual(ev[0]['kind'], 'gate')
        self.assertEqual(ev[0]['source'], 'code')
        self.assertEqual(ev[1]['source'], 'sina')
        self.assertTrue(ev[1]['degraded'])


class TestDisciplineMetrics(unittest.TestCase):
    """I-08：客观指标由代码算、默认关闭、不进入门控综合分。"""

    def test_metrics_from_real_signals(self):
        from quant_engine.services.ai.discipline import (
            build_discipline_metrics,
            render_discipline_fields,
        )
        sigs = [
            make_signal(freshness='current', data_as_of='2026-11-09',
                        cross_check='confirmed_current',
                        current_price=10.5, price_ref=10.0),
            make_signal(freshness='lagging', data_as_of='2026-11-06',
                        frame_status='invalid'),
        ]
        m = build_discipline_metrics(sigs, sealed_dates=['2026-11-06'])
        self.assertEqual(m['signal_count'], 2)
        self.assertEqual(m['buy_count'], 2)
        self.assertEqual(m['fresh_current_count'], 1)
        self.assertEqual(m['fresh_current_ratio'], 0.5)
        self.assertEqual(m['lagging_count'], 1)
        self.assertEqual(m['cross_checked_count'], 1)
        self.assertEqual(m['gap_warned_count'], 1)
        self.assertEqual(m['frame_invalid_count'], 1)
        self.assertEqual(m['computed_by'], 'code')
        self.assertEqual(len(render_discipline_fields(m)), 7)

    def test_default_switch_off(self):
        from quant_engine.config import Settings
        self.assertFalse(Settings().DAILY_REPORT_DISCIPLINE_DIMENSION)


class TestAuthPartialAndEfficiencyExtras(unittest.TestCase):
    """I-01 / I-11 / I-05 / I-04 辅助 assertions。"""

    def test_auth_coverage_detects_missing(self):
        dep = SimpleNamespace(dependencies=[
            SimpleNamespace(call=__import__(
                'quant_engine.api.auth', fromlist=['verify_api_key']).verify_api_key)])
        app = SimpleNamespace(routes=[
            SimpleNamespace(path='/api/health', dependant=SimpleNamespace(dependencies=[])),
            SimpleNamespace(path='/api/ok', dependant=dep),
            SimpleNamespace(path='/api/missing',
                            dependant=SimpleNamespace(dependencies=[])),
        ])
        cov = audit_auth_coverage(app)
        self.assertEqual(cov['checked'], 2)
        self.assertEqual(cov['missing'], ['/api/missing'])
        self.assertIn('/api/health', cov['exempt'])

    def test_local_summary_marked_partial(self):
        from quant_engine.models.market import MarketType
        ai = AISummary.__new__(AISummary)
        # _generate_local 只消费 type/score/breadth，用轻量替身避免构造完整 MarketState
        market = SimpleNamespace(type=MarketType.SIDEWAYS, score=50.0, breadth=0.5)
        with mock.patch('quant_engine.config.get_settings') as gs:
            gs.return_value = SimpleNamespace(AI_PARTIAL_NOTIFY=True)
            out = ai._generate_local(market, [])
        self.assertTrue(out['partial'])
        self.assertTrue(out['degraded'])
        self.assertEqual(out['missing_sections'], ['ai_narrative'])
        self.assertIn('部分结果', out['full_text'])

    def test_capital_flow_provider_injection(self):
        gen = SignalGenerator.__new__(SignalGenerator)

        class Provider:
            @staticmethod
            def get_capital_flow(code, days=5):
                return 12345.0

        gen.set_capital_flow_provider(Provider)
        self.assertEqual(gen._capital_flow_provider.get_capital_flow('x', 5), 12345.0)

    def test_invalidate_daily_cache(self):
        from quant_engine.data.repositories.market_data import MarketDataRepository

        class FakeCache:
            def __init__(self):
                self.data = {'daily:2026-11-09:000001.SZ:120': 1,
                             'daily:2026-11-06:000001.SZ:120': 1,
                             'daily:2026-11-09:600519.SH:120': 1}

            def keys(self, pattern):
                prefix = pattern.split('*')[0]
                return [k for k in self.data if k.startswith(prefix)]

            def delete(self, k):
                return self.data.pop(k, None) is not None

        repo = MarketDataRepository.__new__(MarketDataRepository)
        repo.cache = FakeCache()
        n = repo.invalidate_daily_cache('000001.SZ')
        self.assertEqual(n, 2)
        self.assertEqual(list(repo.cache.data), ['daily:2026-11-09:600519.SH:120'])


if __name__ == '__main__':
    unittest.main()
