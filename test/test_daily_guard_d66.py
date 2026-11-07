"""D66 日K 数据防误用测试（2026-11-06）

覆盖四条治理路径：
1. 血缘标签只写 attrs，不动任何列（趋势/指标零影响）
2. 跨源针对性复核：lagging 才触发、结论正确、不猜数
3. 收盘封存：打包 + 标签 + 次日锚点校验
4. 日切护栏：缓存 key 带交易日、隔夜 stale 快照作废

全部用替身，不打网络、不依赖本机 data/stock_info.json 新鲜度。
"""
import sys
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data import daily_guard
from quant_engine.data.source_manager import SourceManager
from quant_engine.utils import kline_provenance as prov

SESSION = '2026-11-06'      # 周五，交易日
PREV = '2026-11-05'


def _df(dates, closes=None):
    n = len(dates)
    closes = closes or [10.0 + i for i in range(n)]
    return pd.DataFrame({
        'date': dates,
        'open': [c - 0.1 for c in closes],
        'high': [c + 0.2 for c in closes],
        'low': [c - 0.2 for c in closes],
        'close': closes,
        'volume': [1000.0 * (i + 1) for i in range(n)],
    })


class FakeCache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl=0):
        self.data[key] = value
        return True


class FakeStore:
    def __init__(self):
        self.rows = []

    def save_daily_seals(self, rows):
        self.rows.extend(rows)
        return len(rows)

    def get_daily_seal(self, trade_date, code):
        for r in self.rows:
            if r['trade_date'] == trade_date and r['code'] == code:
                return dict(r)
        return None

    def list_sealed_dates(self, limit=10):
        return []


class TestProvenance(unittest.TestCase):
    """① 标签与新鲜度判定"""

    def test_last_bar_date_normalizes_formats(self):
        for raw, expect in [('2026-11-06', SESSION),
                            ('2026-11-06 15:00:00', SESSION),
                            ('20261106', SESSION)]:
            self.assertEqual(prov.last_bar_date(_df([raw])), expect)

    def test_tag_does_not_change_columns_or_values(self):
        df = _df([PREV, SESSION])
        before = df.copy()
        prov.tag(df, 'tencent', sealed=True, verified_by='em')
        pd.testing.assert_frame_equal(df, before)   # 列与值完全一致
        meta = prov.read(df)
        self.assertEqual(meta['as_of'], SESSION)
        self.assertEqual(meta['source'], 'tencent')
        self.assertTrue(meta['sealed'])
        self.assertEqual(meta['verified_by'], 'em')

    def test_assess_three_states(self):
        self.assertEqual(prov.assess(_df([PREV, SESSION]), SESSION), 'current')
        self.assertEqual(prov.assess(_df([PREV]), SESSION), 'lagging')
        self.assertEqual(prov.assess(_df(['2026-04-02']), SESSION), 'stale')
        self.assertEqual(prov.assess(pd.DataFrame({'close': [1]}), SESSION),
                         'unknown')

    def test_holiday_aware_adjacent(self):
        """周五 → 周一：自然日差 3 天，但仍是相邻交易日"""
        self.assertTrue(prov.is_adjacent_session('2026-11-06', '2026-11-09'))
        self.assertFalse(prov.is_adjacent_session('2026-11-03', '2026-11-09'))

    def test_cross_check_only_when_lagging_in_session(self):
        self.assertFalse(prov.should_cross_check(
            _df([PREV, SESSION]), SESSION, phase='AFTERNOON_SESSION'))
        self.assertTrue(prov.should_cross_check(
            _df([PREV]), SESSION, phase='AFTERNOON_SESSION'))
        self.assertFalse(prov.should_cross_check(
            _df([PREV]), SESSION, phase='PRE_MARKET'))
        self.assertFalse(prov.should_cross_check(
            _df(['2026-04-02']), SESSION, phase='AFTERNOON_SESSION'))


class TestCrossCheck(unittest.TestCase):
    """② 针对性跨源复核"""

    def setUp(self):
        self.main = _df([PREV, '2026-11-04'])  # 末根 = PREV 吗？不，见下
        self.main = _df(['2026-11-04', PREV], closes=[10.0, 10.5])

    def test_not_needed_when_current(self):
        df = _df([PREV, SESSION])
        out = daily_guard.cross_check(df, '600519.SH', SESSION,
                                      phase='AFTERNOON_SESSION',
                                      verifier=object())
        self.assertEqual(out['status'], daily_guard.CC_NOT_NEEDED)

    def test_confirmed_current_when_verifier_has_today_bar(self):
        class V:
            @staticmethod
            def get_daily_data(code, days):
                return _df([PREV, SESSION], closes=[10.5, 10.8])

        df = self.main
        out = daily_guard.cross_check(df, '600519.SH', SESSION,
                                      phase='AFTERNOON_SESSION', verifier=V)
        self.assertEqual(out['status'], daily_guard.CC_CONFIRMED_CURRENT)
        self.assertEqual(out['ref_date'], SESSION)
        self.assertEqual(out['verified_by'], 'V')

    def test_confirmed_lagging_when_both_sources_agree(self):
        class V:
            @staticmethod
            def get_daily_data(code, days):
                return _df(['2026-11-04', PREV], closes=[10.0, 10.5])

        out = daily_guard.cross_check(self.main, '600519.SH', SESSION,
                                      phase='AFTERNOON_SESSION', verifier=V)
        self.assertEqual(out['status'], daily_guard.CC_CONFIRMED_LAGGING)
        self.assertEqual(out['gap_pct'], 0.0)

    def test_price_mismatch_flagged(self):
        class V:
            @staticmethod
            def get_daily_data(code, days):
                return _df(['2026-11-04', PREV], closes=[10.0, 12.0])

        out = daily_guard.cross_check(self.main, '600519.SH', SESSION,
                                      phase='AFTERNOON_SESSION', verifier=V)
        self.assertEqual(out['status'], daily_guard.CC_PRICE_MISMATCH)

    def test_unavailable_when_verifier_fails(self):
        class V:
            @staticmethod
            def get_daily_data(code, days):
                return pd.DataFrame()

        out = daily_guard.cross_check(self.main, '600519.SH', SESSION,
                                      phase='AFTERNOON_SESSION', verifier=V)
        self.assertEqual(out['status'], daily_guard.CC_UNAVAILABLE)
        self.assertIsNone(out['ref_close'])

    def test_apply_dedupes_via_cache(self):
        calls = []

        class V:
            @staticmethod
            def get_daily_data(code, days):
                calls.append(code)
                return _df([PREV, SESSION], closes=[10.5, 10.8])

        cache = FakeCache()
        df = self.main
        daily_guard.apply_cross_check(df, '600519.SH', SESSION, verifier=V,
                                      phase='AFTERNOON_SESSION', cache=cache)
        self.assertEqual(len(calls), 1)
        df2 = self.main
        daily_guard.apply_cross_check(df2, '600519.SH', SESSION, verifier=V,
                                      phase='AFTERNOON_SESSION', cache=cache)
        self.assertEqual(len(calls), 1)   # 第二轮命中去重，不再打源
        self.assertEqual(prov.read(df2)['cross_check']['status'],
                         daily_guard.CC_CONFIRMED_CURRENT)


class TestSeal(unittest.TestCase):
    """③ 收盘封存与次日锚点"""

    def setUp(self):
        daily_guard.collector = daily_guard.SessionBarCollector()

    def tearDown(self):
        daily_guard.collector = daily_guard.SessionBarCollector()

    def test_collect_only_current_session(self):
        daily_guard.collector.collect(_df([PREV, SESSION]), '600519.SH', SESSION)
        daily_guard.collector.collect(_df([PREV]), '000001.SZ', SESSION)
        self.assertEqual(daily_guard.collector.size(), 1)
        rows = daily_guard.collector.drain(SESSION)
        self.assertEqual(rows[0]['code'], '600519.SH')
        self.assertEqual(rows[0]['trade_date'], SESSION)
        self.assertEqual(rows[0]['close'], 11.0)
        self.assertEqual(daily_guard.collector.size(), 0)

    def test_session_switch_clears_old_bars(self):
        daily_guard.collector.collect(_df([PREV, SESSION]), '600519.SH', SESSION)
        daily_guard.collector.collect(
            _df([SESSION, '2026-11-09']), '000001.SZ', '2026-11-09')
        self.assertEqual(daily_guard.collector.size(), 1)
        self.assertEqual(daily_guard.collector.drain('2026-11-09')[0]['code'],
                         '000001.SZ')

    def test_seal_session_writes_store_and_cache(self):
        prov.tag(_df([PREV, SESSION]), 'tencent')
        daily_guard.collector.collect(_df([PREV, SESSION]), '600519.SH', SESSION)
        store, cache = FakeStore(), FakeCache()
        out = daily_guard.seal_session(SESSION, store=store, cache=cache)
        self.assertEqual(out['sealed'], 1)
        self.assertEqual(store.rows[0]['trade_date'], SESSION)
        self.assertIn(f'kline_seal:{SESSION}', cache.data)

    def test_verify_against_seal_marks_lagging_bar(self):
        prov.tag(_df([PREV, SESSION]), 'tencent')
        daily_guard.collector.collect(_df([PREV, SESSION]), '600519.SH', SESSION)
        store = FakeStore()
        daily_guard.seal_session(SESSION, store=store)
        # 次日某源仍返回 11-06 的 bar（末根停在 T-1）
        stale_df = _df([PREV, SESSION], closes=[10.5, 11.0])
        out = daily_guard.verify_against_seal(stale_df, '600519.SH',
                                              '2026-11-09', store=store)
        self.assertEqual(out['status'], 'matches')
        self.assertEqual(out['as_of'], SESSION)
        self.assertEqual(out['sealed_close'], 11.0)

    def test_verify_mismatch_detected(self):
        prov.tag(_df([PREV, SESSION]), 'tencent')
        daily_guard.collector.collect(_df([PREV, SESSION]), '600519.SH', SESSION)
        store = FakeStore()
        daily_guard.seal_session(SESSION, store=store)
        wrong = _df([PREV, SESSION], closes=[10.5, 12.5])
        out = daily_guard.verify_against_seal(wrong, '600519.SH',
                                              '2026-11-09', store=store)
        self.assertEqual(out['status'], 'mismatch')


class TestDayBoundaryGuard(unittest.TestCase):
    """④ 日切护栏：隔夜快照作废 / 缓存 key 带交易日"""

    def test_stale_snapshot_rejected_across_sessions(self):
        mgr = SourceManager()
        mgr.register_source('demo')
        df = _df([PREV, SESSION])
        mgr._save_snapshot('get_daily_data', ('600519.SH', 120), df)
        key = mgr._snapshot_key('get_daily_data', ('600519.SH', 120))
        # 人为把快照归属日改成昨天
        mgr._snapshot_cache[key]['session'] = '2026-11-05'
        with mock.patch.object(SourceManager, '_expected_session',
                               staticmethod(lambda: SESSION)):
            self.assertIsNone(mgr._load_stale_snapshot(
                'get_daily_data', ('600519.SH', 120)))
        self.assertNotIn(key, mgr._snapshot_cache)

    def test_stale_snapshot_kept_within_same_session(self):
        mgr = SourceManager()
        mgr.register_source('demo')
        df = _df([PREV, SESSION])
        mgr._save_snapshot('get_daily_data', ('600519.SH', 120), df)
        with mock.patch.object(SourceManager, '_expected_session',
                               staticmethod(lambda: SESSION)):
            out = mgr._load_stale_snapshot(
                'get_daily_data', ('600519.SH', 120))
        self.assertIsNotNone(out)
        self.assertTrue(out.attrs.get('_stale'))

    def test_dead_priority_entry_warns(self):
        class NoDaily:
            pass

        mgr = SourceManager()
        mgr.register_source('dead')
        # loguru 不接入标准 logging，直接 mock 其 warning
        with mock.patch('quant_engine.data.source_manager.logger') as lg:
            out, name = mgr.execute_with_fallback(
                ['dead'], {'dead': NoDaily()}, 'get_daily_data',
                '600519.SH', 120, return_source=True)
            warned = ' '.join(str(c) for c in lg.warning.call_args_list)
        self.assertIsNone(out)
        self.assertIsNone(name)
        self.assertIn('no-op', warned)

    def test_return_source_reports_actual_hit(self):
        class Good:
            @staticmethod
            def get_daily_data(code, days):
                return _df([PREV, SESSION])

        mgr = SourceManager()
        mgr.register_source('good')
        out, name = mgr.execute_with_fallback(
            ['good'], {'good': Good()}, 'get_daily_data',
            '600519.SH', 120, return_source=True)
        self.assertEqual(name, 'good')
        self.assertEqual(len(out), 2)


class TestSinaStaticmethod(unittest.TestCase):
    """P0 回归：新浪日K 必须是 staticmethod，降级链才不会 TypeError"""

    def test_instance_call_works(self):
        from quant_engine.data.sources.sina import SinaSource
        src = SinaSource()
        with mock.patch.object(SinaSource, 'DAILY_URL', 'http://invalid'), \
                mock.patch('quant_engine.data.sources.sina.requests.get') as g:
            g.return_value.status_code = 503
            out = src.get_daily_data('600519.SH', 120)
        self.assertTrue(out.empty)   # 不再抛 TypeError


if __name__ == '__main__':
    unittest.main()
