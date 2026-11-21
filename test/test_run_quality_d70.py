"""D70 运行质量增强专项测试（2026-11-21）

覆盖 docs/39 §5 零信号影响批次：
- N-08 非有限数入口拦截（finite_or_none / signal_to_api_dict choke point）
- N-02 行情质量元数据四元组（只写 df.attrs，不改列不改值）
- N-05 通知按通道字节预算（UTF-8 字节安全截断）
- N-04 doctor 只读脱敏端点（不泄漏密钥）

共同纪律：**趋势信号零漂移**——本批改动只作用展示层/旁路 attrs，
不触碰 Signal 决策字段（action/score/strategy/position/kelly）。
"""
from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_engine.data.source_manager import SourceManager
from quant_engine.models.signal import Signal, SignalAction
from quant_engine.services.notifications.manager import (
    _CHANNEL_BYTE_BUDGET,
    clip_to_byte_budget,
)
from quant_engine.services.signals.signal_generator import signal_to_api_dict
from quant_engine.utils.numeric import finite_or_none, sanitize_floats


class TestN08NonFinite(unittest.TestCase):
    """N-08：NaN/Inf → None，合法值不变。"""

    def test_nan_and_infinities(self):
        for bad in (float('nan'), float('inf'), float('-inf')):
            self.assertIsNone(finite_or_none(bad))
        self.assertEqual(finite_or_none(0.0), 0.0)
        self.assertEqual(finite_or_none(-3.5), -3.5)

    def test_non_float_passthrough(self):
        for v in ('x', True, None, 7):
            self.assertEqual(finite_or_none(v), v)

    def test_sanitize_floats_key_filter(self):
        d = {'score': float('nan'), 'code': '600519', 'price': 1.5}
        out = sanitize_floats(d, keys=('score', 'price'))
        self.assertIsNone(out['score'])
        self.assertEqual(out['price'], 1.5)
        self.assertEqual(out['code'], '600519')

    def test_signal_to_api_dict_cleans_nan(self):
        s = Signal(
            code='600519.SH', name='贵州茅台', action=SignalAction.BUY,
            score=float('nan'), position=0.1, reason=['test'],
            timestamp=datetime(2026, 11, 21), strategy='demo', price=1500.0,
            current_price=float('inf'),
        )
        d = signal_to_api_dict(s)
        self.assertIsNone(d['score'])
        self.assertIsNone(d['current_price'])
        # 决策字段原样保留（零信号影响）
        self.assertEqual(d['action'], 'BUY')
        self.assertEqual(d['code'], '600519.SH')
        self.assertEqual(d['price'], 1500.0)


class TestN02QualityMetadata(unittest.TestCase):
    """N-02：陈旧快照质量元数据只写 attrs，不改列不改值。"""

    def setUp(self):
        self.mgr = SourceManager()

    @staticmethod
    def _df():
        return pd.DataFrame({'close': [10.0, 11.0]})

    def test_stale_snapshot_attrs_metadata(self):
        mgr = self.mgr
        df = self._df()
        mgr.register_source('demo')
        # save/load 同处一个 mock 上下文：避免 D66 跨交易日护栏按真实运行日
        # 把固定 session 的快照正确作废（同类日历老化陷阱）
        with mock.patch.object(SourceManager, '_expected_session',
                               staticmethod(lambda: '2026-11-21')):
            mgr._save_snapshot('get_realtime_quote', ('600519.SH',), df,
                               source_name='tencent')
            # 把 timestamp 往回调 600s，模拟陈旧
            key = mgr._snapshot_key('get_realtime_quote', ('600519.SH',))
            mgr._snapshot_cache[key]['timestamp'] -= 600
            out = mgr._load_stale_snapshot(
                'get_realtime_quote', ('600519.SH',))
        self.assertIsNotNone(out)
        self.assertEqual(out.attrs['data_quality'], 'stale')
        self.assertEqual(out.attrs['fallback_from'], 'tencent')
        self.assertEqual(out.attrs['quote_date'], '2026-11-21')
        self.assertGreaterEqual(out.attrs['stale_seconds'], 600)
        self.assertIn('provider_timestamp', out.attrs)
        self.assertIn('missing_fields', out.attrs)
        # 值/列零改动
        self.assertEqual(list(out.columns), ['close'])
        self.assertEqual(out['close'].tolist(), [10.0, 11.0])


class TestN05ByteBudget(unittest.TestCase):
    """N-05：按 UTF-8 字节预算安全截断（中文不被切坏）。"""

    def test_short_content_unchanged(self):
        self.assertEqual(clip_to_byte_budget('正常', 'DingTalk'), '正常')

    def test_truncates_to_budget(self):
        long = '中' * 3000  # 9000 字节 > 4096
        out = clip_to_byte_budget(long, 'DingTalk')
        self.assertLessEqual(len(out.encode('utf-8')), 4096)
        self.assertIn('已截断', out)

    def test_no_budget_means_passthrough(self):
        long = '中' * 3000
        self.assertEqual(clip_to_byte_budget(long, 'DingTalk', budget=0), long)

    def test_all_channels_have_budget(self):
        for name in ('DingTalk', 'Feishu', 'WeChat', 'Telegram'):
            self.assertGreater(_CHANNEL_BYTE_BUDGET.get(name, 0), 0)


class TestN03ErrorClassification(unittest.TestCase):
    """N-03：错误分类计数旁路（不改变现有熔断判定）。"""

    def test_error_kind_counts(self):
        from quant_engine.data.source_manager import SourceStatus
        s = SourceStatus('demo')
        s.record_error(300, kind='timeout', message='t')
        s.record_error(300, kind='error', message='e')
        s.record_error(300, kind='timeout', message='t2')
        self.assertEqual(s.error_kind_counts['timeout'], 2)
        self.assertEqual(s.error_kind_counts['error'], 1)
        # 不影响现有 error_count 语义
        self.assertEqual(s.error_count, 3)

    def test_reset_clears_classification(self):
        from quant_engine.data.source_manager import SourceStatus
        s = SourceStatus('demo')
        s.record_error(300, kind='timeout')
        s.error_kind_counts['auth'] = 1
        s.endpoint_cooldowns[('get_daily_data', '600519')] = 999.0
        s.reset()
        self.assertEqual(s.error_kind_counts, {})
        self.assertEqual(s.endpoint_cooldowns, {})

    def test_status_report_exposes_classification(self):
        from quant_engine.data.source_manager import SourceStatus
        sm = SourceManager()
        sm.sources['demo'] = SourceStatus('demo')
        sm.sources['demo'].record_error(300, kind='timeout')
        report = sm.get_status_report()
        self.assertEqual(report['demo']['error_kind_counts'], {'timeout': 1})
        self.assertIn('endpoint_cooldowns', report['demo'])


class TestN04DoctorContract(unittest.TestCase):
    """N-04：doctor 端点注册 + 脱敏（不输出任何 key/token/webhook）。"""

    def test_doctor_route_registered(self):
        from quant_engine.api.routers import export_router
        paths = {r.path for r in export_router().routes}
        self.assertIn('/api/system/doctor', paths)

    def test_doctor_payload_has_no_secrets(self):
        from quant_engine.api.routers.system import system_doctor
        with mock.patch('quant_engine.api.routers.system.datacenter') as dc:
            dc.cache.is_connected = False
            out = system_doctor()
        import json
        raw = json.dumps(out, ensure_ascii=False, default=str)
        for leaked in ('tp-', 'sk-', 'access_token', 'DINGTALK_SECRET',
                       'srm8d93'):
            self.assertNotIn(leaked, raw)
        self.assertIn('checked_at', out)
        self.assertIn('settings', out)
        self.assertIn('unisound_model', out['settings'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
