"""D80 eltdx 主力 + 东财证实源降频专项测试

纪律：只验证「数据源选择 / 成交量口径对齐 / 抽样门控」三类确定性逻辑，
不触碰评分/门控/Kelly/回测。注入假配置与假客户端，本地 py3.9 无 eltdx 亦可跑。
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from quant_engine.config import Settings
from quant_engine.data.source import (
    _PRIORITY,
    _PRIORITY_PRIMARY,
    resolve_priority,
)
from quant_engine.data.sources.eltdx_client import EltdxClient
from quant_engine.utils import kline_provenance as prov


def _df(last: str = '2026-11-23', status: str | None = None) -> pd.DataFrame:
    df = pd.DataFrame({
        'date': ['2026-11-21', '2026-11-22', last],
        'close': [10.0, 10.5, 10.3],
    })
    if status is not None:
        df.attrs['kline_frame_status'] = status
    return df


class _Bar:
    def __init__(self, lots):
        self.time = datetime(2026, 11, 23)
        self.open = 10.0
        self.high = 10.5
        self.low = 9.9
        self.close = 10.3
        self.volume_lots = lots
        self.amount = 103000.0


class _Bars:
    def __init__(self, lots):
        self.lots = lots

    def get(self, code, *, period, count, adjust):
        return SimpleNamespace(bars=[_Bar(self.lots)])


class TestPrioritySwitch(unittest.TestCase):
    """D80：主力开关只在 daily/minute 生效，stock_list/realtime 不动。"""

    def test_default_is_off(self):
        """用户裁定：候选主力默认关闭，主链保持 tencent 原序。"""
        self.assertFalse(Settings().ELTDX_AS_PRIMARY)

    def test_primary_constants(self):
        self.assertEqual(_PRIORITY_PRIMARY['daily'][0], 'eltdx')
        self.assertEqual(_PRIORITY_PRIMARY['minute'][0], 'eltdx')
        # 原链仍保留 eltdx 末位（向后兼容断言）
        self.assertEqual(_PRIORITY['daily'][-1], 'eltdx')

    def test_resolve_primary_on(self):
        cfg = Settings(ELTDX_ENABLED=True, ELTDX_AS_PRIMARY=True)
        with mock.patch('quant_engine.config.get_settings', return_value=cfg):
            self.assertEqual(resolve_priority('daily'), _PRIORITY_PRIMARY['daily'])
            self.assertEqual(resolve_priority('minute'), _PRIORITY_PRIMARY['minute'])

    def test_resolve_primary_off(self):
        cfg = Settings(ELTDX_ENABLED=True, ELTDX_AS_PRIMARY=False)
        with mock.patch('quant_engine.config.get_settings', return_value=cfg):
            self.assertEqual(resolve_priority('daily'), _PRIORITY['daily'])
            self.assertEqual(resolve_priority('minute'), _PRIORITY['minute'])

    def test_stock_list_realtime_never_touched(self):
        """无主力模式定义 → 恒回退原序，eltdx 无全市场/realtime 主链能力。"""
        cfg = Settings(ELTDX_ENABLED=True, ELTDX_AS_PRIMARY=True)
        with mock.patch('quant_engine.config.get_settings', return_value=cfg):
            self.assertEqual(resolve_priority('stock_list'), _PRIORITY['stock_list'])
            self.assertEqual(resolve_priority('realtime'), _PRIORITY['realtime'])


class TestVolumeAlignment(unittest.TestCase):
    """D80：eltdx volume 取整到百位，对齐腾讯 fqkline（零量能因子漂移）。"""

    def test_lots_rounded_to_hundreds(self):
        # 2444036.16 手 → 2444036 手 → 244403600 股（腾讯口径）
        fake = SimpleNamespace(
            bars=_Bars(2444036.16),
            minutes=SimpleNamespace(today=lambda c: SimpleNamespace(points=[])),
            trades=SimpleNamespace(all_today=lambda c: []),
            money_flow=SimpleNamespace(daily=lambda c: SimpleNamespace(records=[])),
            helpers=SimpleNamespace(limit_ladder=lambda *a, **k: SimpleNamespace(rows=[]),
                                    realtime_rank=lambda: SimpleNamespace(rows=[]),
                                    full_quotes=lambda c: []),
        )
        cfg = Settings(ELTDX_ENABLED=True)
        c = EltdxClient(settings=cfg, client_factory=lambda h: fake)
        df = c.get_daily_data('000001.SZ', 5)
        self.assertEqual(float(df['volume'].iloc[0]), 244403600.0)

    def test_ohlc_untouched(self):
        """取整只作用于 volume，四价保持精确值。"""
        fake = SimpleNamespace(
            bars=_Bars(56975.89),  # → 56976 手
            minutes=SimpleNamespace(today=lambda c: SimpleNamespace(points=[])),
            trades=SimpleNamespace(all_today=lambda c: []),
            money_flow=SimpleNamespace(daily=lambda c: SimpleNamespace(records=[])),
            helpers=SimpleNamespace(limit_ladder=lambda *a, **k: SimpleNamespace(rows=[]),
                                    realtime_rank=lambda: SimpleNamespace(rows=[]),
                                    full_quotes=lambda c: []),
        )
        cfg = Settings(ELTDX_ENABLED=True)
        c = EltdxClient(settings=cfg, client_factory=lambda h: fake)
        df = c.get_daily_data('600519.SH', 5)
        self.assertEqual(float(df['close'].iloc[0]), 10.3)
        self.assertEqual(float(df['volume'].iloc[0]), 5697600.0)


class TestCrossCheckSampling(unittest.TestCase):
    """D80：东财证实源降频——异常帧必查 + 1/N 确定性抽样。"""

    PHASE = 'AFTER_HOURS'

    def test_legacy_behavior_when_no_sample_n(self):
        """sample_n=None → 保持 D66 原行为（老调用路径/既有测试兼容）。"""
        df = _df()  # lagging（末根 11-23 但 session 设为更晚）
        self.assertTrue(prov.should_cross_check(df, '2026-11-24',
                                                phase=self.PHASE, code='000001',
                                                sample_n=None))

    def test_sample_n_zero_only_invalid_frame(self):
        valid = _df()
        invalid = _df(status='invalid')
        # 正常帧 + sample_n=0 → 不查
        self.assertFalse(prov.should_cross_check(valid, '2026-11-24',
                                                 phase=self.PHASE, code='000001',
                                                 sample_n=0))
        # 异常帧 + sample_n=0 → 必查
        self.assertTrue(prov.should_cross_check(invalid, '2026-11-24',
                                                phase=self.PHASE, code='000001',
                                                sample_n=0))

    def test_sampling_is_deterministic(self):
        """同 code+session 结果稳定（重启/多进程一致），不用 random。"""
        df = _df()
        results = [prov.should_cross_check(df, '2026-11-24', phase=self.PHASE,
                                           code='600519', sample_n=2)
                   for _ in range(5)]
        self.assertEqual(len(set(results)), 1)

    def test_sample_n_one_samples_all(self):
        """sample_n=1 → hash%1==0 恒真 → 全查（可用于回退到 D66 行为）。"""
        df = _df()
        for code in ['000001', '600519', '000858']:
            self.assertTrue(prov.should_cross_check(df, '2026-11-24',
                                                    phase=self.PHASE, code=code,
                                                    sample_n=1))

    def test_reduces_request_volume(self):
        """27 票池 sample_n=9 → 抽样票数显著少于全量 27。"""
        df = _df()
        codes = [f'{600000 + i}' for i in range(27)]
        hit = [c for c in codes
               if prov.should_cross_check(df, '2026-11-24', phase=self.PHASE,
                                          code=c, sample_n=9)]
        self.assertLess(len(hit), 27)
        self.assertGreaterEqual(len(hit), 0)  # 确定性，可能 0~6 票


if __name__ == '__main__':
    unittest.main(verbosity=2)
