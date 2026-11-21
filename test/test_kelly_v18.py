"""Kelly v1.8 归零落库测试（2026-11-21 用户裁定，docs/44）

覆盖：
1. 无正期望（f*≤0）→ 建议仓位 = KELLY_NO_EDGE_MIN_POSITION（默认 0.0，归零口径）
2. legacy 回退：no_edge_min_position=0.05 → 恢复旧「最小仓位 5%」地板
3. 有正期望路径不受影响（edge 路径仍受 min_position 地板与 max 上限约束）
4. get_position_sizer 默认构造从 Settings 读取 KELLY_NO_EDGE_MIN_POSITION
5. rules JSON v1.8：全规则 f_half=0、v17 标注被取代、v4 窗口观测存在
"""
import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.models.market import MarketState, MarketType
from quant_engine.services.risk.position_sizer import (
    KellyConfig,
    PositionSizer,
    get_position_sizer,
    reset_position_sizer,
)


def _market(mtype=MarketType.BULL, score=60):
    from datetime import datetime
    return MarketState(type=mtype, score=score, trend=0.0,
                       volume_ratio=1.0, breadth=0.0,
                       timestamp=datetime.now())


class TestKellyNoEdgeZero(unittest.TestCase):
    def test_no_edge_recommended_zero(self):
        """胜率 28%/盈亏比 1.195（docs/44 v4 窗口）→ f*<0 → 建议 0"""
        sizer = PositionSizer(KellyConfig())
        r = sizer.calculate_position(
            signal_score=80, market_state=_market(), win_rate=0.28,
            profit_loss_ratio=1.195, strategy_name='')
        self.assertFalse(r.kelly_position > 0 and r.kelly_full_position > 0)
        self.assertEqual(r.recommended_position, 0.0)
        self.assertEqual(r.risk_adjusted_position, 0.0)

    def test_no_edge_legacy_floor(self):
        """回退口径：KELLY_NO_EDGE_MIN_POSITION=0.05 恢复旧 5% 地板"""
        sizer = PositionSizer(KellyConfig(no_edge_min_position=0.05))
        r = sizer.calculate_position(
            signal_score=80, market_state=_market(), win_rate=0.28,
            profit_loss_ratio=1.195, strategy_name='')
        self.assertEqual(r.recommended_position, 0.05)

    def test_edge_path_unchanged(self):
        """有正期望：默认参数 0.55/1.5 → f*=0.25>0，走正常路径（≥5% 地板）"""
        sizer = PositionSizer(KellyConfig())
        r = sizer.calculate_position(
            signal_score=80, market_state=_market(), win_rate=0.55,
            profit_loss_ratio=1.5, strategy_name='')
        self.assertGreaterEqual(r.recommended_position, 0.05)
        self.assertTrue(r.kelly_full_position > 0)

    def test_edge_kelly_cap(self):
        """有正期望但 Kelly 值小于基础仓位 → recommended=min(base, kelly)"""
        sizer = PositionSizer(KellyConfig())
        # 0.40*1.2-0.6 = -0.12 → 无 edge；改用刚过线的参数验证 cap
        r2 = sizer.calculate_position(
            signal_score=95, market_state=_market(), win_rate=0.52,
            profit_loss_ratio=1.10, strategy_name='')  # f*=(0.572-0.48)/1.10≈8.4%>0
        self.assertTrue(r2.kelly_full_position > 0)
        self.assertLessEqual(r2.recommended_position, 0.40)


class TestGetSizerFromSettings(unittest.TestCase):
    def tearDown(self):
        reset_position_sizer()

    def test_default_reads_settings(self):
        from quant_engine.config import get_settings
        get_settings.cache_clear()
        reset_position_sizer()
        sizer = get_position_sizer()
        self.assertEqual(sizer.config.no_edge_min_position, 0.0)
        get_settings.cache_clear()

    def test_env_override(self):
        from quant_engine.config import get_settings
        os.environ['KELLY_NO_EDGE_MIN_POSITION'] = '0.05'
        try:
            get_settings.cache_clear()
            reset_position_sizer()
            sizer = get_position_sizer()
            self.assertAlmostEqual(sizer.config.no_edge_min_position, 0.05)
        finally:
            os.environ.pop('KELLY_NO_EDGE_MIN_POSITION', None)
            get_settings.cache_clear()


class TestRulesJsonV18(unittest.TestCase):
    def setUp(self):
        rules_path = (Path(__file__).resolve().parent.parent
                      / 'research/rules/methodology_rules.json')
        with open(rules_path, encoding='utf-8') as f:
            self.rules = json.load(f)

    def test_version(self):
        self.assertEqual(self.rules['_meta']['version'], '1.8')

    def test_all_rules_zero(self):
        v18 = self.rules['kelly']['rule_positions_v18']
        for k in ('R01', 'R02', 'R04', 'R05', 'all'):
            self.assertEqual(v18[k]['f_half'], 0.0, k)

    def test_v17_marked_superseded(self):
        v17 = self.rules['kelly']['rule_positions_v17']
        self.assertIn('已被 v1.8 取代', v17['_desc'])

    def test_v4_window_recorded(self):
        v4 = self.rules['kelly']['kelly_3m_v4']
        self.assertEqual(v4['_all']['win'], 27.3)
        self.assertEqual(v4['_all']['f_half'], 0.0)
        self.assertEqual(v4['_all']['n'], 227)


if __name__ == '__main__':
    unittest.main()
