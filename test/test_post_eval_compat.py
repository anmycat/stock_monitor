"""B4 回归：/api/evaluation/runs 对缺统计字段的旧 run 数据不再 500

背景（13 报告 §4.3 + B4）：PostEvalResponse 原有 8 个必填字段，而
PostEvaluation.get_run_list 仅返回 run_id/run_date/total_signals/market_state
四键——5 个统计字段（win_count/win_rate/avg_pnl/avg_max_profit/avg_max_drawdown）
在任何非空 runs:list 下都缺失，pydantic ValidationError 使端点恒 500。
修复：五个统计字段加默认值（真实统计走 /api/evaluation/{run_id} 单点现算）。
本测试冻结该行为，防回归。
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.api.routes import PostEvalResponse
from quant_engine.backtest.post_evaluation import PostEvaluation


class FakeCache:
    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, ttl=0):
        self._data[key] = value
        return True


class PostEvalCompatTests(unittest.TestCase):
    def _make_eval(self):
        pe = PostEvaluation.__new__(PostEvaluation)
        pe.cache = FakeCache()
        return pe

    def test_legacy_run_missing_stats_fields(self):
        """旧 run（仅四键）构造 Response 不抛异常，统计字段落默认值"""
        legacy = {
            "run_id": "run_20261008_113000",
            "run_date": "2026-10-08T11:30:00",
            "total_signals": 7,
            "market_state": "bear",
        }
        resp = PostEvalResponse(**legacy)
        self.assertEqual(resp.run_id, "run_20261008_113000")
        self.assertEqual(resp.total_signals, 7)
        self.assertEqual(resp.win_count, 0)
        self.assertEqual(resp.win_rate, 0.0)
        self.assertEqual(resp.avg_pnl, 0.0)
        self.assertEqual(resp.avg_max_profit, 0.0)
        self.assertEqual(resp.avg_max_drawdown, 0.0)

    def test_run_list_endpoint_shape_no_exception(self):
        """get_run_list 输出逐条可构造 Response（修复前此处恒抛 ValidationError）"""
        pe = self._make_eval()
        pe.cache.set("runs:list", ["run_a", "run_b"], ttl=0)
        pe.cache.set("run:run_a", {
            "run_id": "run_a",
            "timestamp": "2026-10-09T09:30:00",
            "signals": [{"code": "000001.SZ"}, {"code": "600000.SH"}],
            "market_state": {"type": "neutral"},
        }, ttl=0)
        pe.cache.set("run:run_b", {
            "run_id": "run_b",
            "timestamp": "2026-10-10T09:30:00",
            "signals": [],
            "market_state": {"type": "bear"},
        }, ttl=0)
        runs = pe.get_run_list(20)
        self.assertEqual(len(runs), 2)
        for r in runs:
            resp = PostEvalResponse(**r)  # 不抛异常即通过
            self.assertGreaterEqual(resp.total_signals, 0)

    def test_full_record_still_parses(self):
        """完整字段记录（含统计）不受默认值影响"""
        full = {
            "run_id": "run_c",
            "run_date": "2026-10-11T09:30:00",
            "total_signals": 12,
            "win_count": 7,
            "win_rate": 58.3,
            "avg_pnl": 1.94,
            "avg_max_profit": 3.2,
            "avg_max_drawdown": -2.1,
        }
        resp = PostEvalResponse(**full)
        self.assertEqual(resp.win_count, 7)
        self.assertAlmostEqual(resp.win_rate, 58.3)


if __name__ == "__main__":
    unittest.main()
