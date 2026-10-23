"""数据校验器测试（D32 审计补充）

背景：B5（2026-10-21）给 market overview 增加 suspect 标志，但
validate_market_data 原先不消费该标志——suspect 零结果会静默通过
校验，validation 成功率对数据源系统性损坏不感知。本测试冻结修复后
的行为：suspect=True 必须记校验失败；无 suspect 字段的旧结构不误报。
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.services.market.data_validator import DataValidator


class _FakeCache:
    def get(self, key):
        return None

    def set(self, key, value, ttl=0):
        return True


class ValidateMarketDataTests(unittest.TestCase):
    def setUp(self):
        self.v = DataValidator(_FakeCache())

    def test_valid_overview_passes(self):
        ok, errors = self.v.validate_market_data(
            {"total": 100, "up": 60, "down": 30, "flat": 10,
             "avg_change": 0.5, "suspect": False})
        self.assertTrue(ok)
        self.assertEqual(errors, [])

    def test_suspect_overview_fails(self):
        """suspect=True（数据卫生断言未过）必须校验失败"""
        ok, errors = self.v.validate_market_data(
            {"total": 0, "up": 0, "down": 0, "flat": 0,
             "avg_change": 0, "suspect": True})
        self.assertFalse(ok)
        self.assertTrue(any("可疑" in e for e in errors))

    def test_legacy_dict_without_suspect_not_flagged(self):
        """旧结构（无 suspect 键）不因新标志误报"""
        ok, errors = self.v.validate_market_data(
            {"total": 100, "up": 60, "down": 30, "flat": 10, "avg_change": 0.5})
        self.assertTrue(ok)
        self.assertEqual(errors, [])

    def test_sum_mismatch_fails(self):
        ok, _errors = self.v.validate_market_data(
            {"total": 100, "up": 60, "down": 30, "flat": 5, "avg_change": 0.5})
        self.assertFalse(ok)

    def test_extreme_avg_fails(self):
        ok, _errors = self.v.validate_market_data(
            {"total": 10, "up": 10, "down": 0, "flat": 0, "avg_change": 11.35})
        self.assertFalse(ok)


if __name__ == '__main__':
    unittest.main(verbosity=2)
