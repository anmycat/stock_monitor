"""数据源契约层测试（D53：spec / align / registry）

覆盖 MCP 风格契约层的三个承诺：
1. 能力声明可判（registry.sources_for 按优先级返回）
2. 字段对齐只做「源列 → 标准列」映射（缺列填 None、多余列丢弃、不猜不造）
3. 成交量单位换算依据源口径声明（腾讯 688=股 / 其他板=手）
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.align import (
    align,
    align_daily_default,
    align_daily_tencent,
    align_pool_default,
)
from quant_engine.data.registry import (
    COLUMN_MAPPINGS,
    SOURCES,
    describe,
    get_spec,
    sources_for,
)
from quant_engine.data.spec import (
    ADJUST_FORWARD,
    STANDARD_COLUMNS,
    UNIT_SHARES,
    Capability,
    DataResult,
)


class TestSpec(unittest.TestCase):
    def test_capability_enum_values(self):
        self.assertEqual(str(Capability.DAILY), 'daily')
        self.assertIn(Capability.POOL, Capability)

    def test_standard_columns_defined_for_all_capabilities(self):
        for cap in Capability:
            self.assertIn(cap, STANDARD_COLUMNS)
            self.assertTrue(STANDARD_COLUMNS[cap])

    def test_data_result_success_and_failure(self):
        ok = DataResult.success(pd.DataFrame({'a': [1]}), 'demo', Capability.DAILY,
                                elapsed_ms=12)
        self.assertTrue(ok.ok)
        self.assertEqual(ok.row_count, 1)
        self.assertFalse(ok.stale)

        bad = DataResult.failure('demo', Capability.DAILY, 'TIMEOUT', 'timeout', 30)
        self.assertFalse(bad.ok)
        self.assertEqual(bad.error_code, 'TIMEOUT')
        self.assertEqual(bad.row_count, 0)
        self.assertIn('error_code', bad.to_dict())


class TestRegistry(unittest.TestCase):
    def test_all_sources_have_spec(self):
        for name in ('tencent', 'sina', 'ths_web', 'eastmoney', 'tdx', 'tushare'):
            spec = get_spec(name)
            self.assertIsNotNone(spec, f'{name} 缺规格声明')
            self.assertTrue(spec.capabilities)

    def test_sources_for_capability_sorted_by_priority(self):
        daily = sources_for(Capability.DAILY)
        self.assertTrue(daily)
        names = [s.name for s in daily]
        self.assertEqual(names[0], 'tencent')
        self.assertLess(daily[0].rank(Capability.DAILY), daily[-1].rank(Capability.DAILY))

    def test_pool_only_from_em_pool(self):
        pool = [s.name for s in sources_for(Capability.POOL)]
        self.assertEqual(pool, ['em_pool'])

    def test_tushare_requires_key(self):
        self.assertTrue(get_spec('tushare').needs_key)
        self.assertEqual(get_spec('tushare').key_env, 'TUSHARE_TOKEN')

    def test_volume_unit_declared(self):
        self.assertEqual(get_spec('ths_web').volume_unit, UNIT_SHARES)
        self.assertEqual(get_spec('tencent').adjust, ADJUST_FORWARD)

    def test_column_mappings_reference_known_sources(self):
        for src in COLUMN_MAPPINGS:
            self.assertIn(src, SOURCES)

    def test_describe_returns_all(self):
        self.assertEqual(len(describe()), len(SOURCES))


class TestAlign(unittest.TestCase):
    def test_align_maps_and_fills_missing(self):
        raw = pd.DataFrame({'d': ['2026-01-01'], 'o': [1.0], 'c': [2.0], 'junk': [9]})
        out = align(Capability.DAILY, raw,
                    {'d': 'date', 'o': 'open', 'c': 'close'})
        self.assertEqual(list(out.columns), list(STANDARD_COLUMNS[Capability.DAILY]))
        self.assertEqual(out.iloc[0]['date'], '2026-01-01')
        self.assertIsNone(out.iloc[0]['high'])
        self.assertNotIn('junk', out.columns)

    def test_align_empty_returns_empty_standard_frame(self):
        out = align(Capability.DAILY, None, {})
        self.assertEqual(len(out), 0)
        self.assertEqual(list(out.columns), list(STANDARD_COLUMNS[Capability.DAILY]))

    def test_tencent_volume_conversion(self):
        """腾讯 688 → 股（不换算）；600 → 手（×100）"""
        raw = pd.DataFrame({'date': ['2026-01-01'], 'close': [10.0], 'volume': [1000.0]})
        out = align_daily_tencent(raw, '688001.SH')
        self.assertEqual(out.iloc[0]['volume'], 1000.0)
        out2 = align_daily_tencent(raw, '600000.SH')
        self.assertEqual(out2.iloc[0]['volume'], 100000.0)

    def test_default_daily_keeps_shares(self):
        raw = pd.DataFrame({'date': ['2026-01-01'], 'close': [10.0], 'volume': [500.0]})
        out = align_daily_default(raw)
        self.assertEqual(out.iloc[0]['volume'], 500.0)

    def test_pool_alignment(self):
        raw = pd.DataFrame({'code': ['600000.SH'], 'name': ['浦发'], 'lbc': [3]})
        out = align_pool_default(raw)
        self.assertEqual(out.iloc[0]['code'], '600000.SH')
        self.assertIsNone(out.iloc[0]['seal_fund'])


if __name__ == '__main__':
    unittest.main()
