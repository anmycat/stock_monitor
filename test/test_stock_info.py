"""D49 股票静态信息表测试（2026-10-27）

覆盖：板块识别 / ST 判定 / ingest 小表拒绝 / diff 五类变更 /
fresh 判定 / to_dataframe 过滤 / get_stock_list 免打源路径。
"""
import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from quant_engine.data.stock_info import (
    MIN_INGEST_ROWS,
    StockInfoTable,
    detect_board,
    format_stock_info_diff,
    is_st_name,
)


class TestBoardDetection(unittest.TestCase):
    def test_main_boards(self):
        self.assertEqual(detect_board('600519.SH'), 'SH_MAIN')
        self.assertEqual(detect_board('601318.SH'), 'SH_MAIN')
        self.assertEqual(detect_board('000001.SZ'), 'SZ_MAIN')
        self.assertEqual(detect_board('002594.SZ'), 'SZ_SME')

    def test_star_and_gem(self):
        self.assertEqual(detect_board('688256.SH'), 'SH_STAR')
        self.assertEqual(detect_board('689009.SH'), 'SH_STAR_CDR')
        self.assertEqual(detect_board('300750.SZ'), 'SZ_GEM')
        self.assertEqual(detect_board('301399.SZ'), 'SZ_GEM')

    def test_bj_and_fund(self):
        self.assertEqual(detect_board('920819.BJ'), 'BJ')
        self.assertEqual(detect_board('830799.BJ'), 'BJ')
        self.assertEqual(detect_board('510300.SH'), 'FUND')
        self.assertEqual(detect_board('159915.SZ'), 'FUND')

    def test_unknown(self):
        self.assertEqual(detect_board('999999.SH'), 'UNKNOWN')


class TestStNameDetection(unittest.TestCase):
    def test_variants(self):
        self.assertTrue(is_st_name('ST 众泰'))
        self.assertTrue(is_st_name('*ST 博天'))
        self.assertTrue(is_st_name('S*ST 生化'))
        self.assertTrue(is_st_name('SST 前锋'))

    def test_normal_names(self):
        self.assertFalse(is_st_name('贵州茅台'))
        self.assertFalse(is_st_name('中际旭创'))
        # 防御：名称含 ST 字母但不以 ST 开头
        self.assertFalse(is_st_name('万科A'))


class TestStockInfoTable(unittest.TestCase):
    def setUp(self):
        self.tmp_path = Path('/tmp/test_stock_info_table.json')
        if self.tmp_path.exists():
            self.tmp_path.unlink()
        self.table = StockInfoTable(path=str(self.tmp_path))

    def tearDown(self):
        if self.tmp_path.exists():
            self.tmp_path.unlink()

    def _make_df(self, rows):
        return pd.DataFrame(rows, columns=['code', 'name'])

    def _big_df(self, n=MIN_INGEST_ROWS):
        rows = [{'code': f'60{i:04d}.SH', 'name': f'股票{i}'} for i in range(n)]
        return self._make_df(rows)

    def test_ingest_rejects_small_table(self):
        """兜底小表（5 只）不得覆盖静态表"""
        small = self._make_df([
            {'code': '000001.SZ', 'name': '平安银行'},
            {'code': '600519.SH', 'name': '贵州茅台'},
        ])
        result = self.table.ingest(small)
        self.assertIsNone(result)
        self.assertEqual(len(self.table), 0)

    def test_ingest_and_diff(self):
        df = self._big_df()
        diff = self.table.ingest(df)
        self.assertEqual(len(self.table), MIN_INGEST_ROWS)
        self.assertEqual(len(diff['added']), MIN_INGEST_ROWS)  # 首次全新增
        self.assertFalse(diff.get('skipped'))
        # 文件落盘
        data = json.loads(self.tmp_path.read_text(encoding='utf-8'))
        self.assertEqual(data['stocks']['600000.SH']['name'], '股票0')
        self.assertEqual(data['stocks']['600000.SH']['board'], 'SH_MAIN')
        self.assertFalse(data['stocks']['600000.SH']['is_st'])

    def test_diff_five_categories(self):
        self.table.ingest(self._big_df())
        # 构造变更：一只戴帽 / 一只脱帽 / 一只更名 / 一只新增 / 一只退市
        rows = [{'code': f'60{i:04d}.SH', 'name': f'股票{i}'} for i in range(MIN_INGEST_ROWS)]
        rows[1]['name'] = '*ST 变更'          # st_on
        rows[2]['name'] = '原名ST股'           # 先戴帽
        rows[2]['name'] = '已脱帽'             # st_off（先建 st_on 需两轮）
        rows[3]['name'] = '新名字'             # renamed
        rows.append({'code': '688999.SH', 'name': '新上市'})  # added
        del rows[4]                            # removed（60i=4）
        diff = self.table.ingest(self._make_df(rows))
        self.assertIn('688999.SH', diff['added'])
        self.assertIn('600004.SH', diff['removed'])
        self.assertIn('600001.SH', diff['st_on'])
        self.assertIn('600003.SH', diff['renamed'])
        # st_off：先造一只 ST 再脱帽
        self.table.ingest(self._make_df(
            [{'code': f'60{i:04d}.SH', 'name': ('*ST 甲' if i == 10 else f'股票{i}')}
             for i in range(MIN_INGEST_ROWS)]))
        rows2 = [{'code': f'60{i:04d}.SH', 'name': ('脱帽甲' if i == 10 else f'股票{i}')}
                 for i in range(MIN_INGEST_ROWS)]
        diff2 = self.table.ingest(self._make_df(rows2))
        self.assertIn('600010.SH', diff2['st_off'])

    def test_is_fresh(self):
        self.assertFalse(self.table.is_fresh())  # 空表
        self.table.ingest(self._big_df())
        self.assertTrue(self.table.is_fresh())
        self.table._updated_at = time.time() - 8 * 86400
        self.assertFalse(self.table.is_fresh(max_age_days=7))

    def test_to_dataframe_filters(self):
        rows = [{'code': f'60{i:04d}.SH', 'name': f'股票{i}'} for i in range(MIN_INGEST_ROWS)]
        rows.append({'code': '600100.SH', 'name': '*ST 风险'})
        rows.append({'code': '920819.BJ', 'name': '北交所股'})
        self.table.ingest(self._make_df(rows))
        df = self.table.to_dataframe()
        self.assertEqual(list(df.columns), ['code', 'name', 'industry'])
        self.assertNotIn('920819.BJ', set(df['code']))      # 默认排 BJ
        self.assertIn('600100.SH', set(df['code']))         # 默认含 ST
        df2 = self.table.to_dataframe(exclude_st=True)
        self.assertNotIn('600100.SH', set(df2['code']))
        self.assertEqual(len(df2), len(df) - 1)

    def test_query_helpers(self):
        self.table.ingest(self._make_df(
            [{'code': f'60{i:04d}.SH', 'name': ('*ST 甲' if i == 5 else f'股票{i}')}
             for i in range(MIN_INGEST_ROWS)]))
        self.assertEqual(self.table.get_name('600005.SH'), '*ST 甲')
        self.assertTrue(self.table.is_st('600005.SH'))
        self.assertFalse(self.table.is_st('600006.SH'))
        self.assertIsNone(self.table.get_name('999999.SH'))
        self.assertIn('600005.SH', self.table)
        self.assertEqual(self.table.get_board('600005.SH'), 'SH_MAIN')

    def test_refresh_frequency_gate(self):
        self.table.ingest(self._big_df())
        # refresh() 内部局部 from .sources.sina import SinaSource，
        # patch 目标为源模块属性
        with patch('quant_engine.data.sources.sina.SinaSource.get_all_stocks') as m:
            result = self.table.refresh()
            self.assertTrue(result['skipped'])
            m.assert_not_called()


class TestFormatDiff(unittest.TestCase):
    def test_skipped_returns_empty(self):
        self.assertEqual(format_stock_info_diff({'skipped': True}), '')

    def test_formats_changes(self):
        tbl = StockInfoTable(path='/tmp/test_stock_info_fmt.json')
        tbl._stocks = {'600001.SH': {'name': '*ST 甲', 'board': 'SH_MAIN', 'is_st': True}}
        text = format_stock_info_diff({'st_on': ['600001.SH'], 'added': [], 'removed': [],
                                       'st_off': [], 'renamed': []})
        self.assertIn('600001.SH', text)
        self.assertIn('戴帽ST', text)
        if Path('/tmp/test_stock_info_fmt.json').exists():
            Path('/tmp/test_stock_info_fmt.json').unlink()


class TestGetStockListLocalBuild(unittest.TestCase):
    """D49：静态表 fresh 时 get_stock_list 不打源"""

    def test_local_build_no_source_call(self):
        from quant_engine.data.repositories.market_data import MarketDataRepository

        repo = MarketDataRepository.__new__(MarketDataRepository)
        repo.cache = MagicMock()
        repo.cache.get.return_value = None   # Redis 未命中
        repo.source = MagicMock()
        repo._validation_errors = []
        repo._cache_lock = __import__('threading').Lock()
        repo._all_quotes_cache = None
        repo._all_quotes_cache_time = 0
        from quant_engine.data.inflight import InflightMerger
        repo._inflight = InflightMerger()

        rows = [{'code': f'60{i:04d}.SH', 'name': f'股票{i}'} for i in range(MIN_INGEST_ROWS)]
        with patch('quant_engine.data.repositories.market_data.get_stock_info_table') as mock_tbl:
            table = StockInfoTable(path='/tmp/test_stock_info_pool.json')
            table.ingest(pd.DataFrame(rows, columns=['code', 'name']))
            mock_tbl.return_value = table
            repo.get_stock_list()
            repo.source.get_stock_list.assert_not_called()
            repo.cache.set.assert_called_once()   # 静态表结果回填 Redis
            # BJ 过滤仍生效
            args = repo.cache.set.call_args[0]
            self.assertFalse(any(str(r['code']).endswith('.BJ') for r in args[1]))
        Path('/tmp/test_stock_info_pool.json').unlink(missing_ok=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
