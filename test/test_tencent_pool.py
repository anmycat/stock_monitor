"""M2' 回归：腾讯 realtime 池动态化（废弃硬编码号段主路）

背景（12 §3.14 C1 + 07 §15 M2'）：TencentSource.get_all_stocks 曾走
_get_main_stock_codes 硬编码号段（~406 只，缺科创/ETF/北交所/深市后段）
且 min(len(codes), 500) 截断；本地直调曾返 0 只/52s。
修复（2026-10-21）：三层代码池——新浪 hs_a 列表主路 / 东财 clist 备路 /
静态号段 emergency-only 兜底；报价复用 get_realtime_quote 分批 200。
本地实测：新浪列表 5575 只 → 腾讯报价 100% 覆盖（SZ 2906/SH 2317/BJ 352），
全链约 26s；/api/market total ≥ 5000 验收口径由此达成。
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from quant_engine.data.sources.tencent import TencentSource


def _fake_line(prefix: str, num: str) -> str:
    """构造腾讯行情行（52 个 ~ 字段，位置对齐真实响应：
    [1]=名称 [2]=代码 [3]=现价 [6]=成交量 [32]=涨跌幅）"""
    f = ['1', f'股票{num}', num, '10.00', '10.10', '10.20', '1000', '500', '500', '10.00']
    f += ['10.00', '100'] * 5                      # 10..19 五档
    f += ['10.00', '100'] * 4                      # 20..27
    f += ['10.00', '']                             # 28, 29
    f += ['20261021161448', '0.10', '1.01']        # 30..32
    f += ['10.10', '9.90', '10.00/1000/10000', '1000', '100', '0.10', '5.00', '']  # 33..40
    f += ['10.10', '9.90', '1.00', '100.0', '100.0', '0.10',
          '10.50', '8.00', '0.10', '100', '10.00']  # 41..51
    return f'v_{prefix}="' + '~'.join(f) + '"'


def _resp_for(url: str) -> MagicMock:
    codes = url.split('=', 1)[1].split(',')
    resp = MagicMock()
    resp.status_code = 200
    resp.text = '\n'.join(_fake_line(c, c[2:]) for c in codes)
    return resp


def _sina_pool(n: int) -> pd.DataFrame:
    """多市场代码池（沪/深/北三分）"""
    third = n // 3
    codes = ([f'{i:06d}.SH' for i in range(600000, 600000 + third)] +
             [f'{i:06d}.SZ' for i in range(1, third + 1)] +
             [f'92{i:04d}.BJ' for i in range(8000, 8000 + third)])
    return pd.DataFrame({'code': codes, 'name': ['x'] * len(codes),
                         'industry': [''] * len(codes)})


class TencentPoolTests(unittest.TestCase):
    """M2' 三层代码池行为冻结"""

    def test_static_segments_still_available(self):
        """静态号段保留作 emergency-only（行为冻结）"""
        codes = TencentSource._get_main_stock_codes()
        self.assertGreater(len(codes), 1000)
        self.assertTrue(all(c.endswith(('.SH', '.SZ')) for c in codes))

    @patch('quant_engine.data.sources.sina.SinaSource.get_stock_list')
    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_sina_pool_primary_full_coverage(self, mock_get, mock_list):
        """主路：新浪全市场列表 → 腾讯分批报价，全覆盖 + 多市场后缀正确"""
        pool = _sina_pool(1200)
        mock_list.return_value = pool
        mock_get.side_effect = lambda url, headers=None, timeout=None: _resp_for(url)

        df = TencentSource.get_all_stocks()

        self.assertEqual(len(df), len(pool))
        self.assertEqual(set(df['code']), set(pool['code']))
        self.assertEqual(mock_get.call_count, 6)  # 1200/200
        # 市场后缀不串（BJ 不得判成 SH/SZ）
        by_suffix = df['code'].str[-2:].value_counts().to_dict()
        self.assertEqual(by_suffix.get('BJ'), 400)
        self.assertEqual(by_suffix.get('SH'), 400)
        self.assertEqual(by_suffix.get('SZ'), 400)
        row = df[df['code'] == '600000.SH'].iloc[0]
        self.assertAlmostEqual(row['price'], 10.00)
        self.assertAlmostEqual(row['change_pct'], 1.01)
        self.assertAlmostEqual(row['volume'], 1000.0)

    @patch('quant_engine.data.sources.eastmoney.EastMoneySource.get_all_stocks')
    @patch('quant_engine.data.sources.sina.SinaSource.get_stock_list')
    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_eastmoney_fallback_when_sina_small(self, mock_get, mock_sina, mock_em):
        """备路：新浪池过小（<MIN_POOL_CODES）→ 东财 clist 池接管"""
        mock_sina.return_value = _sina_pool(500)
        em_pool = _sina_pool(1500)
        mock_em.return_value = em_pool
        mock_get.side_effect = lambda url, headers=None, timeout=None: _resp_for(url)

        df = TencentSource.get_all_stocks()

        self.assertTrue(mock_em.called)
        self.assertEqual(len(df), len(em_pool))

    @patch('quant_engine.data.sources.eastmoney.EastMoneySource.get_all_stocks')
    @patch('quant_engine.data.sources.sina.SinaSource.get_stock_list')
    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_small_dynamic_pools_rejected(self, mock_get, mock_sina, mock_em):
        """C1 病灶冻结：动态池过小不得被采信（防覆盖数随应答源波动）"""
        mock_sina.return_value = pd.DataFrame()
        mock_em.return_value = _sina_pool(100)  # 代理截断曾返 100 行
        mock_get.side_effect = lambda url, headers=None, timeout=None: _resp_for(url)

        df = TencentSource.get_all_stocks()

        # 两个动态池都低于门槛 → 走静态 emergency-only
        self.assertEqual(len(df), len(TencentSource._get_main_stock_codes()))

    @patch('quant_engine.data.sources.eastmoney.EastMoneySource.get_all_stocks')
    @patch('quant_engine.data.sources.sina.SinaSource.get_stock_list')
    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_static_emergency_when_dynamic_sources_raise(self, mock_get, mock_sina, mock_em):
        """兜底：两个动态源都抛异常 → 静态号段应急（不返回空）"""
        mock_sina.side_effect = ConnectionError('sina down')
        mock_em.side_effect = ConnectionError('em down')
        mock_get.side_effect = lambda url, headers=None, timeout=None: _resp_for(url)

        df = TencentSource.get_all_stocks()

        self.assertFalse(df.empty)
        self.assertEqual(len(df), len(TencentSource._get_main_stock_codes()))

    @patch('quant_engine.data.sources.sina.SinaSource.get_stock_list')
    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_pool_capped_at_max(self, mock_get, mock_list):
        """运行时可上限 MAX_POOL_CODES 封顶（防耗时失控）"""
        mock_list.return_value = _sina_pool(7000)
        mock_get.side_effect = lambda url, headers=None, timeout=None: _resp_for(url)

        df = TencentSource.get_all_stocks()

        self.assertEqual(len(df), TencentSource.MAX_POOL_CODES)
        self.assertEqual(mock_get.call_count,
                         TencentSource.MAX_POOL_CODES // TencentSource.BATCH_SIZE)

    @patch('quant_engine.data.sources.sina.SinaSource.get_stock_list')
    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_batch_failure_tolerated(self, mock_get, mock_list):
        """单批失败不炸全链（缺该批，其余批次照常）"""
        pool = _sina_pool(1200)
        mock_list.return_value = pool
        state = {'n': 0}

        def flaky(url, headers=None, timeout=None):
            state['n'] += 1
            if state['n'] == 2:
                raise ConnectionError('batch boom')
            return _resp_for(url)

        mock_get.side_effect = flaky
        df = TencentSource.get_all_stocks()
        self.assertEqual(len(df), len(pool) - TencentSource.BATCH_SIZE)

    @patch('quant_engine.data.sources.sina.SinaSource.get_stock_list')
    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_duplicate_codes_deduped(self, mock_get, mock_list):
        """池源重复代码去重（防御性）"""
        pool = _sina_pool(1200)
        dup = pd.concat([pool, pool.head(200)], ignore_index=True)
        mock_list.return_value = dup
        mock_get.side_effect = lambda url, headers=None, timeout=None: _resp_for(url)

        df = TencentSource.get_all_stocks()
        self.assertEqual(len(df), 1200)


if __name__ == '__main__':
    unittest.main(verbosity=2)
