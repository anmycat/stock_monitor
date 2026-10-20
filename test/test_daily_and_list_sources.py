"""新浪/腾讯取数能力扩展测试（2026-10-20 数据源重组）

背景：东财出口 IP 经常被风控（ECS 实测 22 连败），按用户决策降为备路；
腾讯第一、新浪第二互为配合。同时修复两条断裂的数据链路：
- stock_list：原仅 akshare 一条路且未安装 → 退化为 5 只兜底
- daily：原仅 akshare 一条路且未安装 → signals 全空

新增能力（均为真实 API 实测后落地）：
- SinaSource.get_all_stocks / get_stock_list（hs_a 全市场含北交所，
  实测 5574 只）
- SinaSource.get_daily_data（getKLineData）
- TencentSource.get_daily_data（fqkline 前复权，volume 手→股，
  北交所 day 键兼容）
- _PRIORITY 重组：tencent/sina 一线、eastmoney 备路、akshare 移出主路
- etf_monitor 弃 akshare 直调改实时路；burst 非交易时间跳过刷新
"""
import json
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from quant_engine.data.sources.sina import SinaSource
from quant_engine.data.sources.tencent import TencentSource

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')


def _load_text(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as f:
        return f.read()


def _mock_json(payload, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    text = payload if isinstance(payload, str) else json.dumps(payload)
    resp.text = text
    # 腾讯源走 response.json()，需单独桩；空 body（错误路径）不桩
    if text.strip():
        resp.json.return_value = json.loads(text)
    return resp


class TestSinaList(unittest.TestCase):
    """新浪全市场列表/快照（第二线数据源）"""

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_get_all_stocks_parses_bj_prefix(self, mock_get):
        """北交所 bj 前缀 → .BJ 后缀（历史 bug：else 一律判 SZ）"""
        mock_get.side_effect = [
            _mock_json(_load_text('sina_list_page1.json')),
            _mock_json('[]'),  # 第二页空 → 终止
        ]
        df = SinaSource.get_all_stocks()
        self.assertEqual(len(df), 5)
        self.assertIn('920000.BJ', set(df['code']))
        row = df[df['code'] == '920000.BJ'].iloc[0]
        self.assertEqual(row['name'], '安徽凤凰')
        self.assertAlmostEqual(row['price'], 13.45, places=2)
        self.assertAlmostEqual(row['change_pct'], 1.894, places=3)

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_get_all_stocks_pagination_stops_on_short_page(self, mock_get):
        """不满页即终止（fixture 5 条 < 100）"""
        mock_get.side_effect = [
            _mock_json(_load_text('sina_list_page1.json')),
        ]
        df = SinaSource.get_all_stocks()
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(len(df), 5)

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_get_stock_list_schema(self, mock_get):
        """stock_list 输出 code/name/industry 三列（industry 新浪无源留空）"""
        mock_get.side_effect = [
            _mock_json(_load_text('sina_list_page1.json')),
        ]
        df = SinaSource.get_stock_list()
        self.assertEqual(list(df.columns), ['code', 'name', 'industry'])
        self.assertEqual(len(df), 5)
        self.assertTrue((df['industry'] == '').all())

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_get_all_stocks_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_json('', status_code=503)
        df = SinaSource.get_all_stocks()
        self.assertTrue(df.empty)


class TestSinaDaily(unittest.TestCase):
    """新浪日线（getKLineData）"""

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_get_daily_data_columns_and_sort(self, mock_get):
        """列结构 date/OHLC/volume，按日期升序，close>0 过滤"""
        mock_get.return_value = _mock_json(_load_text('sina_daily_600000.json'))
        df = SinaSource.get_daily_data('600000.SH', 3)
        self.assertEqual(list(df.columns),
                         ['date', 'open', 'high', 'low', 'close', 'volume'])
        self.assertEqual(len(df), 3)
        self.assertListEqual(df['date'].tolist(), sorted(df['date'].tolist()))
        self.assertAlmostEqual(df.iloc[-1]['close'], 9.78, places=2)

    @patch('quant_engine.data.sources.sina.requests.get')
    def test_get_daily_data_uses_sina_code_format(self, mock_get):
        """请求参数 symbol=sh600000（_convert_code 转换）"""
        mock_get.return_value = _mock_json(_load_text('sina_daily_600000.json'))
        SinaSource.get_daily_data('600000.SH', 120)
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params']['symbol'], 'sh600000')
        self.assertEqual(kwargs['params']['scale'], 240)


class TestTencentDaily(unittest.TestCase):
    """腾讯日线（fqkline）——修复日线链路断裂"""

    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_qfqday_field_order_and_volume_unit(self, mock_get):
        """qfqday 数组顺序 [day,open,close,high,low,volume]；volume 手→股"""
        mock_get.return_value = _mock_json(_load_text('tencent_daily_sh600000.json'))
        df = TencentSource.get_daily_data('600000.SH', 3)
        self.assertEqual(len(df), 4)  # fixture 实拉 4 行（含当日）
        row = df.iloc[-1]
        self.assertEqual(row['date'], '2026-10-20')
        self.assertAlmostEqual(row['open'], 9.79, places=2)
        self.assertAlmostEqual(row['close'], 9.74, places=2)
        self.assertAlmostEqual(row['high'], 9.86, places=2)
        self.assertAlmostEqual(row['low'], 9.68, places=2)
        # 318280 手 ×100 = 31828000 股
        self.assertAlmostEqual(row['volume'], 31828000.0, places=1)

    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_bj_uses_day_key(self, mock_get):
        """北交所返回 day 键而非 qfqday，需兼容"""
        mock_get.return_value = _mock_json(_load_text('tencent_daily_bj920819.json'))
        df = TencentSource.get_daily_data('920819.BJ', 3)
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.iloc[0]['close'], 3.06, places=2)
        self.assertAlmostEqual(df.iloc[0]['volume'], 3613900.0, places=1)

    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_empty_bars_returns_empty_df(self, mock_get):
        mock_get.return_value = _mock_json({'code': 0, 'data': {}})
        df = TencentSource.get_daily_data('600000.SH', 120)
        self.assertTrue(df.empty)

    @patch('quant_engine.data.sources.tencent.requests.get')
    def test_http_error_returns_empty_df(self, mock_get):
        mock_get.return_value = _mock_json('', status_code=502)
        df = TencentSource.get_daily_data('600000.SH', 120)
        self.assertTrue(df.empty)


class TestPriorityReorg(unittest.TestCase):
    """_PRIORITY 重组：腾讯/新浪一线、东财备路、akshare 移出主路"""

    def test_priority_matrix(self):
        from quant_engine.data.source import _PRIORITY
        # 实时：腾讯第一、新浪第二、东财备路
        self.assertEqual(_PRIORITY['realtime'][0], 'tencent')
        self.assertEqual(_PRIORITY['realtime'][1], 'sina')
        self.assertEqual(_PRIORITY['realtime'][2], 'eastmoney')
        # 列表：新浪第一（实测 5574 只含北交所）、东财备路
        self.assertEqual(_PRIORITY['stock_list'][0], 'sina')
        self.assertIn('eastmoney', _PRIORITY['stock_list'])
        # 日线：腾讯第一、新浪第二、东财备路
        self.assertEqual(_PRIORITY['daily'][0], 'tencent')
        self.assertEqual(_PRIORITY['daily'][1], 'sina')
        # akshare 未安装，移出全部主路
        for key in ('stock_list', 'realtime', 'daily'):
            self.assertNotIn('akshare', _PRIORITY[key],
                             f'akshare 未安装不应在 {key} 主路')
        # etf 死路已删（唯一消费方 etf_monitor 已改实时路）
        self.assertNotIn('etf', _PRIORITY)

    def test_composite_has_no_etf_quotes_dead_path(self):
        """CompositeSource.get_etf_quotes 死路已删除"""
        from quant_engine.data.source import CompositeSource
        self.assertFalse(hasattr(CompositeSource, 'get_etf_quotes'))


class TestConsumersReorg(unittest.TestCase):
    """消费方改造：etf_monitor 弃 akshare 直调；burst 交易时间门控"""

    def test_etf_monitor_no_akshare_dependency(self):
        import inspect
        from quant_engine.services import etf_monitor
        src = inspect.getsource(etf_monitor)
        # 不得 import akshare 源、不得持有 self.akshare 实例
        self.assertNotIn('from ..data.akshare_source', src)
        self.assertNotIn('self.akshare', src)

    def test_etf_monitor_uses_realtime_quote(self):
        """ETF 状态按 target_codes 直查实时路（不再拉全市场过滤）"""
        from quant_engine.services.etf_monitor import ETFMonitor
        dc = MagicMock()
        dc.get_realtime_quote.return_value = pd.DataFrame([
            {'code': '510300.SH', 'name': '沪深300ETF', 'price': 4.23,
             'change_pct': 0.5, 'volume': 1000.0},
        ])
        monitor = ETFMonitor(dc)
        rows = monitor.get_etf_status()
        called_codes = dc.get_realtime_quote.call_args[0][0]
        self.assertIn('510300.SH', called_codes)
        self.assertEqual(len(called_codes), 14)
        got = [r for r in rows if r['code'] == '510300.SH'][0]
        self.assertFalse(got.get('no_data'))
        self.assertAlmostEqual(got['price'], 4.23, places=2)

    def test_burst_skips_refresh_outside_trading_hours(self):
        """非交易时间不调源（用户决策：只在交易时间取数，新闻除外）"""
        from quant_engine.services.burst_monitor import BurstMonitor
        dc = MagicMock()
        monitor = BurstMonitor(dc)
        monitor.active_pool = ['600000.SH']
        with patch('quant_engine.services.burst_monitor.is_trading_time',
                   return_value=False):
            pool = monitor.refresh_active_pool()
        dc.get_all_realtime_quotes.assert_not_called()
        self.assertEqual(pool, ['600000.SH'])

    def test_burst_refreshes_during_trading_hours(self):
        """交易时间正常刷新"""
        from quant_engine.services.burst_monitor import BurstMonitor
        dc = MagicMock()
        dc.get_all_realtime_quotes.return_value = pd.DataFrame([
            {'code': '600000.SH', 'name': '浦发银行', 'price': 9.8,
             'change_pct': 1.0, 'volume': 1000.0},
        ])
        monitor = BurstMonitor(dc)
        with patch('quant_engine.services.burst_monitor.is_trading_time',
                   return_value=True):
            pool = monitor.refresh_active_pool()
        dc.get_all_realtime_quotes.assert_called_once()
        self.assertEqual(pool, ['600000.SH'])


if __name__ == '__main__':
    unittest.main()
