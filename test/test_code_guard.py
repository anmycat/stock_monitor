"""CodeGuard 代码一致性校验服务测试

覆盖：
- collect_targets：HOT_ETFS + ETF持仓快照 + stock_list 三路聚合
- check_static：market_mismatch（自动修复脏缓存）/ missing /
  name_mismatch / invalid_format / bad_price 五类检出
- check_auction：price_gap 检出并清缓存、正常价格不误报
- 事故回归：510300.SZ（腾讯源旧 bug 产物）必须报 market_mismatch
- hot_etf 手写简称不触发 name_mismatch（降噪）
- check_and_push：全绿不推送、有问题才推送
- 报告持久化与读取
"""
import os
import sys
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from quant_engine.services.code_guard import CodeGuard, SEV_ERROR, SEV_WARNING
from quant_engine.services.monitors.etf_monitor import HOT_ETFS


class FakeCache:
    """内存 Redis 替身（get/set/delete/exists/keys）"""

    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, ttl=None):
        self._data[key] = value
        return True

    def delete(self, key):
        return self._data.pop(key, None) is not None

    def exists(self, key):
        return key in self._data

    def keys(self, pattern):
        import fnmatch
        return [k for k in self._data if fnmatch.fnmatch(k, pattern)]


class FakeDataCenter:
    def __init__(self):
        self.cache = FakeCache()


def _quote_df(rows):
    """构造腾讯行情返回的 DataFrame"""
    return pd.DataFrame(rows, columns=['code', 'name', 'price', 'change_pct', 'volume'])


# 权威行情样本（与真实接口口径一致）
AUTH_ROWS = [
    ('510300.SH', '沪深300ETF华泰柏瑞', 4.236, -0.14, 37995085),
    ('518880.SH', '黄金ETF华安', 8.554, 0.97, 1000000),
    ('000001.SZ', '平安银行', 11.5, 0.79, 1111851),
    ('920819.BJ', '颖泰生物', 2.99, 1.7, 47999),
    ('600519.SH', '贵州茅台', 1500.0, 0.5, 20000),
    ('300750.SZ', '宁德时代', 33.0, 0.5, 100000),
]


class TestCollectTargets(unittest.TestCase):
    """目标收集：三路聚合"""

    def setUp(self):
        self.dc = FakeDataCenter()
        self.guard = CodeGuard(self.dc)

    def test_hot_etfs_included(self):
        targets = self.guard.collect_targets()
        for etf in HOT_ETFS:
            self.assertIn(etf['code'], targets)
        self.assertEqual(targets['510300.SH']['source'], 'hot_etf')

    def test_holdings_snapshot_codes_included(self):
        """ETF 持仓快照成分股纳入（latest -> snapshot 两跳读取）"""
        self.dc.cache.set('etf_holdings:latest:510300', {
            'etf_code': '510300.SH', 'quarter': '2026Q2'})
        self.dc.cache.set('etf_holdings:510300:2026Q2', [
            {'code': '600519.SH', 'name': '贵州茅台'},
            {'code': '300750.SZ', 'name': '宁德时代'},
            {'code': '920819.BJ', 'name': '颖泰生物'},
        ])
        targets = self.guard.collect_targets()
        self.assertEqual(targets['600519.SH']['source'], 'etf_holdings')
        self.assertEqual(targets['300750.SZ']['name'], '宁德时代')

    def test_stock_list_cache_included(self):
        self.dc.cache.set('stock_list', [
            {'code': '600519.SH', 'name': '贵州茅台'},
            {'code': '300750.SZ', 'name': '宁德时代'},
        ])
        targets = self.guard.collect_targets()
        self.assertEqual(targets['600519.SH']['source'], 'stock_list')

    def test_invalid_codes_filtered(self):
        """脏数据（非法码）不进入目标集"""
        self.dc.cache.set('stock_list', [
            {'code': 'ABC', 'name': '脏数据'},
            {'code': '12345', 'name': '短码'},
            {'code': '600519.SH', 'name': '贵州茅台'},
        ])
        targets = self.guard.collect_targets()
        self.assertNotIn('ABC', targets)
        self.assertNotIn('12345', targets)


class TestCheckStatic(unittest.TestCase):
    """盘前静态校验"""

    def setUp(self):
        self.dc = FakeDataCenter()
        self.guard = CodeGuard(self.dc)

    def _run_with_targets(self, targets, auth_rows=AUTH_ROWS):
        """指定目标全集 + mock 权威行情，跑静态校验"""
        self.guard.collect_targets = lambda: targets
        with patch.object(self.guard.tencent, 'get_realtime_quote',
                          return_value=_quote_df(auth_rows)):
            return self.guard.check_static()

    def test_market_mismatch_detected_and_repaired(self):
        """事故回归：510300.SZ 错码 → error + 自动修复（删错键写对键）"""
        # 预置脏缓存：历史 bug 残留的错误键
        self.dc.cache.set('quote:510300.SZ',
                          {'code': '510300.SZ', 'name': '沪深300ETF', 'price': 4.0})
        report = self._run_with_targets({
            '510300.SZ': {'name': '沪深300ETF', 'source': 'legacy_bug'},
            '000001.SZ': {'name': '平安银行', 'source': 'etf_holdings'},
        })
        mismatches = [i for i in report['issues'] if i['type'] == 'market_mismatch']
        self.assertEqual(len(mismatches), 1)
        issue = mismatches[0]
        self.assertEqual(issue['severity'], SEV_ERROR)
        self.assertEqual(issue['code'], '510300.SZ')
        self.assertEqual(issue['detail']['correct_code'], '510300.SH')
        # 自动修复：错键已删、正确键已写
        self.assertTrue(issue['repaired'])
        self.assertFalse(self.dc.cache.exists('quote:510300.SZ'))
        self.assertTrue(self.dc.cache.exists('quote:510300.SH'))

    def test_bj_mismatch_detected(self):
        """北交所错判：920819.SH → 920819.BJ"""
        report = self._run_with_targets({
            '920819.SH': {'name': '颖泰生物', 'source': 'legacy_bug'},
            '000001.SZ': {'name': '平安银行', 'source': 'etf_holdings'},
        })
        mismatches = [i for i in report['issues'] if i['type'] == 'market_mismatch']
        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0]['detail']['correct_code'], '920819.BJ')

    def test_missing_detected(self):
        """幽灵代码：权威源查无 → warning"""
        report = self._run_with_targets({
            '999999.SH': {'name': '幽灵股票', 'source': 'legacy_bug'},
            '000001.SZ': {'name': '平安银行', 'source': 'etf_holdings'},
        })
        missing = [i for i in report['issues'] if i['type'] == 'missing']
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]['severity'], SEV_WARNING)
        self.assertEqual(missing[0]['code'], '999999.SH')

    def test_name_mismatch_detected_for_holdings(self):
        """etf_holdings 来源名称变更检出（ST/改名）"""
        report = self._run_with_targets(
            {'000001.SZ': {'name': '平安银行', 'source': 'etf_holdings'}})
        # AUTH_ROWS 中 000001.SZ 名为平安银行 → 一致，不报
        self.assertEqual([i for i in report['issues']
                          if i['type'] == 'name_mismatch'], [])
        # 改名场景
        report2 = self._run_with_targets(
            {'600519.SH': {'name': '国酒茅台', 'source': 'etf_holdings'}})
        nm = [i for i in report2['issues'] if i['type'] == 'name_mismatch']
        self.assertEqual(len(nm), 1)
        self.assertEqual(nm[0]['detail']['auth_name'], '贵州茅台')

    def test_hot_etf_short_name_no_false_positive(self):
        """hot_etf 手写简称（新能源ETF vs 新能源车ETF华夏）不误报"""
        rows = AUTH_ROWS + [('515030.SH', '新能源车ETF华夏', 1.5, 0.3, 1000)]
        report = self._run_with_targets(
            {'515030.SH': {'name': '新能源ETF', 'source': 'hot_etf'}},
            auth_rows=rows)
        self.assertEqual([i for i in report['issues']
                          if i['type'] == 'name_mismatch'], [])

    def test_bad_price_detected(self):
        rows = [('510300.SH', '沪深300ETF华泰柏瑞', 0, -0.14, 0)] + AUTH_ROWS[1:]
        report = self._run_with_targets(
            {'510300.SH': {'name': '沪深300ETF', 'source': 'hot_etf'}},
            auth_rows=rows)
        bad = [i for i in report['issues'] if i['type'] == 'bad_price']
        self.assertEqual(len(bad), 1)

    def test_all_ok_report(self):
        """全部正常：0 issue"""
        targets = {r[0]: {'name': r[1], 'source': 'etf_holdings'} for r in AUTH_ROWS}
        report = self._run_with_targets(targets)
        self.assertEqual(report['issue_count'], 0)
        self.assertEqual(report['error_count'], 0)
        self.assertEqual(report['checked'], len(AUTH_ROWS))

    def test_report_persisted(self):
        self._run_with_targets({
            '999999.SH': {'name': '幽灵股票', 'source': 'legacy_bug'},
            '000001.SZ': {'name': '平安银行', 'source': 'etf_holdings'},
        })
        saved = self.dc.cache.get('code_guard:report')
        self.assertIsNotNone(saved)
        self.assertEqual(saved['mode'], 'static')
        self.assertGreaterEqual(saved['issue_count'], 1)


class TestCheckAuction(unittest.TestCase):
    """集合竞价缓存校验"""

    def setUp(self):
        self.dc = FakeDataCenter()
        self.guard = CodeGuard(self.dc)

    def _run(self, auth_rows=AUTH_ROWS):
        original = self.guard.collect_targets
        self.guard.collect_targets = lambda: {r[0]: {'name': r[1], 'source': 'etf_holdings'}
                                              for r in auth_rows}
        with patch.object(self.guard.tencent, 'get_realtime_quote',
                          return_value=_quote_df(auth_rows)):
            return self.guard.check_auction()

    def test_price_gap_detected_and_cache_cleared(self):
        """缓存价与竞价价跳空 30% → error + 清缓存（防串号）"""
        self.dc.cache.set('quote:000001.SZ',
                          {'code': '000001.SZ', 'name': '平安银行', 'price': 8.0})
        report = self._run()
        gaps = [i for i in report['issues'] if i['type'] == 'price_gap']
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]['severity'], SEV_ERROR)
        self.assertTrue(gaps[0]['repaired'])
        self.assertFalse(self.dc.cache.exists('quote:000001.SZ'))

    def test_normal_price_no_false_positive(self):
        """缓存价与竞价价接近（±3%）→ 不报"""
        self.dc.cache.set('quote:000001.SZ',
                          {'code': '000001.SZ', 'name': '平安银行', 'price': 11.2})
        self.dc.cache.set('quote:600519.SH',
                          {'code': '600519.SH', 'name': '贵州茅台', 'price': 1520.0})
        report = self._run()
        self.assertEqual(report['issue_count'], 0)

    def test_limit_up_gap_not_flagged(self):
        """涨停 10% 属正常波动，不应误报（阈值 11% 缓冲）"""
        self.dc.cache.set('quote:000001.SZ',
                          {'code': '000001.SZ', 'name': '平安银行', 'price': 10.45})
        # 竞价价 11.5 vs 缓存 10.45 → +10.05% < 11%
        report = self._run()
        self.assertEqual([i for i in report['issues']
                          if i['type'] == 'price_gap'], [])

    def test_no_cached_quote_skipped(self):
        """无缓存价 → 跳过连续性校验（首次抓取）"""
        report = self._run()
        self.assertEqual(report['issue_count'], 0)


class TestHelpers(unittest.TestCase):
    """辅助方法"""

    def setUp(self):
        self.guard = CodeGuard()

    def test_match_authoritative_exact(self):
        auth = {'510300.SH': {'market': 'SH', 'name': 'x', 'price': 1}}
        self.assertIsNotNone(self.guard._match_authoritative('510300.SH', auth))

    def test_match_authoritative_by_key(self):
        """本地码后缀错时按 6 位 key 匹配（market_mismatch 判定基础）"""
        auth = {'510300.SH': {'market': 'SH', 'name': 'x', 'price': 1}}
        matched = self.guard._match_authoritative('510300.SZ', auth)
        self.assertIsNotNone(matched)
        self.assertEqual(matched[0], '510300.SH')

    def test_match_authoritative_none(self):
        self.assertIsNone(self.guard._match_authoritative('999999.SH', {}))

    def test_normalize_name_strips_st(self):
        f = CodeGuard._normalize_name
        self.assertEqual(f('ST平安银行'), '平安银行')
        self.assertEqual(f('*ST 明诚'), '明诚')
        self.assertEqual(f('平安银行'), '平安银行')
        self.assertEqual(f('XD平安银行'), '平安银行')
        self.assertEqual(f('N国联'), '国联')
        self.assertEqual(f('DR华菱'), '华菱')

    def test_check_name_substring_ok(self):
        """简称互为子串 → 一致"""
        self.assertIsNone(self.guard._check_name(
            '510300.SH', '沪深300ETF', '沪深300ETF华泰柏瑞'))

    def test_check_name_different_reports(self):
        issue = self.guard._check_name('600519.SH', '国酒茅台', '贵州茅台')
        self.assertIsNotNone(issue)
        self.assertEqual(issue['type'], 'name_mismatch')

    def test_fetch_authoritative_batching(self):
        """超过 BATCH_SIZE 的目标分批拉取"""
        codes = [f'{600000 + i}.SH' for i in range(120)]
        calls = []

        def fake_quote(batch):
            calls.append(list(batch))
            return _quote_df([(c, 'name', 1.0, 0, 1) for c in batch])
        with patch.object(self.guard.tencent, 'get_realtime_quote',
                          side_effect=fake_quote):
            result = self.guard.fetch_authoritative(codes)
        self.assertEqual(len(calls), 3)          # 120/50 → 3 批
        self.assertEqual(len(result), 120)


class TestPushAndReport(unittest.TestCase):
    """推送与报告"""

    def setUp(self):
        self.dc = FakeDataCenter()
        self.guard = CodeGuard(self.dc)
        self.notifier = MagicMock()

    def test_all_ok_no_push(self):
        original = self.guard.collect_targets
        self.guard.collect_targets = lambda: {
            '000001.SZ': {'name': '平安银行', 'source': 'etf_holdings'}}
        with patch.object(self.guard.tencent, 'get_realtime_quote',
                          return_value=_quote_df(AUTH_ROWS)):
            report = self.guard.check_and_push(self.notifier, mode='static')
        self.assertIsNone(report)  # 全绿返回 None
        self.notifier.send_custom_message.assert_not_called()

    def test_issues_trigger_push(self):
        original = self.guard.collect_targets
        self.guard.collect_targets = lambda: {
            '510300.SZ': {'name': '沪深300ETF', 'source': 'legacy_bug'}}
        with patch.object(self.guard.tencent, 'get_realtime_quote',
                          return_value=_quote_df(AUTH_ROWS)):
            report = self.guard.check_and_push(self.notifier, mode='static')
        self.assertIsNotNone(report)
        self.notifier.send_custom_message.assert_called_once()
        title, msg = self.notifier.send_custom_message.call_args[0]
        self.assertIn('代码一致性', title)
        self.assertIn('市场标记错判', msg)

    def test_format_report_message_structure(self):
        report = {
            'mode': 'auction', 'checked': 100, 'quoted': 98,
            'issue_count': 2, 'error_count': 1, 'warning_count': 1,
            'by_type': {'market_mismatch': 1, 'missing': 1},
            'issues': [
                {'severity': SEV_ERROR, 'type': 'market_mismatch',
                 'code': '510300.SZ', 'name': '', 'message': '市场标记错误',
                 'detail': {}, 'repaired': True},
                {'severity': SEV_WARNING, 'type': 'missing',
                 'code': '999999.SH', 'name': '', 'message': '查无此码',
                 'detail': {}},
            ],
            'started_at': datetime.now().isoformat(), 'duration_sec': 1.2,
        }
        title, msg = CodeGuard.format_report_message(report)
        self.assertIn('集合竞价校验', title)
        self.assertIn('🔴', msg)
        self.assertIn('🟡', msg)
        self.assertIn('已自动修复', msg)

    def test_get_report_from_cache(self):
        self.dc.cache.set('code_guard:report', {'mode': 'static', 'issue_count': 0})
        report = self.guard.get_report()
        self.assertEqual(report['mode'], 'static')


if __name__ == '__main__':
    unittest.main()
