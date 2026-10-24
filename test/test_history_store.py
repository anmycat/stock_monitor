"""SQLite 持久层测试（docs/25 §5.1，2026-10-25）

覆盖：四表 CRUD / 信号 upsert 幂等 / 历史过滤 / 评估快照回退形状 /
保留期清理 / 全方法优雅降级（坏路径不抛异常）。
fixture 隔离：临时目录 + 显式 db_path，不打网络、不碰仓库 data/。
"""
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.data.store import HistoryStore


def _signal_row(code='000001.SZ', strategy='dip_reversal_v1', action='BUY',
                score=72.5, day='2026-10-23', created='2026-10-23T10:30:00',
                price=10.5):
    return {
        'signal_date': day,
        'created_at': created,
        'code': code,
        'name': '平安银行',
        'action': action,
        'level': 'A',
        'score': score,
        'strategy': strategy,
        'price': price,
        'position': 0.15,
        'target_price': 11.2,
        'stop_loss': 9.8,
        'capital_flow_5d': 1.2e8,
        'kelly_position': 0.12,
        'kelly_confidence': 68.0,
        'kelly_source': 'dynamic',
        'market_type': 'neutral',
        'reason': ['连涨2日', '量比1.8'],
    }


class HistoryStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = HistoryStore(db_path=str(Path(self.tmp.name) / 'history.db'))
        self.assertTrue(self.store.enabled, '临时库应初始化成功')

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    # ---- 信号 ----
    def test_save_signals_upsert_idempotent(self):
        """同日同代码同策略重复写只留一行且更新字段（盘中多轮语义）"""
        n1 = self.store.save_signals([_signal_row(score=70.0)])
        n2 = self.store.save_signals([_signal_row(score=75.0, price=10.9)])
        self.assertEqual((n1, n2), (1, 1))
        rows = self.store.get_signal_history(limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['score'], 75.0)
        self.assertEqual(rows[0]['price'], 10.9)
        self.assertEqual(rows[0]['reason'], ['连涨2日', '量比1.8'])
        self.assertEqual(rows[0]['capital_flow_5d'], 1.2e8)

    def test_save_signals_distinct_strategy_kept(self):
        """同代码不同策略是两行（UNIQUE 含 strategy）"""
        self.store.save_signals([
            _signal_row(strategy='dip_reversal_v1'),
            _signal_row(strategy='trend_breakout_v1'),
        ])
        self.assertEqual(len(self.store.get_signal_history()), 2)

    def test_signal_history_filters(self):
        self.store.save_signals([
            _signal_row(code='000001.SZ', day='2026-10-21'),
            _signal_row(code='600000.SH', action='WATCH', day='2026-10-22'),
            _signal_row(code='000001.SZ', day='2026-10-23', score=80.0),
        ])
        by_code = self.store.get_signal_history(code='000001.SZ')
        self.assertEqual(len(by_code), 2)
        by_action = self.store.get_signal_history(action='WATCH')
        self.assertEqual([r['code'] for r in by_action], ['600000.SH'])
        by_range = self.store.get_signal_history(date_from='2026-10-22', date_to='2026-10-22')
        self.assertEqual([r['code'] for r in by_range], ['600000.SH'])
        # 倒序：最新日期在前
        default = self.store.get_signal_history()
        self.assertEqual(default[0]['signal_date'], '2026-10-23')
        # limit 生效
        self.assertEqual(len(self.store.get_signal_history(limit=1)), 1)

    def test_save_signals_empty(self):
        self.assertEqual(self.store.save_signals([]), 0)

    # ---- 后验评估 ----
    def test_eval_run_roundtrip_and_list(self):
        signals = [{'code': '000001.SZ', 'name': '平安银行', 'price': 10.5,
                    'timestamp': '2026-10-23T15:05:00', 'action': 'BUY'}]
        self.assertTrue(self.store.save_eval_run(
            'run_20261023', '2026-10-23T18:15:00', 'neutral', 55.0, signals))
        run = self.store.get_eval_run('run_20261023')
        self.assertIsNotNone(run)
        self.assertEqual(run['market_state'], {'type': 'neutral', 'score': 55.0})
        self.assertEqual(run['signals'][0]['code'], '000001.SZ')
        # upsert 同 run_id
        self.store.save_eval_run(
            'run_20261023', '2026-10-23T18:20:00', 'bear', 30.0, signals + signals)
        run2 = self.store.get_eval_run('run_20261023')
        self.assertEqual(run2['market_state']['type'], 'bear')
        self.assertEqual(len(run2['signals']), 2)
        # 列表（timestamp 倒序，形状同 get_run_list 元素）
        self.store.save_eval_run('run_20261022', '2026-10-22T18:15:00', 'bull', 70.0, [])
        lst = self.store.list_eval_runs(limit=10)
        self.assertEqual([r['run_id'] for r in lst], ['run_20261023', 'run_20261022'])
        self.assertEqual(lst[1]['market_state'], 'bull')
        self.assertIsNone(self.store.get_eval_run('run_missing'))

    def test_eval_result_upsert(self):
        result = {'total_signals': 3, 'win_count': 2, 'win_rate': 0.667,
                  'avg_pnl': 0.012, 'avg_max_profit': 0.03, 'avg_max_drawdown': -0.01,
                  'details': [{'code': '000001.SZ', 'pnl_pct': 0.02}]}
        self.assertTrue(self.store.save_eval_result('run_20261023', result))
        got = self.store.get_eval_result('run_20261023')
        self.assertEqual(got['win_count'], 2)
        self.assertEqual(got['details'][0]['code'], '000001.SZ')
        # latest-wins
        result2 = dict(result, win_count=3, win_rate=1.0)
        self.store.save_eval_result('run_20261023', result2)
        got2 = self.store.get_eval_result('run_20261023')
        self.assertEqual(got2['win_count'], 3)
        self.assertEqual(got2['win_rate'], 1.0)
        self.assertIsNone(self.store.get_eval_result('run_missing'))

    # ---- 总结 ----
    def test_summary_save_and_history(self):
        data = {'market': '震荡市', 'signals': '2 个信号', 'recommendation': '持有观望',
                'full_text': '全天震荡，建议观望', 'model': 'deepseek-v4',
                'summary_type': 'daily_review', 'tokens_used': 1200}
        self.assertTrue(self.store.save_summary(
            'daily_review', data, signals_count=3, sectors_count=5))
        self.assertTrue(self.store.save_summary(
            'morning_review', {'full_text': '早盘总结', 'model': 'ensemble'},
            signals_count=2, sectors_count=4))
        hist = self.store.get_summary_history(limit=10)
        self.assertEqual(len(hist), 2)
        self.assertEqual(hist[0]['summary_type'], 'morning_review')  # id 倒序
        self.assertEqual(hist[0]['signals_count'], 2)
        self.assertEqual(hist[0]['data']['full_text'], '早盘总结')
        typed = self.store.get_summary_history(summary_type='daily_review')
        self.assertEqual(len(typed), 1)
        self.assertEqual(typed[0]['model'], 'deepseek-v4')
        self.assertIn('全天震荡', typed[0]['full_text'])

    # ---- 保留期清理 ----
    def test_prune_removes_old_rows(self):
        settings_stub = type('S', (), {
            'SQLITE_DB_PATH': 'x', 'SIGNAL_RETENTION_DAYS': 400,
            'EVAL_RETENTION_DAYS': 200, 'SUMMARY_RETENTION_COUNT': 2})()
        with patch('quant_engine.data.store.get_settings', return_value=settings_stub):
            old_day = (datetime.now() - timedelta(days=401)).strftime('%Y-%m-%d')
            fresh_day = datetime.now().strftime('%Y-%m-%d')
            self.store.save_signals([
                _signal_row(code='000001.SZ', day=old_day),
                _signal_row(code='600000.SH', day=fresh_day),
            ])
            old_ts = (datetime.now() - timedelta(days=201)).isoformat()
            self.store.save_eval_run('run_old', old_ts, 'bear', 20.0, [])
            self.store.save_eval_run('run_new', datetime.now().isoformat(), 'bull', 70.0, [])
            for i in range(4):
                self.store.save_summary('daily_review', {'full_text': f'第{i}篇'})
            pruned = self.store._prune()
            self.assertEqual(pruned['signals'], 1)
            self.assertEqual(pruned['eval_runs'], 1)
            # 新行保留
            rows = self.store.get_signal_history()
            self.assertEqual([r['code'] for r in rows], ['600000.SH'])
            self.assertEqual(len(self.store.list_eval_runs()), 1)
            # 总结按 id 保留最新 2 条
            hist = self.store.get_summary_history(limit=10)
            self.assertEqual([h['full_text'] for h in hist], ['第3篇', '第2篇'])

    def test_table_counts(self):
        self.store.save_signals([_signal_row()])
        counts = self.store.table_counts()
        self.assertEqual(counts['signals'], 1)
        self.assertEqual(counts['eval_runs'], 0)

    # ---- 降级 ----
    def test_disabled_store_degrades(self):
        """坏路径初始化失败 → 全方法返回安全默认值且不抛异常"""
        store = HistoryStore(db_path='/proc/nonexistent_dir/history.db')
        self.assertFalse(store.enabled)
        self.assertEqual(store.save_signals([_signal_row()]), 0)
        self.assertEqual(store.get_signal_history(), [])
        self.assertFalse(store.save_eval_run('r', 't', None, None, []))
        self.assertIsNone(store.get_eval_run('r'))
        self.assertEqual(store.list_eval_runs(), [])
        self.assertFalse(store.save_eval_result('r', {}))
        self.assertIsNone(store.get_eval_result('r'))
        self.assertFalse(store.save_summary('daily_review', {}))
        self.assertEqual(store.get_summary_history(), [])
        self.assertEqual(store.table_counts(), {})

    def test_runtime_failure_degrades_and_self_heals(self):
        """运行期 sqlite 错误自吞降级；连接丢弃后下轮可恢复"""
        self.store.save_signals([_signal_row()])
        # 破坏底层文件后触发 OperationalError → 降级不抛
        for suffix in ('', '-wal', '-shm'):
            p = Path(str(self.store.db_path) + suffix)
            if p.exists():
                p.unlink()
        rows = self.store.get_signal_history()  # 不抛异常即为通过
        self.assertIsInstance(rows, list)
        # 自愈：重建连接后可再次写入（新库）
        self.store._drop_conn()
        self.assertEqual(self.store.save_signals([_signal_row(code='600000.SH')]), 1)


if __name__ == '__main__':
    unittest.main()
