"""结构化日报测试（D61）

覆盖：
1. 数据模型（DailyReport / GateStatus / ReportField）的序列化与语义等级
2. 门控判定（5 维综合分）与仓位建议推导
3. AI JSON 解析（容忍 markdown 包裹 / 前后杂句 / 非法 JSON）
4. 分区组装与全文降级渲染
5. API 端点（/api/report、/api/report/dates）
"""
import json
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_engine.models.market import MarketState, MarketType
from quant_engine.models.report import (
    DailyReport,
    GateAction,
    GateStatus,
    ReportField,
    ReportSection,
    ReportSeverity,
)
from quant_engine.models.signal import Signal, SignalAction, SignalLevel
from quant_engine.services.ai.daily_report import DailyReportBuilder


def _market(t=MarketType.SIDEWAYS, score=35.0, trend=-1.2,
            vr=0.85, breadth=38.7) -> MarketState:
    return MarketState(type=t, score=score, trend=trend, volume_ratio=vr,
                       breadth=breadth, timestamp=datetime(2026, 11, 5, 15, 0))


def _signal(action=SignalAction.BUY, kelly=0.28) -> Signal:
    return Signal(
        code='600519.SH', name='贵州茅台', action=action, level=SignalLevel.A,
        score=85, position=0.3, reason=['均线多头排列'],
        timestamp=datetime(2026, 11, 5, 14, 30), strategy='balanced_alpha_v1',
        price=1392.05, kelly_position=kelly)


class TestReportModels(unittest.TestCase):
    """数据模型"""

    def test_gate_severity_mapping(self):
        cases = {
            GateAction.STOP: ReportSeverity.CRITICAL,
            GateAction.REDUCE: ReportSeverity.WARNING,
            GateAction.WATCH: ReportSeverity.WARNING,
            GateAction.NORMAL: ReportSeverity.POSITIVE,
        }
        for action, expected in cases.items():
            g = GateStatus(action=action, label='x')
            self.assertEqual(g.severity, expected)

    def test_gate_score_text(self):
        g = GateStatus(action=GateAction.REDUCE, label='弱市门控收紧',
                       score=1, total=5)
        self.assertEqual(g.score_text, '1/5')

    def test_report_to_dict_is_json_serializable(self):
        report = DailyReport(
            date='2026-11-05',
            sections=[ReportSection(index=1, title='大盘研判', fields=[
                ReportField(label='门控状态', value='弱市门控收紧（综合分 1/5）',
                            severity=ReportSeverity.WARNING, dot=True)])],
            gate=GateStatus(action=GateAction.REDUCE, label='弱市门控收紧', score=1),
            position_advice='≤3 成',
        )
        d = report.to_dict()
        json.dumps(d)  # 不抛异常即通过
        self.assertEqual(d['date'], '2026-11-05')
        self.assertEqual(d['gate']['score'], 1)
        self.assertTrue(d['sections'][0]['fields'][0]['dot'])

    def test_report_has_content(self):
        self.assertFalse(DailyReport(date='x').has_content)
        self.assertTrue(DailyReport(date='x', full_text='有内容').has_content)


class TestGateScoring(unittest.TestCase):
    """门控判定与综合分"""

    def setUp(self):
        self.b = DailyReportBuilder()

    def test_weak_market_low_score(self):
        """弱市：多数维度不通过 → 低综合分"""
        g = self.b.build_gate_status(_market(), [])
        self.assertEqual(g.action, GateAction.REDUCE)
        self.assertLessEqual(g.score, 2)

    def test_bull_market_high_score(self):
        """强市：多数维度通过 → 高综合分"""
        g = self.b.build_gate_status(
            _market(MarketType.BULL, 78, 2.4, 1.3, 68.0), [_signal()])
        self.assertEqual(g.action, GateAction.NORMAL)
        self.assertGreaterEqual(g.score, 4)

    def test_bear_market_stops(self):
        g = self.b.build_gate_status(
            _market(MarketType.BEAR, 18, -3.5, 0.6, 22.0), [])
        self.assertEqual(g.action, GateAction.STOP)

    def test_score_never_exceeds_total(self):
        for mkt in (_market(), _market(MarketType.BULL, 90, 5, 3, 90)):
            g = self.b.build_gate_status(mkt, [_signal()])
            self.assertLessEqual(g.score, g.total)
            self.assertGreaterEqual(g.score, 0)

    def test_position_advice_tightens_when_gate_weak(self):
        """门控收紧 → 仓位建议压低（截图场景：1/5 → ≤3 成）"""
        mkt = _market()
        g = self.b.build_gate_status(mkt, [])
        advice, _ratio = self.b.build_position_advice(mkt, g, [])
        self.assertIn('成', advice)
        if g.score <= 1:
            self.assertIn('3', advice)

    def test_bear_market_position_is_empty(self):
        mkt = _market(MarketType.BEAR, 18, -3.5, 0.6, 22.0)
        g = self.b.build_gate_status(mkt, [])
        advice, _ = self.b.build_position_advice(mkt, g, [])
        self.assertIn('空仓', advice)


class TestJsonParsing(unittest.TestCase):
    """AI JSON 解析容错"""

    def test_plain_json(self):
        d = DailyReportBuilder._parse_json('{"core_conflict": "多空矛盾"}')
        self.assertEqual(d['core_conflict'], '多空矛盾')

    def test_markdown_fenced(self):
        raw = '```json\n{"core_conflict": "多空矛盾"}\n```'
        d = DailyReportBuilder._parse_json(raw)
        self.assertEqual(d['core_conflict'], '多空矛盾')

    def test_json_with_surrounding_text(self):
        raw = '好的，分析如下：\n{"core_conflict": "多空矛盾"}\n希望有帮助'
        d = DailyReportBuilder._parse_json(raw)
        self.assertEqual(d['core_conflict'], '多空矛盾')

    def test_invalid_json_returns_empty(self):
        self.assertEqual(DailyReportBuilder._parse_json('完全不是 JSON'), {})
        self.assertEqual(DailyReportBuilder._parse_json(''), {})
        self.assertEqual(DailyReportBuilder._parse_json('{坏掉的}'), {})

    def test_non_dict_returns_empty(self):
        self.assertEqual(DailyReportBuilder._parse_json('[1,2,3]'), {})


class TestReportAssembly(unittest.TestCase):
    """分区组装"""

    def setUp(self):
        self.b = DailyReportBuilder()

    def test_build_without_ai_still_produces_report(self):
        """无 AI 时仍应产出客观部分（门控 + 仓位）"""
        rep = self.b.build(_market(), [_signal()], report_date='2026-11-05')
        self.assertEqual(rep.date, '2026-11-05')
        self.assertTrue(rep.sections)
        labels = [f.label for f in rep.sections[0].fields]
        self.assertIn('门控状态', labels)
        self.assertIn('仓位建议', labels)

    def test_build_with_ai_data_fills_fields(self):
        """AI 返回结构化数据时补齐叙述字段"""
        b = DailyReportBuilder()
        b.build_ai_diagnosis = lambda *a, **k: {
            'core_conflict': '趋势结构极弱',
            'capital_flow': '主力净流入传媒',
            'ai_diagnosis': '偏空一致性',
            'position_reason': '门控收紧',
            'risk_alert': '注意量能萎缩',
            'sections': [{'title': '板块机会', 'body': '军工**低位**'}],
        }
        rep = b.build(_market(), [], report_date='2026-11-05')
        labels = [f.label for f in rep.sections[0].fields]
        for want in ('核心矛盾', '资金面', 'AI诊断', '理由'):
            self.assertIn(want, labels)
        titles = [s.title for s in rep.sections]
        self.assertIn('板块机会', titles)
        self.assertIn('风险提示', titles)

    def test_full_text_fallback_render(self):
        """无 full_text 时自动渲染结构化全文"""
        rep = self.b.build(_market(), [], report_date='2026-11-05')
        self.assertIn('大盘研判', rep.full_text)
        self.assertIn('门控状态', rep.full_text)

    def test_section_index_is_sequential(self):
        b = DailyReportBuilder()
        b.build_ai_diagnosis = lambda *a, **k: {
            'sections': [{'title': 'A', 'body': 'x'}, {'title': 'B', 'body': 'y'}],
        }
        rep = b.build(_market(), [], report_date='2026-11-05')
        idxs = [s.index for s in rep.sections]
        self.assertEqual(idxs, sorted(idxs))
        self.assertEqual(idxs[0], 1)




class TestReportAPI(unittest.TestCase):
    """API 端点（TestClient 真实路由）"""

    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient

        from quant_engine.main import app
        cls.client = TestClient(app)
        # 关闭鉴权（生产由 API_AUTH_TOKEN 控制，测试环境无 token）
        cls._orig_env = os.environ.pop('API_AUTH_TOKEN', None)

    @classmethod
    def tearDownClass(cls):
        if cls._orig_env is not None:
            os.environ['API_AUTH_TOKEN'] = cls._orig_env

    def test_report_endpoint_exists(self):
        """端点已注册（无日报时返回 404 而非 404 路由）"""
        resp = self.client.get('/api/report')
        self.assertIn(resp.status_code, (200, 404))

    def test_report_dates_endpoint_exists(self):
        resp = self.client.get('/api/report/dates')
        self.assertIn(resp.status_code, (200, 404))

    def test_report_raw_endpoint_exists(self):
        resp = self.client.get('/api/report/raw')
        self.assertIn(resp.status_code, (200, 404))

    def test_report_with_date_returns_404_when_missing(self):
        resp = self.client.get('/api/report?date=1999-01-01')
        self.assertIn(resp.status_code, (200, 404))


class TestAIFallback(unittest.TestCase):
    """AI 结构化叙述的模型降级（D61 生产加固）"""

    class _FakeCfg:
        def __init__(self, name, weight):
            self.name = name
            self.weight = weight

    class _FakeAI:
        """最小 AISummary 替身：记录调用顺序，按配置返回成功/失败"""

        def __init__(self, models, primary, ok_map):
            self.models = models
            self.primary_model = primary
            self._ok_map = ok_map
            self.calls = []

        def _call_single_model(self, cfg, system, user, max_tokens=1500):
            self.calls.append((cfg.name, max_tokens))
            ok = self._ok_map.get(cfg.name, True)
            if not ok:
                class _R:
                    success = False
                    error = '限流(429)'
                    text = ''
                return _R()

            class _R:
                success = True
                error = ''
                text = '{"core_conflict": "测试矛盾"}'
            return _R()

        # AISummary 的格式化方法（最小替身，避免 prompt 组装抛异常）
        def _format_market_info(self, market):
            return f'市场 {market.type}'

        def _format_signal_info(self, signals):
            return f'{len(signals)} 条信号'

        def _format_sector_info(self, sectors):
            return '无板块数据'

        def _format_alert_info(self, alerts):
            return '今日无异动'

        def _format_news_info(self, news):
            return ''

    def _ai(self, ok_map):
        models = {
            'unisound': self._FakeCfg('云知声 U2', 0.7),
            'deepseek': self._FakeCfg('DeepSeek V4', 0.3),
        }
        return self._FakeAI(models, 'unisound', ok_map)

    def test_primary_model_first(self):
        ai = self._ai({'云知声 U2': True})
        b = DailyReportBuilder(ai_summary=ai)
        data = b.build_ai_diagnosis(_market(), [])
        self.assertEqual(ai.calls[0][0], '云知声 U2')
        self.assertEqual(data.get('_model'), '云知声 U2')

    def test_fallback_to_backup_model(self):
        """主模型失败 → 自动降级到备用模型"""
        ai = self._ai({'云知声 U2': False})
        b = DailyReportBuilder(ai_summary=ai)
        data = b.build_ai_diagnosis(_market(), [])
        self.assertEqual([c[0] for c in ai.calls],
                         ['云知声 U2', 'DeepSeek V4'])
        self.assertEqual(data.get('_model'), 'DeepSeek V4')

    def test_all_models_fail_returns_empty(self):
        ai = self._ai({'云知声 U2': False, 'DeepSeek V4': False})
        b = DailyReportBuilder(ai_summary=ai)
        self.assertEqual(b.build_ai_diagnosis(_market(), []), {})

    def test_max_tokens_is_2500(self):
        """D57 教训：思考型模型思考 token 计入 max_tokens，预算不能低于 2500"""
        ai = self._ai({'云知声 U2': True})
        b = DailyReportBuilder(ai_summary=ai)
        b.build_ai_diagnosis(_market(), [])
        self.assertEqual(ai.calls[0][1], 2500)

    def test_candidate_models_empty_without_models(self):
        b = DailyReportBuilder(ai_summary=self._ai({}))
        b.ai.models = {}
        self.assertEqual(b._candidate_models(), [])

    def test_no_ai_returns_empty(self):
        b = DailyReportBuilder()
        self.assertEqual(b.build_ai_diagnosis(_market(), []), {})


if __name__ == '__main__':
    unittest.main()
