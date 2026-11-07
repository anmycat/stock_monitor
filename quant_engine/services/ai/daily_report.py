"""结构化日报生成器（D61）

把市场数据 + 门控判定 + AI 叙述，组装成"首席分析师日报"式的结构化报告。

**关键设计：客观 vs 主观分离**
- 客观部分（门控状态、综合分、仓位建议）由**代码从真实数据计算**，
  不交给模型发挥 —— 截图里的「门控状态 🔴 弱市门控收紧（综合分 1/5）」
  必须与系统的实际门控行为一致，否则日报会误导决策。
- 主观部分（核心矛盾、AI 诊断、风险提示）由多模型总结提炼，
  要求模型按固定 JSON schema 输出，解析失败则降级到自由文本。

门控综合分（5 维，各 1 分，5 = 最宽松）：
  1. 市场类型（DefenseStrategy.check_market）
  2. 趋势结构（趋势 > 0）
  3. 市场宽度（breadth >= 50%）
  4. 量能（volume_ratio >= 1.0）
  5. 信号质量（有 BUY 信号）
"""
from __future__ import annotations

import json
import re
from datetime import datetime

from loguru import logger

from ...models.market import MarketState
from ...models.report import (
    DailyReport,
    GateAction,
    GateStatus,
    ReportField,
    ReportSection,
    ReportSeverity,
)
from ...models.signal import Signal, SignalAction
from ...strategies.defense import DefenseStrategy

# AI 结构化输出的 JSON schema（写进 prompt）
_JSON_SCHEMA_HINT = """请严格输出 JSON（不要 markdown 代码块包裹），结构如下：
{
  "core_conflict": "核心矛盾：一句话点出当前市场最本质的多空矛盾",
  "capital_flow": "资金面：主力资金流向方向与力度，引用具体板块与金额",
  "ai_diagnosis": "AI诊断：对当前局面的一句话判断（可为空表示无额外观点）",
  "position_advice": "仓位建议：建议仓位表述，如「≤3 成」",
  "position_reason": "仓位建议理由：解释为什么给这个仓位",
  "risk_alert": "风险提示：需要警惕的信号，可为空",
  "sections": [
    {"title": "板块机会", "body": "该分区的叙述段落"},
    {"title": "操作策略", "body": "该分区的叙述段落"}
  ]
}"""


class DailyReportBuilder:
    """日报构建器：客观判定 + AI 叙述 → DailyReport"""

    def __init__(self, ai_summary=None):
        self.ai = ai_summary
        self.defense = DefenseStrategy()

    # ------------------------------------------------------------------
    # 客观部分：门控状态与综合分
    # ------------------------------------------------------------------

    def build_gate_status(self, market: MarketState,
                          signals: list[Signal] | None = None) -> GateStatus:
        """门控状态 + 5 维综合分（全部由真实数据判定）"""
        signals = signals or []
        check = self.defense.check_market(market)

        action_map = {
            'STOP': GateAction.STOP,
            'REDUCE': GateAction.REDUCE,
            'WATCH': GateAction.WATCH,
            'NORMAL': GateAction.NORMAL,
        }
        action = action_map.get(check['action'], GateAction.NORMAL)

        # 5 个维度各 1 分
        dims = {
            '市场类型': action == GateAction.NORMAL,
            '趋势结构': market.trend > 0,
            '市场宽度': market.breadth >= 50,
            '量能配合': market.volume_ratio >= 1.0,
            '信号质量': any(s.action == SignalAction.BUY for s in signals),
        }
        score = sum(1 for v in dims.values() if v)

        label_map = {
            GateAction.STOP: '全面停止交易',
            GateAction.REDUCE: '弱市门控收紧',
            GateAction.WATCH: '门控观察',
            GateAction.NORMAL: '门控正常',
        }
        # 未通过的维度（不含门控原因说明 —— 那是"当前状态"不是"缺失项"）
        reasons = [k for k, v in dims.items() if not v]

        return GateStatus(
            action=action,
            label=label_map[action],
            score=score,
            total=5,
            reasons=reasons,
        )

    def build_position_advice(self, market: MarketState, gate: GateStatus,
                               signals: list[Signal] | None = None) -> tuple[str, float | None]:
        """仓位建议（表述 + 比例）——由门控与 Kelly 推导，不用模型发挥

        Returns:
            (建议文本, 建议比例 0-1 或 None)
        """
        signals = signals or []
        # 门控的仓位因子（DefenseStrategy 已实现）
        factor = self.defense.get_position_factor(market, [], [])

        # 候选信号的 Kelly 建议（有则取加权均值）
        kelly = [s.kelly_position for s in signals
                 if s.kelly_position is not None and s.action == SignalAction.BUY]
        if kelly:
            base = sum(kelly) / len(kelly)
            ratio = min(base * factor, 0.5)
            advice = f"≤{ratio * 10:.0f} 成"
        else:
            ratio = None
            advice = {
                GateAction.STOP: '空仓观望',
                GateAction.REDUCE: '≤2 成',
                GateAction.WATCH: '≤3 成',
                GateAction.NORMAL: '≤5 成',
            }[gate.action]

        # 门控收紧时上限再压一档（与截图「门控 1/5 → ≤3 成」的逻辑一致）
        if gate.score <= 1 and gate.action != GateAction.STOP:
            advice = '≤3 成'
            if ratio is not None:
                ratio = min(ratio, 0.3)
        elif gate.score <= 2 and gate.action != GateAction.STOP:
            advice = '≤4 成'
            if ratio is not None:
                ratio = min(ratio, 0.4)

        return advice, ratio

    # ------------------------------------------------------------------
    # 主观部分：AI 结构化叙述
    # ------------------------------------------------------------------

    def build_ai_diagnosis(self, market: MarketState, signals: list[Signal],
                           sectors: list[dict] | None = None,
                           alerts: list[dict] | None = None,
                           news: list[dict] | None = None) -> dict:
        """调用 AI 生成结构化叙述

        Returns:
            解析后的 dict；AI 不可用或解析失败时返回 {}（调用方降级）
        """
        if not self.ai:
            return {}
        try:
            prompt = self._build_structured_prompt(
                market, signals, sectors, alerts, news)
            # 按权重降序逐个尝试（与 AISummary 的 cascade 策略一致）：
            # 主模型 U2 失败时自动降级到 DeepSeek，而不是直接放弃结构化叙述。
            last_error = ''
            for cfg in self._candidate_models():
                resp = self.ai._call_single_model(
                    cfg, prompt['system'], prompt['user'], max_tokens=2500)
                if resp.success:
                    data = self._parse_json(resp.text)
                    if data:
                        data['_model'] = cfg.name
                        return data
                    last_error = 'JSON 解析失败'
                    continue
                last_error = resp.error
                logger.warning(
                    f"日报结构化生成失败({cfg.name}): {last_error}")
            if last_error:
                logger.warning(f"日报结构化叙述降级为空: {last_error}")
            return {}
        except Exception as e:  # noqa: BLE001 结构化失败不应中断日报生成
            logger.warning(f"日报结构化生成异常: {type(e).__name__}: {e}")
            return {}

    def _candidate_models(self) -> list:
        """候选模型列表（主模型优先，按权重降序）"""
        models = getattr(self.ai, 'models', None) or {}
        if not models:
            return []
        primary = getattr(self.ai, 'primary_model', None)
        ordered = sorted(models.values(),
                         key=lambda m: getattr(m, 'weight', 0), reverse=True)
        if primary and primary in models:
            head = [models[primary]]
            tail = [m for k, m in models.items() if k != primary]
            return head + tail
        return ordered

    def _build_structured_prompt(self, market: MarketState, signals: list[Signal],
                                 sectors: list[dict] | None = None,
                                 alerts: list[dict] | None = None,
                                 news: list[dict] | None = None) -> dict:
        """构造结构化输出的 prompt"""
        fmt = self.ai._format_market_info
        market_info = fmt(market)
        signal_info = self.ai._format_signal_info(signals)
        sector_info = self.ai._format_sector_info(sectors) if sectors else '无板块数据'
        alert_info = self.ai._format_alert_info(alerts) if alerts else '今日无异动'
        news_info = self.ai._format_news_info(news) if news else ''

        system = (
            '你是一位资深A股首席分析师。你的任务是把盘面数据转成结构化的日报判断。\n'
            '要求：\n'
            '- 每个字段都是一句话或两句，数据驱动，不要空话\n'
            '- 资金面要引用具体板块与金额\n'
            '- 仓位建议要与给定的门控状态一致（门控收紧时给低仓位）\n'
            f'{_JSON_SCHEMA_HINT}'
        )
        user = f"""## 市场数据
{market_info}

## 板块表现
{sector_info}

## 交易信号
{signal_info}

## 异动记录
{alert_info}

{f'## 新闻与事件{chr(10)}{news_info}' if news_info else ''}

请按上述 JSON 结构输出（纯 JSON，不要其他文字）："""
        return {'system': system, 'user': user}

    @staticmethod
    def _parse_json(text: str) -> dict:
        """从模型输出里提取 JSON（容忍 ```json 包裹与前后杂句）"""
        if not text:
            return {}
        cleaned = text.strip()
        # 去掉 markdown 代码块包裹
        cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned).strip()

        # 直接解析
        try:
            data = json.loads(cleaned)
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, ValueError):
            pass

        # 提取第一个 {...} 块
        start = cleaned.find('{')
        end = cleaned.rfind('}')
        if start >= 0 and end > start:
            try:
                data = json.loads(cleaned[start:end + 1])
                if isinstance(data, dict):
                    return data
            except (json.JSONDecodeError, ValueError):
                pass
        logger.warning('日报结构化 JSON 解析失败')
        return {}

    # ------------------------------------------------------------------
    # 组装
    # ------------------------------------------------------------------

    def build(self, market: MarketState, signals: list[Signal],
              sectors: list[dict] | None = None,
              alerts: list[dict] | None = None,
              news: list[dict] | None = None,
              report_date: str | None = None,
              full_text: str = '',
              model: str | None = None) -> DailyReport:
        """组装完整日报

        Args:
            market: 市场状态
            signals: 交易信号
            sectors: 板块数据
            alerts: 异动记录
            news: 新闻
            report_date: 报告日期（默认今天）
            full_text: 已有的自由文本总结（AI 总结产物），作为降级与全文
            model: 生成模型标识
        """
        report_date = report_date or datetime.now().strftime('%Y-%m-%d')

        # 客观部分
        gate = self.build_gate_status(market, signals)
        position_advice, position_ratio = self.build_position_advice(
            market, gate, signals)

        # 主观部分
        ai_data = self.build_ai_diagnosis(
            market, signals, sectors, alerts, news)

        sections = self._build_sections(
            market, signals, sectors, alerts, gate, position_advice,
            position_reason=ai_data.get('position_reason', ''),
            ai_data=ai_data)

        # I-08：可选「过程纪律」客观分区（默认关闭，口径待用户裁定）。
        # 关闭时本段完全跳过 → 日报输出与上一版逐字段一致。
        try:
            from ...config import get_settings
            if bool(get_settings().DAILY_REPORT_DISCIPLINE_DIMENSION):
                from .discipline import (
                    build_discipline_metrics,
                    render_discipline_fields,
                )
                sealed: list = []
                try:
                    from ...data.store import get_history_store
                    sealed = get_history_store().list_sealed_dates(limit=5)
                except Exception as e:  # noqa: BLE001 封存不可读不影响日报
                    logger.debug(f"[I-08] sealed dates unavailable: {e}")
                metrics = build_discipline_metrics(signals, sealed_dates=sealed)
                sections.append(ReportSection(
                    index=len(sections) + 1,
                    title='过程纪律（客观指标）',
                    collapsed=True,
                    fields=[ReportField(label=k, value=v)
                            for k, v in render_discipline_fields(metrics)],
                    evidence=[{'kind': 'discipline', 'source': 'code',
                               'degraded': False, 'note': metrics['note']}],
                ))
        except Exception as e:  # noqa: BLE001 可选维度失败不得影响日报主流程
            logger.debug(f"[I-08] discipline section skipped: {e}")

        return DailyReport(
            date=report_date,
            model=model,
            generated_at=datetime.now(),
            sections=sections,
            gate=gate,
            position_advice=position_advice,
            position_ratio=position_ratio,
            full_text=full_text or self._render_full_text(sections),
            raw=ai_data,
        )

    def _build_sections(self, market: MarketState, signals: list[Signal],
                        sectors: list[dict] | None, alerts: list[dict] | None,
                        gate: GateStatus, position_advice: str,
                        position_reason: str = '', ai_data: dict | None = None) -> list[ReportSection]:
        """构建一级分区"""
        ai_data = ai_data or {}
        sections: list[ReportSection] = []

        # 一、大盘研判
        market_fields = [
            ReportField(
                label='门控状态',
                value=f"{gate.label}（综合分 {gate.score_text}）",
                severity=gate.severity,
                dot=True,
            ),
        ]
        if ai_data.get('core_conflict'):
            market_fields.append(ReportField(
                label='核心矛盾', value=ai_data['core_conflict'],
                severity=ReportSeverity.NEUTRAL))
        if ai_data.get('capital_flow'):
            market_fields.append(ReportField(
                label='资金面', value=ai_data['capital_flow'],
                severity=ReportSeverity.NEUTRAL))
        if ai_data.get('ai_diagnosis'):
            market_fields.append(ReportField(
                label='AI诊断', value=ai_data['ai_diagnosis'],
                severity=ReportSeverity.INFO))
        market_fields.append(ReportField(
            label='仓位建议', value=position_advice,
            severity=(ReportSeverity.WARNING if gate.score <= 2
                      else ReportSeverity.NEUTRAL)))
        if position_reason:
            market_fields.append(ReportField(
                label='理由', value=position_reason,
                severity=ReportSeverity.INFO))

        sections.append(ReportSection(
            index=1, title='大盘研判', fields=market_fields,
            evidence=self._build_evidence(signals)))

        # 二、AI 自定义分区（模型给的板块机会/操作策略等）
        for i, sec in enumerate(ai_data.get('sections') or [], start=2):
            if not isinstance(sec, dict):
                continue
            title = str(sec.get('title') or f'分区{i}').strip()
            body = str(sec.get('body') or '').strip()
            if not title and not body:
                continue
            sections.append(ReportSection(
                index=i, title=title or f'分区{i}', body=body))

        # 三、风险提示（独立分区，便于前端折叠收起）
        if ai_data.get('risk_alert'):
            sections.append(ReportSection(
                index=len(sections) + 1, title='风险提示',
                body=str(ai_data['risk_alert']),
                fields=[ReportField(
                    label='风险', value=str(ai_data['risk_alert']),
                    severity=ReportSeverity.CRITICAL)]))

        return sections

    @staticmethod
    def _build_evidence(signals) -> list[dict]:
        """I-09：为大盘研判附上结论证据血缘（只读，模型不得编造分值）。

        D61 铁律「客观与主观分离」在此延续：门控状态/综合分/仓位建议
        明确标注由代码计算；数据侧标注源、截止日、新鲜度与是否降级。
        """
        def field(obj, key, default=None):
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        out = [{
            'kind': 'gate', 'source': 'code', 'degraded': False,
            'note': '门控状态 / 5 维综合分 / 仓位建议由代码从真实数据计算',
        }]
        seen = set()
        for s in signals or []:
            source = field(s, 'data_source') or 'unknown'
            as_of = field(s, 'data_as_of')
            key = (source, as_of)
            if key in seen or (source == 'unknown' and not as_of):
                continue
            seen.add(key)
            freshness = field(s, 'freshness')
            out.append({
                'kind': 'daily_kline',
                'code': field(s, 'code'),
                'source': source,
                'as_of': as_of,
                'freshness': freshness,
                'cross_check': field(s, 'cross_check'),
                'degraded': freshness in ('lagging', 'stale'),
            })
        return out[:8]

    @staticmethod
    def _render_full_text(sections: list[ReportSection]) -> str:
        """把结构化分区渲染成可读全文（降级用）"""
        lines: list[str] = []
        for sec in sections:
            lines.append(f"{'一二三四五六七八九十'[min(sec.index - 1, 9)]}、{sec.title}")
            for f in sec.fields:
                lines.append(f"{f.label}：{f.value}")
            if sec.body:
                lines.append(sec.body)
            lines.append('')
        return '\n'.join(lines).strip()
