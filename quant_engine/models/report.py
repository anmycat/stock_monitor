"""结构化日报数据模型（D61）

参照"首席分析师日报"的信息架构：把自由文本的 AI 总结升级为
**可渲染、可归档、可按日期回看**的结构化报告。

设计要点：
1. 一级分区（ReportSection）—— 大盘研判 / 资金面 / 信号 / 风险
2. 结构化字段（ReportField）—— 标签 + 正文 + 语义等级（用于前端着色）
3. 门控状态（GateStatus）—— 直接映射 DefenseStrategy 的 action，
   附带"综合分 x/5"（5 个维度各 1 分），前端渲染成状态点 + 分数
4. 全文（full_text）保留 —— 结构化之外仍保留完整叙述，供钉钉推送与降级

与既有 AISummary 的关系：
- AISummary 产出 free_text（钉钉推送用），本模型在其之上做结构化加工
- 结构化优先，缺失字段降级为「未提供」而不是崩掉
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class ReportSeverity(str, Enum):
    """字段语义等级 —— 前端据此着色（红/橙/蓝/灰）"""

    CRITICAL = "critical"   # 红：风险/收紧
    WARNING = "warning"     # 橙：偏弱/注意
    POSITIVE = "positive"   # 绿：偏强/机会
    NEUTRAL = "neutral"     # 蓝：中性陈述
    INFO = "info"           # 灰：背景信息


class GateAction(str, Enum):
    """门控动作 —— 与 DefenseStrategy.check_market()['action'] 对齐"""

    NORMAL = "NORMAL"
    WATCH = "WATCH"
    REDUCE = "REDUCE"
    STOP = "STOP"


class ReportField(BaseModel):
    """报告中的一个结构化字段（如「核心矛盾」「资金面」）"""

    label: str = Field(..., description="字段标签，如「核心矛盾」")
    value: str = Field(..., description="字段正文")
    severity: ReportSeverity = ReportSeverity.NEUTRAL
    # 门控类字段可带一个"状态点"（如 🔴 弱市门控收紧）
    dot: bool = False


class GateStatus(BaseModel):
    """门控状态 —— 截图里的「🔴 弱市门控收紧（综合分 1/5）」"""

    action: GateAction = GateAction.NORMAL
    label: str = Field(..., description="如「弱市门控收紧」")
    score: int = Field(0, ge=0, le=5, description="综合分（0-5，越高越宽松）")
    total: int = Field(5, ge=1, description="满分（默认 5）")
    reasons: list[str] = Field(default_factory=list, description="触发的门控原因")

    @property
    def severity(self) -> ReportSeverity:
        """门控动作 → 语义等级"""
        return {
            GateAction.STOP: ReportSeverity.CRITICAL,
            GateAction.REDUCE: ReportSeverity.WARNING,
            GateAction.WATCH: ReportSeverity.WARNING,
            GateAction.NORMAL: ReportSeverity.POSITIVE,
        }[self.action]

    @property
    def score_text(self) -> str:
        return f"{self.score}/{self.total}"


class ReportSection(BaseModel):
    """一级分区（如「一、大盘研判」）"""

    index: int = Field(..., ge=1, description="分区序号（1 起）")
    title: str = Field(..., description="分区标题，如「大盘研判」")
    fields: list[ReportField] = Field(default_factory=list)
    # 分区级补充文本（AI 叙述段落）
    body: str = ""
    collapsed: bool = False  # 前端默认展开


class DailyReport(BaseModel):
    """一份完整的结构化日报"""

    date: str = Field(..., description="报告日期 YYYY-MM-DD")
    title: str = Field("首席分析师日报", description="报告标题")
    model: str | None = Field(None, description="生成模型标识")
    generated_at: datetime | None = None
    report_type: str = Field("daily_review", description="报告类型")

    # 一级分区
    sections: list[ReportSection] = Field(default_factory=list)

    # 门控状态（顶层冗余一份，方便前端直接取用与列表页展示）
    gate: GateStatus | None = None

    # 仓位建议（顶层冗余，列表页可直接展示）
    position_advice: str = ""
    position_ratio: float | None = Field(None, description="建议仓位比例 0-1")

    # 完整叙述（结构化之外的可读全文，钉钉推送/降级用）
    full_text: str = ""

    # 结构化原文（AI 返回的 JSON，便于二次加工与排查）
    raw: dict = Field(default_factory=dict)

    @property
    def has_content(self) -> bool:
        return bool(self.sections) or bool(self.full_text)

    def to_dict(self) -> dict:
        return self.model_dump(mode="json")


class ReportListItem(BaseModel):
    """日报列表项（日期选择器的数据源）"""

    date: str
    report_type: str = "daily_review"
    model: str | None = None
    generated_at: str | None = None
    gate_action: str | None = None
    gate_label: str | None = None
    gate_score: int | None = None
    position_advice: str = ""
