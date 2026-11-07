# 架构调研：TradingAgents-CN 与 MCP 风格数据源接口层

> 2026-11-01 ｜ 查证：GitHub REST API + raw 文件直读 + MCP 官方规范（latest 2026-07-28）
> 基准：`hsliuping/TradingAgents-CN` 默认分支 `v3.0`(SHA 51060a76)

## 调研 A：tradingagents-cn

**A1 基本面**（源：GitHub API `/repos/hsliuping/TradingAgents-CN`）
仓库地址与题目一致：https://github.com/hsliuping/TradingAgents-CN ｜ **32,739 star / 6,817 fork**（官网宣传页写 65,391，与 API 不符，勿引用）｜ 最近推送 2026-09-22，默认分支 `v3.0`，无 GitHub Release。（**2026-11-06 GitHub REST API 复测维持**：32,819★ / 6,841 fork，pushed_at 仍为 09-22，仍无 release，结论不变；见 `docs/32` §4.1 与 `docs/33` §6）语言：Python 8.66MB(81%)、Vue 1.05MB、PowerShell 442KB、TypeScript 197KB、Shell 127KB；v3.0 形态 FastAPI+Vue3+MongoDB/Redis，社区版/Pro 版分裂。
**许可为混合授权（关键风险）**：`tradingagents/`、`cli/`、`docs/`、`tests/` = Apache-2.0 可自由用；`app/`(FastAPI)、`frontend/`(Vue3) = 专有 Source-Available，**禁止再分发与商业使用**。API 报 `NOASSERTION` 是因识别不了混合许可。

**A2 agent 编排**（源：`tradingagents/graph/setup.py`、`agents/` 树）——LangGraph `StateGraph`，边全在 setup.py 显式声明（267 行，可读性好，值得抄）：
`START → Analyst(market/social/news/fundamentals 串行，各带 tools_x 回环) → Bull⇄Bear 辩论 → Research Manager 裁决 → Trader → Risky→Safe→Neutral 风险辩论 → Risk Judge → END`
角色文件 `agents/{analysts,researchers,managers,risk_mgmt,trader}/`；状态用 TypedDict 显式声明（`agents/utils/agent_states.py`）：四份研究报告与 `investment_debate_state`/`risk_debate_state` **分槽存放**；决策产物由 `graph/signal_processing.py` 抽成结构化 `{action,target_price,confidence,risk_score,reasoning}`。

**A3 数据层（本项目最相关）——它有两套并行且互不通气的抽象，这是它最大的债**
- ①`tradingagents/dataflows/`（Apache-2.0）：`providers/base_provider.py` 的 `BaseStockDataProvider(ABC)`，**异步**接口 + 基类内置 `standardize_quotes()/standardize_basic_info()` 字段标准化（统一出 20 个标准字段）——**最值得借鉴的一点**；`providers/china/{akshare,tushare,baostock}.py`；`data_source_manager.py`(2474行，臃肿) 管优先级+降级+缓存+字符串输出，优先级**从 DB `system_configs.data_source_configs` 读**(enabled/priority/market_categories)，读不到回落硬编码 `AKShare>Tushare>BaoStock`；`constants/data_sources.py` 有 `DataSourceCode` + `DATA_SOURCE_REGISTRY`（含 `supported_markets/requires_api_key/features`）。
- ②`app/services/data_sources/`（**专有许可**）：`DataSourceAdapter(ABC)` 同步接口（stock_list/daily_basic/realtime_quotes/kline/news/daily_ohlc），**不做字段标准化**；`manager.py` 按 priority 逐个 try/except 返回 `(data, source_name)`；v3.0 新增 `local_adapter.py`、`qmt_adapter.py`；`data_consistency_checker.py` 已退化为 **no-op stub**（永远 is_consistent=True）。
- **A 股源**：Tushare / AKShare / BaoStock。**TDX 已被明确移除**（`data_source_manager.py:504-505` 注释）。**未用东财原生接口与聚宽**（东财仅经 AKShare 间接）。美股 Finnhub/yfinance/AV，港股 AKShare。
- **数据获取＝硬编码调用为主，不是统一 tool calling**：`agents/utils/agent_utils.py` 的 `Toolkit` 用 LangChain `@tool`，四个"统一工具"(`get_stock_market_data_unified` 等)+20 多个细碎工具，但工具内部直接 `from interface import get_china_stock_data_unified` 硬调，**无 schema 契约，返回中文大字符串**。
- **没有统一 tool/interface 抽象层**：`Toolkit`（LangChain 工具集合）与两个数据源 ABC 彼此不继承、字段不互通，重复实现两遍。

**A4 LLM 接入**（源：`llm_clients/`、`app/constants/model_capabilities.py`）
`factory.py` 统一工厂 + `model_catalog.py`：openai/anthropic/google/deepseek/qwen/glm/qianfan/openrouter/aihubmix/volcengine/ollama/custom_openai，每 provider 分 `quick`/`deep` 两档。**模型也做能力声明**：`ModelCapabilityLevel`(1-5) + `ModelFeature`(TOOL_CALLING/LONG_CONTEXT/REASONING/VISION/FAST_RESPONSE/COST_EFFECTIVE) + `ANALYSIS_DEPTH_REQUIREMENTS` 按任务复杂度路由——好模式，值得抄。
**多模型投票/ensemble：没有**（文件树全量正则扫描 `ensemble|vote|consensus` 零命中）。多 agent 是多角色辩论而非多模型投票，辩论角色共用同一 quick_llm。
**反思机制：有但弱闭环**。`graph/reflection.py` 的 `Reflector` 5 个 `reflect_*` 方法输入 `returns_losses`、写 ChromaDB memory，由 `TradingGraph.reflect_and_remember()` 在纸面交易产生盈亏后调用。**无单次运行内自检-重试，无置信度聚合。**

**A5 可借鉴的三点（对应你提的三方向）**
1. **多数据源统一抽象** → 抄"字段标准化下沉到基类"（`standardize_quotes`）；教训是**别做两套抽象**（`dataflows/providers` vs `app/services/data_sources`），只留一套。
2. **研究/决策分离** → 抄 `AgentState` 分槽（研究产物与辩论状态严格分开）+ `SignalProcessor` 把决策文本抽成结构化字段。对我们：信号生成（研究）与告警推送（决策）分离，信号表存结构化字段，告警层只渲染。
3. **配置驱动** → 抄 (a) 数据源优先级/enable 从 DB 读、读不到回落默认值、**永不硬失败**；(b) 模型能力分级按复杂度选模型。对我们：优先级/超时/重试/冷却全进配置；quant-beat 任务分级（分钟级小模型、日终复盘大模型）。

**A6 不适用 / 踩坑**
1. **许可陷阱（最硬）**：最像我们要抄的 `app/services/data_sources/` 与 `app/utils/trading_time.py` **都在专有 `app/` 下，禁止再分发与商业使用**。**能借鉴设计，不能复制代码。**
2. **架构过重**：FastAPI+Vue3+MongoDB+Redis+LangGraph+ChromaDB+LangChain，对个人量化监控收益为负。我们三服务+systemd 形态正确，不要动。
3. **两套抽象并存是负债**：`dataflows/README.md` 自认"…**建议保持现状，避免大规模重构**"——这就是"不敢重构"的终局，不要走这条路。
4. **能力声明只到元数据层**：`DataSourceInfo.features` 是 `List[str]` 自由文本，机器判不了"这源支持 daily 还是 minute"，调用方仍要 if/else。这正是要用 MCP 风格 `capabilities` 显式枚举解决的。
5. **字段标准两版不一致**（基类 20 字段 vs Adapter 不标准化）、**一致性检查是空壳**（v3.0 永远说"一致"）、**工具返回中文大字符串**（LLM 友好但不可测、不可 diff、无法数值校验）——我们一律要结构化 DataFrame + schema 契约。
6. **AKShare 主力违背我们的纪律**：`system/02 §4.3` 明确"不使用 akshare：上游接口一变整库崩"。
7. **TDX 被上游移除**→本地终端源维护成本高，我们的 TDX 必须可插拔可降级。
## 调研 B：MCP 风格统一数据源接口层

**B1 MCP tool 规范**（源：modelcontextprotocol.io/specification/latest/server/tools + 2025-06-18）
Tool 字段：`name`(必填)、`title`(可选)、`description`、`icons`(可选)、`inputSchema`(必填,JSON Schema,默认 2020-12；无参推荐 `{"type":"object","additionalProperties":false}`)、`outputSchema`(可选；若提供则服务端 MUST 返回符合 schema 的结构化结果，客户端 SHOULD 校验)、`annotations`(可选提示 readOnlyHint/destructiveHint/idempotentHint/openWorldHint，默认 readOnly=false/destructive=true/idempotent=false/openWorld=true；规范明确 **annotations 不可信、不改变协议行为**)。交互：`tools/list`(cursor 分页，latest 另带 `ttlMs`/`cacheScope`) → `tools/call`。
**错误两套机制必须区分**：协议错误＝JSON-RPC `error:{code,message,data}`（如 `-32602 Unknown tool`），**无 result**；工具执行错误＝`result:{content:[{type:"text",text}], isError:true}`，**有 result**，模型可读可自我纠正。
**Python SDK v2 三分法**（源：py.sdk.modelcontextprotocol.io/servers/handling-errors/）：`raise ToolError(msg)` → `is_error=True` 且**模型看得见原话**、日志仅一行 INFO；`raise MCPError(code=INVALID_PARAMS)` → 整个 call 变 JSON-RPC 错误、**模型什么都看不到**；其他异常＝crash，模型只收到 `"Error executing tool <name>"`，traceback 只进服务端日志。判据原文：*"could a smarter model have avoided this? Yes → ToolError; No → MCPError."* 结构化输出页：返回类型注解即 output schema（Pydantic/TypedDict/dataclass/dict 直接用；标量与 list 包成 `{"result":...}`），返回值按 schema 校验，不匹配即变工具错误而非"半空对象"。
**给我们的取舍**：我们不是给 LLM 调的工具，是给自己代码调的源。借鉴 ①显式 schema 契约 ②错误结构化返回而非异常 ③能力声明 ④可发现(list)；**不借鉴** `isError`+content 文本块那层。"错误以结构化结果返回而非异常"正是 `isError` 精神在非 LLM 场景的直译。

**B2 业界共识**（源：CAMARA/电信 OpenAPI 化实践、多厂商 Adapter 架构、统一 AI API 设计规范）
1. **契约先行**：先定 OpenAPI 3.1/JSON Schema 描述**你的业务 API 而非厂商原生 API**，厂商改字段只动 adapter。
2. **Adapter/Facade 隔离**：每厂商一薄 adapter 实现同一稳定接口，厂商怪癖用 contract extension(`allOf+if/then`) 表达，不污染核心 schema。
3. **错误信封四段式**：`code`(稳定带版本)+`category`(机器可读 auth_required/rate_limit/validation/transient/permanent)+`message`(人读)+`details`(重试指引)。原话：*"treats the machine as the primary reader and the human as a secondary one."*
4. **契约测试**：同一套测试跑所有 adapter 防行为漂移，监控"契约合规率"作 SLO。
⚠ 教训：标准只解 N×M，不解"数据对不对"。字段对齐层解决不了"腾讯默认前复权"这种语义差异（`system/02 §4.1` 已实测），须由**源自身口径声明 + 落库校验**兜住。

**B3 可落地设计** 目录：`data/spec.py`(契约) `data/align.py`(字段对齐) `data/registry.py`(源注册表) `data/hub.py`(统一入口，取代 source_manager.py) `data/sources/*.py`(薄实现)。

```python
# ---------- spec.py ----------
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable

class Capability(str, Enum):
    DAILY="daily"; REALTIME="realtime"; LIST="list"
    MINUTE="minute"; BOARD="board"; ETF="etf"; FUNDAMENTAL="fundamental"

class ErrorCode(str, Enum):
    OK="ok"; TIMEOUT="timeout"; RATE_LIMITED="rate_limited"
    BAD_PAYLOAD="bad_payload"; NOT_SUPPORTED="not_supported"
    COOLDOWN="cooldown"; CIRCUIT_OPEN="circuit_open"; UNKNOWN="unknown"

@dataclass(frozen=True)
class DataResult:
    """统一返回信封。失败不抛异常——错误是数据的一部分。"""
    ok: bool; data: Any = None; source: str | None = None
    stale: bool = False            # True=来自快照/缓存，非实时
    elapsed_ms: int = 0
    error_code: ErrorCode = ErrorCode.OK; error_msg: str = ""
    fetched_at: float = 0.0        # 数据自身时间戳（非拉取时间）
    meta: dict = field(default_factory=dict)
    @classmethod
    def fail(cls, code, msg, **kw): return cls(ok=False, error_code=code, error_msg=msg, **kw)

# 标准列 schema：字段名即契约，对齐的唯一真源
STANDARD_COLUMNS: dict[Capability, tuple[str,...]] = {
    Capability.DAILY: ("date","open","high","low","close","pre_close",
        "volume","amount","pct_chg","amplitude","turnover_rate"),
    Capability.REALTIME: ("symbol","name","price","pre_close","open","high","low",
        "pct_chg","volume","amount","trade_time"),
    Capability.LIST: ("symbol","name","market","board","list_date"),
    Capability.MINUTE: ("datetime","open","high","low","close","volume","amount"),
}

@runtime_checkable
class Source(Protocol):
    name: str
    capabilities: frozenset[Capability]          # 显式能力声明
    def fetch(self, cap: Capability, **params) -> DataResult: ...

# ---------- align.py（字段对齐层）----------
def align(cap, raw: list[dict], mapping: dict[str,str]) -> list[dict]:
    """源列名→标准列。未知列丢弃，缺列填 None（不猜、不造）。"""
    cols = STANDARD_COLUMNS[cap]
    return [{c: r.get(mapping.get(c, c)) for c in cols} for r in raw]

# ---------- hub.py（统一入口）----------
class DataHub:
    """取代 source_manager.py。冷却/熔断/快照语义保留，错误结构化返回。"""
    def __init__(self, sources, *, priority, cooldown_s=300, fail_threshold=3,
                 snapshot=None, gate=None):
        self._by_name = {s.name: s for s in sources}
        self._order = [n for n in priority if n in self._by_name]
        self._cooldown_s, self._fail_threshold = cooldown_s, fail_threshold
        self._fail_count: dict[str,int] = {}; self._cool_until: dict[str,float] = {}
        self._snapshot = snapshot      # 快照回退（SQLite）
        self._gate = gate              # 交易时段闸门
    def _eligible(self, cap):
        """能力过滤 + 冷却/熔断排除，返回优先级序候选源。"""
        import time; now = time.time()
        return [self._by_name[n] for n in self._order
                if cap in self._by_name[n].capabilities
                and self._cool_until.get(n, 0) <= now]
    def get(self, cap: Capability, **params) -> DataResult:
        """统一取数。永不抛异常；失败返回 DataResult(ok=False, error_code=...)"""
        import time; t0 = time.perf_counter()
        # 1) 交易时段闸门：非交易时段跳过网络层，直接快照
        if self._gate is not None and not self._gate.allows(cap):
            r = self._from_snapshot(cap, **params)
            if r: return r
            return DataResult.fail(ErrorCode.NOT_SUPPORTED, "非交易时段且无快照",
                                   elapsed_ms=int((time.perf_counter()-t0)*1000))
        # 2) 按优先级依次尝试
        last = None
        for s in self._eligible(cap):
            r = self._call_with_breaker(s, cap, **params)
            if r.ok:
                r.elapsed_ms = int((time.perf_counter()-t0)*1000); return r
            last = r
        # 3) 全失败 → 快照回退（stale=True，强制标注数据陈旧）
        snap = self._from_snapshot(cap, **params)
        if snap is not None:
            snap.stale = True; snap.elapsed_ms = int((time.perf_counter()-t0)*1000); return snap
        return last or DataResult.fail(ErrorCode.UNKNOWN, "无可用源")
    def _call_with_breaker(self, s, cap, **p) -> DataResult:
        import time
        try: r = s.fetch(cap, **p)
        except Exception as e:               # 任何源异常都不外泄
            r = DataResult.fail(ErrorCode.BAD_PAYLOAD, f"{type(e).__name__}: {e}")
        if not r.ok:
            self._fail_count[s.name] = self._fail_count.get(s.name, 0) + 1
            if self._fail_count[s.name] >= self._fail_threshold:
                self._cool_until[s.name] = time.time() + self._cooldown_s
                self._fail_count[s.name] = 0
        else: self._fail_count[s.name] = 0
        return r
    def _from_snapshot(self, cap, **p):
        if self._snapshot is None: return None
        rows = self._snapshot.load(cap, **p)
        return DataResult(ok=True, data=rows, source="snapshot", stale=True) if rows else None

# 各源接入形态（tencent 为例，其余同构）：能力声明 + fetch 分发 + 字段映射表
class TencentSource:
    name = "tencent"
    capabilities = frozenset({Capability.REALTIME, Capability.DAILY, Capability.LIST})
    def fetch(self, cap, **params) -> DataResult:
        if cap is not Capability.REALTIME:
            return DataResult.fail(ErrorCode.NOT_SUPPORTED, f"tencent 不支持 {cap}")
        try: raw = self._parse(self._http(**params))
        except socket.timeout: return DataResult.fail(ErrorCode.TIMEOUT, "tencent timeout")
        except Exception as e: return DataResult.fail(ErrorCode.BAD_PAYLOAD, str(e))
        return DataResult(ok=True, data=align(cap, raw, TENCENT_COLUMNS), source=self.name)
```
**迁移路径（不改调用方，4 步）**
1. **并存期**：`DataHub` 与旧 `source_manager.py` 共存；旧 `get_daily_data/get_realtime_quote/get_stock_list` **签名不变**，内部改 `return self.hub.get(Capability.DAILY, ...).data`。调用方（api/worker/beat）**零改动**。
2. **逐源替换**：每迁一个源，加一个实现 `Source` 的类 + 在 `registry.py` 注册（name/capabilities/priority/冷却参数）；旧源文件保留不删，全换完再删。
3. **能力声明收口**：`registry.py` 一张表（可 YAML 覆盖）承载"哪个源支持哪些 capability"，`hub._eligible()` 直接读表路由，消灭散落 if/else。
4. **对齐层 + 契约测试**：每源补 `map`（源列→标准列）+ golden fixture 断言"同标的跨源对齐后 close 一致（容差 0.5%）"——正是 `system/02 §4.1` 要求的腾讯 qfq vs 东财不复权校准，把一次性人工核对变成常驻回归。
**与"非交易时间静默"（时段闸门）结合**
- 闸门做成独立策略 `gate.allows(cap)->bool` 注入 `DataHub`，**不散落在各源里**。
- 分层语义（对齐 `system/02 §6` 时点纪律）：`DAILY/LIST/FUNDAMENTAL` **不受闸门限制**（收盘后拉日K是主路径）；`REALTIME/MINUTE` 受控，仅 09:30-11:30 / 13:00-15:00 放行，非交易时段走快照分支、**不发网络请求**、返回 `stale=True`，由调用方决定是否播报。
- 我们的 cron 是 09:30 open_check / 15:05 signal / 15:30 persist，**都不在盘中** → 闸门对 Phase 1/2 实际不拦截，Phase 3 分钟级预警才生效。**所以接口现在就要留，不能等 Phase 3 再改数据层。**
- 闸门逻辑照 `app/utils/trading_time.py` 的东八区 + 9:30-11:30 / 13:00-15:00 + 15:00-15:30 收盘缓冲期**重写**（该文件在专有 `app/` 下**不能抄**；且它漏法定节假日，我们已有 `config/holidays.json` 兜底）。闸门关闭时不是"报错"而是"返回快照并标 stale"——与 `system/02 §4.2`"⚠ 数据陈旧至 YYYY-MM-DD，宁可错杀不可静默"完全一致。

**B4 语义对照（保证不退化）**
| 现有 `source_manager.py` | 新实现 | 退化? |
|---|---|---|
| 多源优先级 | `hub._order`（registry，YAML 可覆盖） | 否，且顺序外置 |
| 失败计数+冷却熔断(3次→300s) | `_call_with_breaker` 同阈值同冷却；`_eligible` 跳过冷却中的源 | 否 |
| 快照回退 | `_from_snapshot` + `stale=True` | 否，stale 变显式字段 |
| 抛异常给调用方 | 改 `DataResult(ok=False, error_code=...)` | **有意的行为变更**，API 层可加一层适配保留旧语义 |
| 字段人工对齐 / 能力声明（原本都没有） | `align()`+map 表+契约测试 / `capabilities: frozenset[Capability]` | 改善 / 新增 |

## 结论：下一轮先做这 3 件事
**1. 立 `spec.py` 契约层，但不动 `source_manager.py`**（半天，收益最大）
定义 `Capability/ErrorCode/DataResult/STANDARD_COLUMNS/Source` 协议，纯新增文件、零风险。**先有契约再谈重构**——TradingAgents-CN 正是因为没有契约，才有 `dataflows/providers` 与 `app/services/data_sources` 两套互不通气的抽象，最终锁死在"不敢重构"。
**2. 建 `registry.py` 能力声明表 + `align.py` 字段对齐层**
一张表列出 tencent/sina/eastmoney/tdx/tushare 各自的 `capabilities`(daily/realtime/list/minute/board/etf…)、优先级、是否需 token、标准列映射。直接解决最痛的"加源改多处"与"字段对齐靠人工"：**加源＝registry 一行 + 一个新源文件**；字段对齐＝map 表 + golden fixture 回归。**不碰调用方，可独立上线。**
**3. 只迁「实时行情」一条链路做样板**
实时链路最简单（腾讯/新浪单 URL + 文本解析）、最痛（东财封禁已实测）、最能验证新抽象。做法：`DataHub` 与旧 `SourceManager` 并存，`get_realtime_quote` 内部切 `hub.get(Capability.REALTIME)`，加 `stale` 标注与 `elapsed_ms` 埋点，跑满一个交易日对比新旧一致性。日K/分钟K/列表等链路待样板验证通过再逐条迁，**不搞一次性大重构**。
**明确不做**：不引入 LangGraph/MongoDB/Redis/ChromaDB；不做多 agent 辩论；不抄 `app/` 下任何代码（专有许可）；不以 akshare 作主力源。

**风险提醒**：① 混合许可——可自由用的只有 `tradingagents/`、`cli/`、`docs/`、`tests/`，最像的两块在 `app/` 下，**借鉴设计不复制代码**；② 官网宣传页 65,391 stars 与 API 实测 32,739 不符，**引用以 API 为准**；③ `system/02 §4.1` 硬约束——**腾讯源默认前复权**，接入必须与东财不复权逐日校准（>0.5% 弃用），此为对齐层解决不了的语义差异，须靠源口径声明 + 契约测试兜住。
