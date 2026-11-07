"""
AI智能分析总结服务 - 多模型融合架构

模型组合（D56 起）：
1. 云知声 U2 (主力模型) - 原生智能体大模型，长上下文（160k）+ 深度推理
2. DeepSeek V4 (备用模型) - 金融分析见长，作为 U2 不可用时的备份

融合策略：
- ensemble: 双模型并行生成 → 智能合并（用于重要总结）
- cascade: 主模型优先 → 失败降级备用模型 → 本地规则（用于通用分析）
- primary_only: 仅使用主模型（用于快速响应）

协议说明（D56 实测）：
- 云知声同时提供 OpenAI 兼容（/v1/chat/completions）与 Anthropic 原生
  （/anthropic/v1/messages）两套端点。本项目默认用 **OpenAI 兼容**端点
  （响应结构与既有代码一致，改动最小），但保留 Anthropic 原生支持
  （protocol='anthropic'），便于将来切到 thinking 块等 Anthropic 特性。
- 云知声鉴权头 `x-api-key` 与 `Authorization: Bearer` 两者都接受。
- **重要**：u2 系列是思考型模型，思考 token 计入 max_tokens；
  max_tokens 过小 → content=null + finish_reason=length（正文全丢）。
  项目默认 UNISOUND_MAX_TOKENS=1500，实测足够覆盖 500 字总结。
- 注意：请求 model="u2" 时服务端实际路由到 u2-flash（响应 model 字段回显
  u2-flash）；显式写 "u2-flash" 亦可，但 max_tokens 需更大（实测 100 会
  content=null）。项目用 "u2" + 1500。

分析模式：
1. 早盘总结（12:15）: 上午市场回顾 + 板块热点 + 下午展望
2. 全日总结（18:15）: 全天复盘 + 技术面分析 + 明日展望
3. 实时分析: 根据市场变化动态生成
"""
from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from dataclasses import dataclass

import httpx
import pandas as pd
from loguru import logger

from ...config import get_settings
from ...models.market import MarketState, MarketType
from ...models.signal import Signal, SignalAction

# === 模型配置 ===

@dataclass
class ModelConfig:
    """单个模型的配置"""
    name: str
    model_id: str
    api_key: str
    base_url: str
    api_url: str = ""
    auth_header: str = "Authorization"  # "Authorization" 或 "api-key"
    timeout: int = 45
    weight: float = 1.0  # 融合权重（同时决定 cascade 优先级：越大越先用）
    supports_thinking: bool = False
    enabled: bool = True
    protocol: str = "openai"  # "openai" | "anthropic"
    max_tokens: int = 1500  # 该模型的 max_tokens（思考型模型需留足）

    def __post_init__(self):
        if self.protocol == "anthropic":
            # Anthropic 原生：base_url 形如 .../anthropic/v1 → 追加 /messages
            self.api_url = f"{self.base_url.rstrip('/')}/messages"
        else:
            self.api_url = f"{self.base_url.rstrip('/')}/chat/completions"


class _RateLimiter:
    """客户端请求限速器（云知声平台限额 RPM 100 / TPM 10,000,000）

    D57 实测：项目当前每日 AI 调用仅 3 轮（集合竞价/早盘/全日），
    ensemble 展开后瞬时峰值 ≈ 3 RPM —— 占限额 3%，安全边际极大。
    但若将来加"盘中实时总结"之类的高频调用，可能撞上 100 RPM。

    双保险：
    1. 滑动窗口限速：限制本进程发起的最大请求速率（默认 20 RPM）
    2. 429 退避重试：平台真限流时指数退避重试，而不是直接失败
    """

    def __init__(self, max_rpm: int = 20):
        self.max_rpm = max(1, max_rpm)
        self._lock = threading.Lock()
        self._times: list[float] = []

    def acquire(self) -> None:
        """阻塞直到可以发出下一个请求"""
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - 60.0
                self._times = [t for t in self._times if t > cutoff]
                if len(self._times) < self.max_rpm:
                    self._times.append(now)
                    return
                wait = self._times[0] + 60.0 - now
            time.sleep(max(0.1, wait))


# 全进程共享的限速器（云知声与 DeepSeek 共用，避免各自打满）
_RATE_LIMITER = _RateLimiter(max_rpm=20)


@dataclass
class ModelResponse:
    """模型返回结果"""
    model_name: str
    text: str
    tokens_used: int = 0
    latency_ms: float = 0
    success: bool = True
    error: str = ""


# === 多模型融合AI总结 ===

class AISummary:
    def __init__(self):
        settings = get_settings()

        # 注册模型
        self.models: dict[str, ModelConfig] = {}

        # 云知声 U2（D56：主力模型，第一优先）
        # 权重最高 → cascade 优先尝试、ensemble 合并时用它做合并者
        uni_key = settings.UNISOUND_API_KEY
        if uni_key:
            self.models["unisound"] = ModelConfig(
                name="云知声 U2",
                model_id=settings.UNISOUND_MODEL or "u2",
                api_key=uni_key,
                base_url=settings.UNISOUND_BASE_URL
                or "https://maas-api.unisound.com/v1",
                auth_header="Authorization",  # 实测 x-api-key 与 Bearer 都接受
                weight=0.7,  # 主力权重
                supports_thinking=True,
                max_tokens=int(settings.UNISOUND_MAX_TOKENS or 2500),
                timeout=90,  # 思考型模型响应偏慢（实测 11~16s），给足超时
            )

        # DeepSeek V4（D56：降级为备用模型）
        ds_key = settings.DEEPSEEK_API_KEY or settings.OPENAI_API_KEY
        if ds_key:
            self.models["deepseek"] = ModelConfig(
                name="DeepSeek V4",
                model_id="deepseek-chat",
                api_key=ds_key,
                base_url=settings.OPENAI_BASE_URL or "https://api.deepseek.com/v1",
                weight=0.3,  # 备用权重（低于 U2）
            )

        # 小米 MiMo（D56 已下线）：密钥存在也不再注册。
        # 原因：生产日志持续 "MiMo v2.5调用失败: HTTP 401 Invalid API Key"
        # —— 密钥已失效，保留只会每次摘要白等一次 401（D53 已加进程内禁用，
        # 但那是治标）。若要恢复，改回注册即可（配置字段仍在）。

        self.primary_model = (
            "unisound" if "unisound" in self.models
            else ("deepseek" if "deepseek" in self.models else None)
        )

        # 缓存
        self._cache: dict[str, dict] = {}
        self._cache_ttl = 300  # 5分钟

        model_names = [f"{m.name}(w={m.weight})" for m in self.models.values()]
        logger.info(f"🤖 AI总结初始化: {len(self.models)}个模型 | {', '.join(model_names) or '无可用模型'}")

    # === 公开接口 ===

    def generate_summary(self, market: MarketState, signals: list[Signal],
                         sectors: list[dict] | None = None,
                         summary_type: str = "general",
                         alerts: list[dict] | None = None,
                         etf_data: list[dict] | None = None,
                         news: list[dict] | None = None) -> dict:
        """
        生成AI智能分析总结（多模型融合）

        融合策略选择：
        - morning_review / daily_review → ensemble（双模型融合）
        - general → cascade（主模型优先，降级）
        """
        # 缓存检查
        cache_key = f"{summary_type}_{market.score:.0f}_{len(signals)}"
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            if time.time() - cached.get('_ts', 0) < self._cache_ttl:
                logger.debug(f"使用缓存的AI总结: {cache_key}")
                return cached

        if not self._enabled_models():
            logger.warning("无可用AI模型，使用本地生成")
            return self._generate_local(market, signals, sectors)

        try:
            # 重要总结使用ensemble融合，通用总结使用cascade
            if summary_type in ("morning_review", "daily_review") and len(self._enabled_models()) >= 2:
                result = self._generate_ensemble(market, signals, sectors, summary_type, alerts, etf_data, news)
            else:
                result = self._generate_cascade(market, signals, sectors, summary_type, alerts, etf_data, news)

            result['_ts'] = time.time()
            self._cache[cache_key] = result
            return result

        except Exception as e:
            logger.error(f"AI总结生成失败: {e}")
            return self._generate_local(market, signals, sectors)

    # === 融合策略1: Ensemble（双模型并行 + 智能合并）===

    def _generate_ensemble(self, market, signals, sectors, summary_type, alerts, etf_data, news=None) -> dict:
        """
        双模型并行生成 → 智能合并

        流程：
        1. 构建统一的Prompt
        2. 并行调用全部已启用模型（U2 主力 + DeepSeek 备用）
        3. 使用主模型合并多份分析结果
        4. 输出融合后的总结
        """
        market_info = self._format_market_info(market)
        signal_info = self._format_signal_info(signals)
        sector_info = self._format_sector_info(sectors) if sectors else "无板块数据"
        alert_info = self._format_alert_info(alerts) if alerts else "今日无异动"
        etf_info = self._format_etf_info(etf_data) if etf_data else ""
        news_info = self._format_news_info(news) if news else ""

        # 构建Prompt
        if summary_type == "morning_review":
            system_prompt, user_prompt = self._build_morning_prompt(
                market_info, signal_info, sector_info, alert_info, etf_info, news_info)
        elif summary_type == "daily_review":
            system_prompt, user_prompt = self._build_daily_prompt(
                market_info, signal_info, sector_info, alert_info, etf_info, news_info)
        else:
            system_prompt, user_prompt = self._build_general_prompt(
                market_info, signal_info, sector_info, alert_info, news_info)

        logger.info(f"🔀 Ensemble模式: 并行调用{len(self.models)}个模型生成{summary_type}...")

        # 并行调用所有模型
        responses = self._parallel_call(system_prompt, user_prompt)

        # 分离成功和失败的结果
        successes = [r for r in responses if r.success]
        failures = [r for r in responses if not r.success]

        for f in failures:
            logger.warning(f"⚠️ {f.model_name}调用失败: {f.error}")

        if not successes:
            logger.error("所有模型调用失败，降级到本地生成")
            return self._generate_local(market, signals, sectors)

        if len(successes) == 1:
            # 只有一个模型成功，直接使用
            r = successes[0]
            logger.info(f"✅ 仅{r.model_name}成功，直接使用 ({r.latency_ms:.0f}ms)")
            return self._build_result(r.text, market_info, signal_info, sector_info,
                                       alert_info, summary_type, r.model_name, r.tokens_used)

        # 多模型成功 → 智能合并
        logger.info(f"🧬 融合{len(successes)}个模型结果...")
        merged = self._merge_responses(successes, summary_type, market_info)

        return self._build_result(
            merged["text"], market_info, signal_info, sector_info,
            alert_info, summary_type,
            merged["model"], merged["tokens"]
        )

    def _merge_responses(self, responses: list[ModelResponse], summary_type: str,
                         market_info: str) -> dict:
        """
        智能合并多个模型的分析结果

        策略：将两份分析交给主模型做最终合并，要求取长补短
        """
        # 构建合并Prompt
        parts = []
        for idx, r in enumerate(responses):
            label = chr(65 + idx)  # A / B / C...
            parts.append(f"### 分析师{label}（{r.model_name}）的分析：\n{r.text}")

        n = len(responses)
        merge_prompt = f"""以下是{n}位AI分析师对同一市场数据的独立分析。请合并这些分析，取长补短，生成一份更准确、更全面的最终报告。

合并要求：
- 保留多位分析师都认同的观点（高置信度）
- 对于分歧点，给出综合判断或标注"存在分歧"
- 补充任一方遗漏的重要信息
- 最终输出格式与原始分析格式一致
- 不要提及"分析师A/B/C"，直接输出合并后的分析

市场数据：{market_info}

{chr(10).join(parts)}

请输出合并后的最终分析报告："""

        # 用主模型做合并
        primary = self.models.get(self.primary_model)
        if not primary:
            primary = next(iter(self.models.values()))

        try:
            merge_response = self._call_single_model(
                primary,
                "你是一位资深金融分析师，擅长综合多方观点形成全面判断。",
                merge_prompt,
                # D66：不显式传 max_tokens —— 由主模型 cfg.max_tokens 决定。
                # 原写死 1500 会把 U2 的配置预算（2500）砍掉，触发
                # finish_reason=length 空正文（ECS 18:10 融合阶段实测 8 次）。
                max_tokens=None
            )

            if merge_response.success:
                total_tokens = sum(r.tokens_used for r in responses) + merge_response.tokens_used
                model_names = "+".join(r.model_name for r in responses)
                logger.info(
                    f"✅ 融合完成 | 模型: {model_names} → 合并 | "
                    f"总tokens: {total_tokens}"
                )
                return {
                    "text": merge_response.text,
                    "model": f"ensemble({model_names})",
                    "tokens": total_tokens,
                }
        except Exception as e:
            logger.warning(f"合并失败，使用加权选择: {e}")

        # 合并失败 → 回退：优先选权重最高的模型（U2 > DeepSeek），
        # 权重相同时选正文更长的那个。
        # D56 修正：原实现先按权重算出 best，紧接着又被"选最长"覆盖，
        # 权重排序形同虚设。
        def _rank(r: ModelResponse) -> tuple[float, int]:
            weight = max(
                (cfg.weight for cfg in self.models.values()
                 if cfg.name == r.model_name),
                default=0.0,
            )
            return (weight, len(r.text))

        best = max(responses, key=_rank)
        return {
            "text": best.text,
            "model": best.model_name,
            "tokens": best.tokens_used,
        }

    # === 融合策略2: Cascade（主模型优先 → 降级）===

    def _generate_cascade(self, market, signals, sectors, summary_type, alerts, etf_data, news=None) -> dict:
        """
        级联调用：主模型 → 备用模型 → 本地规则

        适用于通用分析和快速响应场景
        """
        market_info = self._format_market_info(market)
        signal_info = self._format_signal_info(signals)
        sector_info = self._format_sector_info(sectors) if sectors else "无板块数据"
        alert_info = self._format_alert_info(alerts) if alerts else "今日无异动"
        etf_info = self._format_etf_info(etf_data) if etf_data else ""
        news_info = self._format_news_info(news) if news else ""

        # 构建Prompt
        if summary_type == "morning_review":
            system_prompt, user_prompt = self._build_morning_prompt(
                market_info, signal_info, sector_info, alert_info, etf_info, news_info)
        elif summary_type == "daily_review":
            system_prompt, user_prompt = self._build_daily_prompt(
                market_info, signal_info, sector_info, alert_info, etf_info, news_info)
        else:
            system_prompt, user_prompt = self._build_general_prompt(
                market_info, signal_info, sector_info, alert_info, news_info)

        # 按权重排序，逐个尝试
        sorted_models = sorted(self.models.values(), key=lambda m: m.weight, reverse=True)

        for model_cfg in sorted_models:
            logger.info(f"🔄 Cascade: 尝试 {model_cfg.name}...")
            resp = self._call_single_model(model_cfg, system_prompt, user_prompt)

            if resp.success:
                logger.info(f"✅ {model_cfg.name} 成功 ({resp.latency_ms:.0f}ms, {resp.tokens_used}tokens)")
                return self._build_result(
                    resp.text, market_info, signal_info, sector_info,
                    alert_info, summary_type, resp.model_name, resp.tokens_used
                )
            else:
                logger.warning(f"⚠️ {model_cfg.name} 失败: {resp.error}，尝试下一个...")

        # 所有模型失败
        logger.error("Cascade: 所有模型均失败，降级到本地生成")
        return self._generate_local(market, signals, sectors)

    # === 底层API调用 ===

    def _parallel_call(self, system_prompt: str, user_prompt: str,
                       max_tokens: int | None = None) -> list[ModelResponse]:
        """并行调用所有可用模型"""
        results = []
        enabled = self._enabled_models()

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(enabled))) as executor:
            futures = {}
            for name, cfg in enabled.items():
                if not cfg.enabled:
                    continue
                future = executor.submit(
                    self._call_single_model, cfg, system_prompt, user_prompt, max_tokens
                )
                futures[future] = name

            for future in concurrent.futures.as_completed(futures, timeout=60):
                name = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except concurrent.futures.TimeoutError:
                    results.append(ModelResponse(
                        model_name=name, text="", success=False, error="超时"
                    ))
                except Exception as e:
                    results.append(ModelResponse(
                        model_name=name, text="", success=False, error=str(e)
                    ))

        return results

    def _call_single_model(self, cfg: ModelConfig, system_prompt: str,
                           user_prompt: str, max_tokens: int | None = None,
                           max_retries: int = 2) -> ModelResponse:
        """调用单个模型（支持 OpenAI 兼容与 Anthropic 原生两种协议）

        D57：429 平台限流时指数退避重试（云知声限额 RPM 100 / TPM 10M）。
        当前项目用量峰值仅 3 RPM（占限额 3%），重试只是兜底——
        防止将来加高频调用时整轮总结被限流打断。
        """
        attempt = 0
        while True:
            resp = self._call_once(cfg, system_prompt, user_prompt, max_tokens)
            if resp.success:
                return resp
            # 可重试的两类瞬时问题：
            # ① 限流(429) —— 平台 RPM/TPM 限制
            # ② 空响应(finish_reason=length) —— 思考型模型偶发思考超出
            #    max_tokens 预算（实测约 1/10 概率），重试一次即可拿到正文。
            #    这不是模型故障，不能禁用模型（否则整轮总结直接废掉）。
            retryable = (resp.error.startswith("限流(429)")
                         or resp.error.startswith("空响应"))
            if not retryable or attempt >= max_retries:
                return resp
            delay = 2.0 * (2 ** attempt)
            attempt += 1
            logger.warning(
                f"{cfg.name} {resp.error}，{delay:.0f}s 后重试"
                f"（{attempt}/{max_retries}）")
            time.sleep(delay)

    def _call_once(self, cfg: ModelConfig, system_prompt: str,
                   user_prompt: str, max_tokens: int | None = None) -> ModelResponse:
        """发起一次模型请求（不重试；重试逻辑在 _call_single_model）"""
        start = time.time()

        # D66：调用方不指定时，直接用模型自身预算（思考型 U2 = 2500）。
        # 原实现默认 1500 + min() → 无论 .env 配 2500，ensemble/cascade
        # 实际都只发 1500，ECS 近 7 天因此出现 8 次 finish_reason=length
        # 空正文、5 次重试、1 次模型级联失败。
        max_tokens = cfg.max_tokens if max_tokens is None else min(max_tokens, cfg.max_tokens)

        # 构建请求头
        if cfg.auth_header == "api-key":
            headers = {"api-key": cfg.api_key, "Content-Type": "application/json"}
        else:
            headers = {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"}
        if cfg.protocol == "anthropic":
            headers["anthropic-version"] = "2023-06-01"

        # 构建payload（两种协议的字段布局不同）
        if cfg.protocol == "anthropic":
            payload = {
                "model": cfg.model_id,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
                "temperature": 0.7,
                "max_tokens": max_tokens,
                "stream": False,
            }
        else:
            payload = {
                "model": cfg.model_id,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.7,
                "max_tokens": max_tokens,
                "top_p": 0.9,
                "frequency_penalty": 0.3,
                "presence_penalty": 0.3,
                "stream": False,
            }

        # MiMo 支持深度思考模式（D56：MiMo 已下线，此分支保留兼容）
        if cfg.supports_thinking and cfg.model_id.startswith("mimo"):
            payload["thinking"] = {"type": "disabled"}  # 总结场景不需要深度思考，加速响应

        try:
            _RATE_LIMITER.acquire()  # 客户端限速（远低于平台 RPM 100）
            with httpx.Client(timeout=cfg.timeout) as client:
                response = client.post(cfg.api_url, json=payload, headers=headers)

                latency = (time.time() - start) * 1000

                if response.status_code == 200:
                    data = response.json()
                    text, tokens = self._parse_response(cfg, data)

                    if not text:
                        # 思考型模型在 max_tokens 内思考不完时，
                        # content=null + finish_reason=length（正文全丢）。
                        # 这不是网络/鉴权问题，不应禁用模型——但必须报失败，
                        # 否则会把空正文当成有效总结推送到钉钉。
                        reason = ""
                        if cfg.protocol == "openai":
                            choice = (data.get('choices') or [{}])[0]
                            reason = choice.get('finish_reason', '') or ''
                        else:
                            reason = data.get('stop_reason', '') or ''
                        logger.warning(
                            f"{cfg.name} 返回空正文（finish_reason={reason}），"
                            f"疑似 max_tokens={max_tokens} 不足以容纳思考过程")
                        return ModelResponse(
                            model_name=cfg.name, text="", success=False,
                            error=f"空响应(finish_reason={reason})",
                            latency_ms=latency,
                        )

                    return ModelResponse(
                        model_name=cfg.name,
                        text=text,
                        tokens_used=tokens,
                        latency_ms=latency,
                        success=True,
                    )
                elif response.status_code == 429:
                    # D57：平台限流（RPM 100 / TPM 10M）→ 交由上层退避重试
                    return ModelResponse(
                        model_name=cfg.name, text="", success=False,
                        error="限流(429)", latency_ms=latency
                    )
                elif response.status_code in (401, 403):
                    # D53：密钥无效/被拒 —— 进程内禁用该模型，不再重复白等。
                    # 原实现每次摘要都发一次请求吃 401（生产 10-30 记录
                    # "MiMo v2.5调用失败: HTTP 401 Invalid API Key"）。
                    # D56：MiMo 已下线，云知声 U2 密钥若失效同样走此路径。
                    # 注意：401 也可能是额度/权限问题，重试无意义——需运维更新
                    # .env 中的密钥后重启服务。
                    self._disable_model(cfg, response.status_code)
                    return ModelResponse(
                        model_name=cfg.name, text="", success=False,
                        error=f"鉴权失败({response.status_code})，模型已禁用待更新密钥",
                        latency_ms=latency,
                    )
                else:
                    return ModelResponse(
                        model_name=cfg.name, text="", success=False,
                        error=f"HTTP {response.status_code}: {response.text[:100]}",
                        latency_ms=latency,
                    )

        except httpx.TimeoutException:
            latency = (time.time() - start) * 1000
            return ModelResponse(
                model_name=cfg.name, text="", success=False,
                error=f"超时({cfg.timeout}s)", latency_ms=latency,
            )
        except Exception as e:
            latency = (time.time() - start) * 1000
            return ModelResponse(
                model_name=cfg.name, text="", success=False,
                error=str(e), latency_ms=latency,
            )

    @staticmethod
    def _parse_response(cfg: ModelConfig, data: dict) -> tuple[str, int]:
        """解析响应 → (正文, tokens)

        OpenAI 兼容：data.choices[0].message.content
          —— 思考型模型另有 reasoning_content 字段（不含正文，忽略）
        Anthropic 原生：data.content[] 逐块，type=='text' 才是正文，
          type=='thinking' 是思考过程（不含正文，忽略）
        """
        if cfg.protocol == "anthropic":
            blocks = data.get('content') or []
            text = ''.join(b.get('text', '') for b in blocks
                           if isinstance(b, dict) and b.get('type') == 'text')
            usage = data.get('usage') or {}
            tokens = usage.get('input_tokens', 0) + usage.get('output_tokens', 0)
            return text.strip(), tokens

        choices = data.get('choices') or []
        if not choices:
            return '', 0
        message = choices[0].get('message') or {}
        text = message.get('content') or ''
        usage = data.get('usage') or {}
        tokens = usage.get('total_tokens', 0)
        return text.strip(), tokens

    # === 结果构建 ===

    def _disable_model(self, cfg: ModelConfig, status_code: int = 401) -> None:
        """禁用鉴权失败的模型（进程内生效，不再重复请求）

        D53：生产日志 "MiMo v2.5调用失败: HTTP 401 Invalid API Key" —— 密钥
        已失效但每次摘要仍发一次请求白等。禁用后仅用剩余模型生成，
        待运维更新 .env 密钥并重启服务后自动恢复。
        """
        if not cfg.enabled:
            return
        cfg.enabled = False
        logger.error(
            f"🚫 模型 {cfg.name} 鉴权失败(HTTP {status_code})，已禁用（需更新密钥后重启服务）")

    def _enabled_models(self) -> dict:
        """当前启用的模型集合"""
        return {k: v for k, v in self.models.items() if v.enabled}

    def _build_result(self, text: str, market_info: str, signal_info: str,
                      sector_info: str, alert_info: str, summary_type: str,
                      model_name: str, tokens: int) -> dict:
        return {
            "market": market_info,
            "signals": signal_info,
            "sectors": sector_info,
            "alerts": alert_info,
            "recommendation": text,
            "full_text": text,
            "summary_type": summary_type,
            "model": model_name,
            "tokens_used": tokens,
        }

    # === Prompt模板 ===

    def _build_morning_prompt(self, market_info, signal_info, sector_info, alert_info, etf_info, news_info=""):
        """早盘总结Prompt（12:15）"""
        system = """你是一位资深A股量化分析师，擅长技术分析、资金流向研判、新闻事件解读和产业链推理。
请基于上午盘面数据和最新消息，给出专业、深度、可操作的早盘总结。

要求：
- 语言精炼，适合手机阅读，但分析要有深度
- 数据驱动，每个观点都有数据支撑
- 结合多维度分析：技术面+资金面+情绪面+板块轮动+事件驱动
- 产业链推理：看到新闻事件时，推导上下游受益/受损公司/板块（如：某国停产钨→钨类上市公司+下游硬质合金受益）
- 给出明确的下午操作策略（仓位、方向、标的类型）
- 总字数控制在500字以内
- 使用emoji增强可读性"""

        user = f"""## 上午市场数据

{market_info}

## 板块表现
{sector_info}

## 交易信号
{signal_info}

## 异动记录
{alert_info}

{f"## ETF状态{chr(10)}{etf_info}" if etf_info else ""}

{f"## 新闻与事件{chr(10)}{news_info}" if news_info else ""}

请按以下格式输出早盘总结：

📊 **早盘回顾**（上午走势概述，核心矛盾是什么，主力资金态度如何）

🔥 **热点板块深度分析**（3-4个强势板块，分析驱动因素：政策/业绩/事件催化，判断持续性）

❄️ **弱势板块**（1-2个领跌板块，分析下跌原因）

📰 **新闻事件与产业链推理**（结合最新消息分析：事件驱动型机会，推导上下游产业链受益/受损标的，如停产/涨价/政策等事件的传导逻辑）

💰 **资金动向**（主力资金流向分析，大单净买入/卖出方向，北向资金态度）

📈 **技术面研判**（大盘支撑/压力位分析，量价关系，MACD/KDJ等关键指标）

⚠️ **风险提示**（需要警惕的信号，如量价背离、高位放量等）

🎯 **下午策略**（具体操作建议：仓位建议、方向选择、关注标的类型）"""

        return system, user

    def _build_daily_prompt(self, market_info, signal_info, sector_info, alert_info, etf_info, news_info=""):
        """全日总结Prompt（18:15）"""
        system = """你是一位资深A股量化分析师，擅长全天复盘、多维度分析、新闻事件解读、产业链推理和次日策略制定。
请基于全天数据和最新消息，给出专业、全面、有深度的日终总结。

要求：
- 复盘今日行情，找出核心矛盾和主力意图
- 多维度分析：技术面+资金面+情绪面+政策面+外盘影响+事件驱动
- 板块轮动分析：今日主线是什么，明天可能切换到哪里
- 产业链推理：从新闻事件推导上下游投资机会（如：某国停产钨→钨矿企业利好+硬质合金企业成本压力+替代品企业机会）
- 预判明日走势，给出具体操作计划
- 总字数控制在600字以内
- 使用emoji增强可读性"""

        user = f"""## 今日市场数据

{market_info}

## 板块表现
{sector_info}

## 交易信号
{signal_info}

## 异动记录
{alert_info}

{f"## ETF状态{chr(10)}{etf_info}" if etf_info else ""}

{f"## 新闻与事件{chr(10)}{news_info}" if news_info else ""}

请按以下格式输出全日总结：

📈 **今日复盘**（全天走势概述，核心矛盾是什么，多空博弈结果）

🏆 **强势板块深度分析**（3-5个领涨板块，分析驱动因素：政策/业绩/事件催化，判断持续性，是否有龙头效应）

📉 **弱势板块**（2-3个领跌板块，分析下跌原因：利空消息/获利回吐/估值压力）

📰 **新闻事件与产业链推理**（结合最新消息深度分析：
  - 事件驱动型机会：推导上下游产业链受益/受损标的
  - 政策利好/利空：分析影响的具体行业和龙头公司
  - 涨价/停产/供需变化：推导受益链条，如停产钨→钨矿股+替代材料股
  - 外围事件：对A股的传导路径分析）

💰 **资金动向深度解读**（主力资金流向，北向资金态度，融资融券变化，大宗交易信号）

📊 **技术面全面研判**（大盘关键支撑/压力位，均线系统状态，MACD/KDJ/RSI等指标，量价关系分析）

🌍 **外盘与政策**（外围市场对A股影响，今日重要政策/消息解读，明天关注的事件日历）

🔮 **明日展望与策略**（预判明日走势，操作计划：仓位建议、方向选择、关注标的类型、进场时机）

⚠️ **风险提示**（需要关注的风险因素，最悲观情景和最乐观情景）"""

        return system, user

    def _build_general_prompt(self, market_info, signal_info, sector_info, alert_info, news_info=""):
        """通用分析Prompt"""
        system = """你是一位专业的A股量化分析师。请基于市场数据和最新消息给出有深度、可操作的分析和策略建议。
多维度分析：技术面+资金面+情绪面+事件驱动。产业链推理：从新闻推导上下游投资机会。回答要具体、可执行，控制在350字以内。"""

        user = f"""## 市场状态
{market_info}

## 板块表现
{sector_info}

## 交易信号
{signal_info}

## 异动记录
{alert_info}

{f"## 新闻与事件{chr(10)}{news_info}" if news_info else ""}

请分析：
1. 📊 市场状态（核心矛盾和多空力量对比）
2. 🔥 热点板块（2-3个，分析驱动因素和持续性）
3. 📰 新闻事件与产业链机会（从最新消息推导上下游受益/受损标的和投资逻辑）
4. 💰 资金动向（主力流向和意图）
5. 🎯 操作建议（具体可执行：仓位、方向、标的类型）
6. ⚠️ 风险提示（最多3条，标注严重度）"""

        return system, user

    # === 数据格式化 ===

    def _format_market_info(self, market: MarketState) -> str:
        type_desc = {
            MarketType.BULL: "牛市",
            MarketType.BULL_WEAK: "弱牛",
            MarketType.SIDEWAYS: "震荡",
            MarketType.BEAR: "熊市",
        }
        market_name = type_desc.get(market.type, "未知")
        return (
            f"市场类型: {market_name} | 评分: {market.score:.0f}/100\n"
            f"涨跌比: {market.breadth:.1%} | "
            f"上涨家数占比: {market.breadth:.1%}"
        )

    def _format_signal_info(self, signals: list[Signal]) -> str:
        if not signals:
            return "今日无交易信号"
        buy_signals = [s for s in signals if s.action == SignalAction.BUY]
        watch_signals = [s for s in signals if s.action == SignalAction.WATCH]
        lines = []
        if buy_signals:
            lines.append(f"买入信号 {len(buy_signals)} 个：")
            for s in buy_signals[:5]:
                lines.append(
                    f"  - {s.name}({s.code}) 评分:{s.score:.0f} "
                    f"仓位:{s.position:.0%} 原因:{s.reason}"
                )
        if watch_signals:
            lines.append(f"观察信号 {len(watch_signals)} 个：")
            for s in watch_signals[:3]:
                lines.append(f"  - {s.name}({s.code}) 评分:{s.score:.0f}")
        return "\n".join(lines)

    def _format_sector_info(self, sectors: list[dict]) -> str:
        if not sectors:
            return "无板块数据"
        hot = [s for s in sectors if s.get('avg_change', 0) > 0][:5]
        cold = [s for s in sectors if s.get('avg_change', 0) < 0][:3]
        lines = []
        if hot:
            lines.append("🔴 强势板块：")
            for s in hot:
                lines.append(f"  {s['name']}({s.get('avg_change', 0):+.2f}%)")
        if cold:
            lines.append("🟢 弱势板块：")
            for s in cold:
                lines.append(f"  {s['name']}({s.get('avg_change', 0):+.2f}%)")
        return "\n".join(lines)

    def _format_alert_info(self, alerts: list[dict]) -> str:
        if not alerts:
            return "无异动"
        lines = [f"共{len(alerts)}次异动："]
        for a in alerts[:5]:
            lines.append(f"  - [{a.get('level', 'info')}] {a.get('title', '')}: {a.get('detail', '')}")
        return "\n".join(lines)

    def _format_etf_info(self, etf_data: list[dict]) -> str:
        if not etf_data:
            return ""
        lines = []
        for etf in etf_data[:5]:
            name = etf.get('name', '')
            change = etf.get('change', 0)
            signal = etf.get('signal', '')
            lines.append(f"  {name}: {change:+.2f}% ({signal})")
        return "\n".join(lines)

    def _format_news_info(self, news: list[dict]) -> str:
        if not news:
            return ""
        lines = [f"近期重要新闻/事件({len(news)}条)："]
        for n in news[:8]:
            title = n.get('title', '')
            source = n.get('source', '')
            lines.append(f"  - {title} ({source})" if source else f"  - {title}")
        return "\n".join(lines)

    # === 股票分析（多模型融合）===

    def generate_stock_analysis(self, code: str, name: str, df: pd.DataFrame, indicators: dict) -> dict:
        """生成单只股票的AI分析报告（cascade模式）"""
        if not self.models:
            return self._generate_local_stock_analysis(code, name, df, indicators)

        prompt = self._build_stock_analysis_prompt(code, name, df, indicators)
        system = "你是一个专业的量化分析师，擅长A股技术分析。请用中文回答，给出具体、可操作的建议。"

        # Cascade: 按权重逐个尝试
        sorted_models = sorted(self.models.values(), key=lambda m: m.weight, reverse=True)
        for cfg in sorted_models:
            resp = self._call_single_model(cfg, system, prompt, max_tokens=1000)
            if resp.success:
                return self._parse_ai_stock_analysis(resp.text, code, name, df, indicators)

        return self._generate_local_stock_analysis(code, name, df, indicators)

    def _build_stock_analysis_prompt(self, code, name, df, indicators):
        current_price = df['close'].iloc[-1]
        change_pct = ((df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100) if len(df) > 1 else 0
        high_20 = df['high'].rolling(20).max().iloc[-1] if len(df) >= 20 else current_price
        low_20 = df['low'].rolling(20).min().iloc[-1] if len(df) >= 20 else current_price

        # 高级特征
        breakout = indicators.get('breakout_strength_20d', 'N/A')
        vol_ratio = indicators.get('volume_ratio', 'N/A')
        bollinger = indicators.get('bollinger_position', 'N/A')
        atr_pct = indicators.get('atr_pct', 'N/A')

        return f"""分析股票：{name}({code})

当前数据：
- 价格：{current_price:.2f}，涨跌幅：{change_pct:.2f}%
- 20日最高：{high_20:.2f}，20日最低：{low_20:.2f}
- RSI：{indicators.get('rsi', 'N/A')}，MACD：{indicators.get('macd', 'N/A')}
- 均线：MA5={indicators.get('ma5', 'N/A')}, MA10={indicators.get('ma10', 'N/A')}, MA20={indicators.get('ma20', 'N/A')}
- 突破强度(20d)：{breakout}，量比：{vol_ratio}
- 布林带位置：{bollinger}，ATR波动率：{atr_pct}%

请严格按JSON格式返回分析结果（无需markdown代码块）：
{{
  "verdict": "买入/观望/卖出",
  "score": 82,
  "trend": "看多/震荡/看空",
  "reasoning": "一句话核心逻辑",
  "key_levels": {{"support": 15.2, "resistance": 17.8}},
  "risk_alerts": ["风险点1", "风险点2"],
  "catalysts": ["利好1", "利好2"],
  "checklist": [
    {{"item": "趋势判断", "pass": true}},
    {{"item": "RSI状态", "pass": true}},
    {{"item": "MACD状态", "pass": true}},
    {{"item": "量价配合", "pass": false}},
    {{"item": "板块联动", "pass": true}}
  ]
}}"""

    def _parse_ai_stock_analysis(self, ai_text, code, name, df, indicators):
        """解析AI股票分析结果（支持结构化 JSON 和自由文本）"""
        try:
            text = ai_text.strip()
            if text.startswith('```'):
                text = text.split('\n', 1)[1] if '\n' in text else text[3:]
            text = text.removesuffix('```')
            text = text.strip()

            data = json.loads(text)
            key_levels = data.get('key_levels', {})
            return {
                "code": code, "name": name,
                "verdict": data.get('verdict', '观望'),
                "score": int(data.get('score', 50)),
                "trend": data.get('trend', '震荡'),
                "reasoning": data.get('reasoning', ''),
                "support_price": key_levels.get('support', df['close'].iloc[-1] * 0.95),
                "resistance_price": key_levels.get('resistance', df['close'].iloc[-1] * 1.05),
                "risk_alerts": data.get('risk_alerts', []),
                "catalysts": data.get('catalysts', []),
                "checklist": data.get('checklist', []),
                "ai_raw": ai_text,
                "current_price": df['close'].iloc[-1],
                "indicators": indicators,
            }
        except (json.JSONDecodeError, KeyError, TypeError):
            # 降级: 返回自由文本
            return {
                "code": code, "name": name,
                "verdict": "观望", "score": 50, "trend": "震荡",
                "reasoning": ai_text[:200],
                "ai_analysis": ai_text,
                "risk_alerts": [], "catalysts": [], "checklist": [],
                "current_price": df['close'].iloc[-1],
                "indicators": indicators,
            }

    def _generate_local_stock_analysis(self, code, name, df, indicators):
        """本地生成股票分析（降级方案）"""
        current_price = df['close'].iloc[-1]
        rsi = indicators.get('rsi', 50)
        macd = indicators.get('macd', 0)
        ma5 = indicators.get('ma5', 0)
        ma20 = indicators.get('ma20', 0)

        if ma5 > ma20:
            trend, score = "看多", 70
        elif ma5 < ma20:
            trend, score = "看空", 30
        else:
            trend, score = "震荡", 50

        risk = None
        if rsi > 70:
            score -= 10; risk = "RSI超买"
        elif rsi < 30:
            score += 10; risk = "RSI超卖"

        action = "买入" if score >= 70 else ("观望" if score >= 50 else "卖出")

        return {
            "code": code, "name": name, "score": score, "action": action, "trend": trend,
            "risk_alerts": [risk] if risk else [], "catalysts": [],
            "support_price": df['low'].rolling(20).min().iloc[-1] if len(df) >= 20 else current_price * 0.95,
            "resistance_price": df['high'].rolling(20).max().iloc[-1] if len(df) >= 20 else current_price * 1.05,
            "checklist": [
                {"item": "趋势判断", "status": "pass" if trend == "看多" else "fail"},
                {"item": "RSI状态", "status": "pass" if 30 < rsi < 70 else "fail"},
                {"item": "MACD状态", "status": "pass" if macd > 0 else "fail"},
            ],
        }

    # === 本地降级生成 ===

    def _generate_local(self, market: MarketState, signals: list[Signal], sectors: list[dict] | None = None) -> dict:
        """本地生成总结（所有API不可用时的降级方案）"""
        type_desc = {
            MarketType.BULL: "牛市", MarketType.BULL_WEAK: "弱牛",
            MarketType.SIDEWAYS: "震荡", MarketType.BEAR: "熊市",
        }
        market_name = type_desc.get(market.type, "未知")
        market_summary = f"📊 市场状态：{market_name}（评分：{market.score:.0f}）涨跌比：{market.breadth:.0%}"

        buy_signals = [s for s in signals if s.action == SignalAction.BUY]
        watch_signals = [s for s in signals if s.action == SignalAction.WATCH]
        signal_lines = []
        if buy_signals:
            signal_lines.append(f"买入信号 {len(buy_signals)} 个")
        if watch_signals:
            signal_lines.append(f"观察信号 {len(watch_signals)} 个")
        signal_summary = "📈 信号提示：" + ("，".join(signal_lines) if signal_lines else "今日无交易信号")

        if market.type == MarketType.BEAR:
            recommendation = "🎯 操作建议：熊市环境，建议空仓观望，等待市场企稳"
        elif not buy_signals:
            recommendation = "🎯 操作建议：暂无合适买入机会，建议耐心等待"
        elif market.type == MarketType.BULL:
            recommendation = "🎯 操作建议：市场强势，可适当加仓，优先选择评分较高的标的"
        elif market.type == MarketType.BULL_WEAK:
            recommendation = "🎯 操作建议：市场动力减弱，建议半仓操作，严格止损"
        else:
            recommendation = "🎯 操作建议：震荡行情，建议小仓位参与，快进快出"

        local = {
            "market": market_summary, "signals": signal_summary,
            "recommendation": recommendation,
            "full_text": f"{market_summary}\n\n{signal_summary}\n\n{recommendation}",
            "summary_type": "local", "model": "local_rule",
        }
        # I-11：AI 全失败时**不整块丢弃**，仍推送已完成的客观数据部分结果，
        # 并显式标注降级（避免用户把规则模板误当成模型研判）。
        local['partial'] = True
        local['degraded'] = True
        local['degraded_reason'] = 'ai_unavailable'
        local['missing_sections'] = ['ai_narrative']
        try:
            from ...config import get_settings
            notify_partial = bool(get_settings().AI_PARTIAL_NOTIFY)
        except Exception:
            notify_partial = True
        if notify_partial:
            local['full_text'] += (
                "\n\n⚠️ 部分结果：AI 叙述生成失败，以上仅为客观数据与规则结论，"
                "未包含模型研判，请勿视为完整分析。")
        return local
