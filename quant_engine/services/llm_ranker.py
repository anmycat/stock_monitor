"""LLM排序层 - 在L1筛选后加入LLM对候选股的排序推理（借鉴 AlphaSift L2）"""
import json
from typing import List, Dict, Optional
from loguru import logger
from .ai_summary import AISummary, ModelConfig


class LLMRanker:
    """
    LLM排序服务
    
    在L1硬筛 + L2多因子评分之后，利用LLM对Top候选进行
    结构化推理排序，输出重新排列的候选列表 + 理由。
    
    LLM不可用时透传（降级到纯因子排序）。
    """

    def __init__(self):
        self._ai_summary = AISummary()

    def rank(self, candidates: List[Dict], max_candidates: int = 10) -> List[Dict]:
        """
        对候选股进行LLM排序
        
        Args:
            candidates: L2评分后的候选列表，每个包含 code/name/score/factors/indicators
            max_candidates: 最多排序的候选数量
        
        Returns:
            重新排序后的候选列表（附加 llm_reason 字段）
        """
        if not candidates or len(candidates) < 2:
            return candidates

        top = candidates[:max_candidates]

        try:
            prompt = self._build_rank_prompt(top)
            response = self._call_llm(prompt)
            if response:
                ranked = self._parse_rank_response(response, top)
                if ranked:
                    return ranked
        except Exception as e:
            logger.warning(f"LLM ranking failed, falling back to factor sort: {e}")

        return top  # 降级：保持原始因子排序

    def _build_rank_prompt(self, candidates: List[Dict]) -> str:
        """构建排序Prompt"""
        lines = [
            "你是一位专业的A股量化分析师。请对以下候选股票进行综合排序分析。",
            "考虑技术面、基本面、市场情绪等因素，给出你的排序和简要理由。",
            "",
            "候选股票列表：",
        ]

        for i, c in enumerate(candidates, 1):
            code = c.get('code', '')
            name = c.get('name', '')
            score = c.get('score', 0)
            factors = c.get('factors', {})
            indicators = c.get('indicators', {})

            # 提取关键指标
            rsi = indicators.get('rsi', 'N/A')
            macd = indicators.get('macd', 'N/A')
            vol_ratio = indicators.get('volume_ratio', 'N/A')
            breakout = indicators.get('breakout_strength_20d', 'N/A')

            factor_str = ', '.join(f'{k}={v:.2f}' for k, v in list(factors.items())[:3])
            lines.append(
                f"{i}. {name}({code}) 多因子评分={score:.1f} | "
                f"RSI={rsi}, MACD={macd}, 量比={vol_ratio}, 突破={breakout} | "
                f"因子: {factor_str}"
            )

        lines.extend([
            "",
            "请以JSON格式返回排序结果（无需markdown代码块）：",
            '{"ranked": [{"code": "xxx", "reason": "一句话理由", "risk": "风险提示"}, ...]}',
            "按推荐程度从高到低排列。",
        ])

        return '\n'.join(lines)

    def _call_llm(self, prompt: str) -> Optional[str]:
        """调用LLM（复用AISummary的模型基础设施）"""
        system_prompt = "你是一位专业的A股量化分析师，擅长技术面分析和多因子排序。请严格按JSON格式返回结果。"

        if not self._ai_summary.models:
            return None

        # 使用第一个可用模型
        cfg = list(self._ai_summary.models.values())[0]
        response = self._ai_summary._call_single_model(cfg, system_prompt, prompt, max_tokens=800)

        if response.success and response.text:
            return response.text
        return None

    def _parse_rank_response(self, response_text: str, original: List[Dict]) -> Optional[List[Dict]]:
        """解析LLM排序结果"""
        try:
            # 尝试解析JSON
            text = response_text.strip()
            # 去除可能的markdown代码块
            if text.startswith('```'):
                text = text.split('\n', 1)[1] if '\n' in text else text[3:]
            if text.endswith('```'):
                text = text[:-3]
            text = text.strip()

            data = json.loads(text)
            ranked_list = data.get('ranked', [])

            if not ranked_list:
                return None

            # 按LLM排序重建候选列表
            code_to_candidate = {c['code']: c for c in original}
            result = []

            for item in ranked_list:
                code = item.get('code', '')
                candidate = code_to_candidate.get(code)
                if candidate:
                    candidate = dict(candidate)  # 复制
                    candidate['llm_reason'] = item.get('reason', '')
                    candidate['llm_risk'] = item.get('risk', '')
                    result.append(candidate)

            # 补充LLM未提及的候选
            ranked_codes = {item.get('code', '') for item in ranked_list}
            for c in original:
                if c['code'] not in ranked_codes:
                    result.append(c)

            return result if result else None

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.debug(f"Failed to parse LLM rank response: {e}")
            return None
