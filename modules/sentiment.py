import os
import json
import time
from typing import List, Dict

import requests

from .ai_engine import _chat_completion, _get_gemini_model

_SENTIMENT_CACHE = {}

_SENTIMENT_SYSTEM_PROMPT = """你是一个财经新闻情感分析助手，返回JSON格式：
{"sentiment": "positive/negative/neutral", "score": -1到1的浮点数, "reason": "简短原因"}"""


def _analyze_with_llm(text: str) -> Dict:
    provider = os.getenv("AI_PROVIDER", "gemini").strip().lower()
    timeout = int(os.getenv("AI_TIMEOUT_SECONDS", "20"))
    
    if provider in ("deepseek", "aihubmix"):
        api_key_env = "DEEPSEEK_API_KEY" if provider == "deepseek" else "AIHUBMIX_API_KEY"
        base_url_env = "DEEPSEEK_BASE_URL" if provider == "deepseek" else "AIHUBMIX_BASE_URL"
        model_env = "DEEPSEEK_MODEL" if provider == "deepseek" else "AIHUBMIX_MODEL"
        default_url = "https://api.deepseek.com" if provider == "deepseek" else "https://api.aihubmix.com/v1"
        default_model = "deepseek-chat" if provider == "deepseek" else "gpt-4o-mini"
        
        api_key = os.getenv(api_key_env, "").strip()
        if not api_key:
            return {"sentiment": "neutral", "score": 0.0, "reason": "missing_api_key"}
        
        base_url = os.getenv(base_url_env, default_url).rstrip("/")
        model = os.getenv(model_env, default_model).strip()
        
        result_text = _chat_completion(base_url, api_key, model, text[:500], timeout, api_key_env)
        if "AI Error:" in result_text:
            return {"sentiment": "neutral", "score": 0.0, "reason": result_text}
        
        if "{" in result_text and "}" in result_text:
            json_str = result_text[result_text.find("{"):result_text.find("}")+1]
            return json.loads(json_str)
        
        return {"sentiment": "neutral", "score": 0.0, "reason": "parse_failed"}
    
    # Gemini
    model = _get_gemini_model()
    if not model:
        return {"sentiment": "neutral", "score": 0.0, "reason": "missing_gemini"}
    
    try:
        prompt = f"{_SENTIMENT_SYSTEM_PROMPT}\n\n新闻内容：{text[:500]}"
        response = model.generate_content(prompt)
        result_text = response.text.strip()
        
        if "{" in result_text and "}" in result_text:
            json_str = result_text[result_text.find("{"):result_text.find("}")+1]
            return json.loads(json_str)
        
        return {"sentiment": "neutral", "score": 0.0, "reason": "parse_failed"}
    except Exception as e:
        return {"sentiment": "neutral", "score": 0.0, "reason": str(e)}


def _analyze_with_deepseek(text: str) -> Dict:
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        return {"sentiment": "neutral", "score": 0.0, "reason": "missing_api_key"}
    
    base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")
    model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip()
    timeout = int(os.getenv("AI_TIMEOUT_SECONDS", "20"))
    
    url = f"{base_url}/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是一个财经新闻情感分析助手，返回JSON格式：{\"sentiment\": \"positive/negative/neutral\", \"score\": -1到1的浮点数, \"reason\": \"简短原因\"}"},
            {"role": "user", "content": text[:500]}
        ],
        "temperature": 0.1,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"].strip()
        
        if "{" in content and "}" in content:
            json_str = content[content.find("{"):content.find("}")+1]
            return json.loads(json_str)
        
        return {"sentiment": "neutral", "score": 0.0, "reason": "parse_failed"}
    except Exception as e:
        return {"sentiment": "neutral", "score": 0.0, "reason": str(e)}


def _analyze_with_aihubmix(text: str) -> Dict:
    api_key = os.getenv("AIHUBMIX_API_KEY", "").strip()
    if not api_key:
        return {"sentiment": "neutral", "score": 0.0, "reason": "missing_api_key"}
    
    base_url = os.getenv("AIHUBMIX_BASE_URL", "https://api.aihubmix.com/v1").rstrip("/")
    model = os.getenv("AIHUBMIX_MODEL", "gpt-4o-mini").strip()
    timeout = int(os.getenv("AI_TIMEOUT_SECONDS", "20"))
    
    url = f"{base_url}/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是一个财经新闻情感分析助手，返回JSON格式：{\"sentiment\": \"positive/negative/neutral\", \"score\": -1到1的浮点数, \"reason\": \"简短原因\"}"},
            {"role": "user", "content": text[:500]}
        ],
        "temperature": 0.1,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"].strip()
        
        if "{" in content and "}" in content:
            json_str = content[content.find("{"):content.find("}")+1]
            return json.loads(json_str)
        
        return {"sentiment": "neutral", "score": 0.0, "reason": "parse_failed"}
    except Exception as e:
        return {"sentiment": "neutral", "score": 0.0, "reason": str(e)}


def analyze_news_sentiment(news_items: List[Dict]) -> Dict:
    if not news_items:
        return {
            "overall_sentiment": "neutral",
            "score": 0.0,
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "items": []
        }
    
    enabled = os.getenv("NEWS_SENTIMENT_ENABLED", "true").lower() == "true"
    if not enabled:
        return {
            "overall_sentiment": "neutral",
            "score": 0.0,
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": len(news_items),
            "items": news_items
        }
    
    results = []
    positive_count = 0
    negative_count = 0
    neutral_count = 0
    total_score = 0.0
    
    cache_ttl = int(os.getenv("SENTIMENT_CACHE_SECONDS", "300"))
    
    for item in news_items[:10]:
        title = item.get("title", "")
        if not title:
            continue
        
        cache_key = f"sentiment_{hash(title)}"
        now = time.time()
        
        if cache_key in _SENTIMENT_CACHE:
            cached = _SENTIMENT_CACHE[cache_key]
            if (now - cached.get("ts", 0)) < cache_ttl:
                result = cached.get("result")
            else:
                result = _analyze_with_llm(title)
                _SENTIMENT_CACHE[cache_key] = {"ts": now, "result": result}
        else:
            result = _analyze_with_llm(title)
            _SENTIMENT_CACHE[cache_key] = {"ts": now, "result": result}
        
        sentiment = result.get("sentiment", "neutral")
        score = result.get("score", 0.0)
        
        if sentiment == "positive":
            positive_count += 1
        elif sentiment == "negative":
            negative_count += 1
        else:
            neutral_count += 1
        
        total_score += score
        
        results.append({
            "title": title[:80],
            "sentiment": sentiment,
            "score": score,
            "reason": result.get("reason", "")
        })
    
    avg_score = total_score / len(results) if results else 0.0
    
    if avg_score > 0.2:
        overall = "positive"
    elif avg_score < -0.2:
        overall = "negative"
    else:
        overall = "neutral"
    
    return {
        "overall_sentiment": overall,
        "score": round(avg_score, 3),
        "positive_count": positive_count,
        "negative_count": negative_count,
        "neutral_count": neutral_count,
        "items": results
    }


def get_market_sentiment_summary(news_items: List[Dict]) -> str:
    analysis = analyze_news_sentiment(news_items)
    
    sentiment = analysis.get("overall_sentiment", "neutral")
    score = analysis.get("score", 0.0)
    pos = analysis.get("positive_count", 0)
    neg = analysis.get("negative_count", 0)
    neu = analysis.get("neutral_count", 0)
    
    emoji_map = {
        "positive": "📈",
        "negative": "📉",
        "neutral": "➡️"
    }
    
    emoji = emoji_map.get(sentiment, "➡️")
    
    summary = f"市场情绪: {emoji} {sentiment.upper()} (得分:{score:+.2f})"
    summary += f" 正面:{pos} 负面:{neg} 中性:{neu}"
    
    if analysis.get("items"):
        hot_items = [item for item in analysis["items"] if item.get("sentiment") != "neutral"]
        if hot_items:
            summary += "\n重点:"
            for item in hot_items[:3]:
                e = "✅" if item["sentiment"] == "positive" else "⚠️"
                summary += f"\n  {e} {item['title'][:40]}"
    
    return summary
