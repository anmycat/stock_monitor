import os

import requests
from dotenv import load_dotenv

load_dotenv("config/.env")

_GEMINI_MODEL = None


def _get_gemini_model():
    global _GEMINI_MODEL
    if _GEMINI_MODEL is not None:
        return _GEMINI_MODEL
    gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not gemini_key:
        return None
    try:
        import google.generativeai as genai
    except Exception:
        return None
    genai.configure(api_key=gemini_key)
    _GEMINI_MODEL = genai.GenerativeModel("gemini-1.5-flash")
    return _GEMINI_MODEL


def _summarize_deepseek(text):
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        return "AI Error: missing DEEPSEEK_API_KEY"
    base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")
    model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip()
    timeout = int(os.getenv("AI_TIMEOUT_SECONDS", "20"))

    return _chat_completion(base_url, api_key, model, text, timeout, "DEEPSEEK_API_KEY")


def _summarize_aihubmix(text):
    api_key = os.getenv("AIHUBMIX_API_KEY", "").strip()
    if not api_key:
        return "AI Error: missing AIHUBMIX_API_KEY"
    base_url = os.getenv("AIHUBMIX_BASE_URL", "https://api.aihubmix.com/v1").rstrip("/")
    model = os.getenv("AIHUBMIX_MODEL", "gpt-4o-mini").strip()
    timeout = int(os.getenv("AI_TIMEOUT_SECONDS", "20"))
    return _chat_completion(base_url, api_key, model, text, timeout, "AIHUBMIX_API_KEY")


def _chat_completion(base_url, api_key, model, text, timeout, key_name):
    url = f"{base_url}/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a concise market analysis assistant."},
            {"role": "user", "content": text},
        ],
        "temperature": 0.2,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        return f"AI Error: {exc} ({key_name})"


def summarize(text):
    provider = os.getenv("AI_PROVIDER", "gemini").strip().lower()
    if provider == "deepseek":
        return _summarize_deepseek(text)
    if provider == "aihubmix":
        return _summarize_aihubmix(text)

    model = _get_gemini_model()
    if not model:
        return "AI Error: missing GEMINI_API_KEY"
    try:
        res = model.generate_content(text)
        return res.text
    except Exception as exc:
        return f"AI Error: {exc}"
