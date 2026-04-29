import os
import json
import time
import hashlib
import hmac
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from dotenv import load_dotenv

load_dotenv("config/.env")


def _post_json(url, payload, timeout=8, max_retries=2):
    if not url:
        return False, "missing url"
    last_error = "unknown"
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            return True, (response.text or "")[:500]
        except Exception as exc:
            last_error = str(exc)
            if attempt < max_retries:
                import time
                time.sleep(0.5 * (attempt + 1))
    return False, last_error


def _state_file():
    return os.getenv("NOTIFY_STATE_FILE", "logs/notify_state.json")


def _ensure_state_parent(path):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _read_state(path):
    if not os.path.exists(path):
        return {"events": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "events" not in data or not isinstance(data["events"], dict):
            return {"events": {}}
        return data
    except Exception:
        return {"events": {}}


def _write_state(path, state):
    _ensure_state_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=True, indent=2)


def _fingerprint(message):
    return hashlib.sha256(message.encode("utf-8")).hexdigest()


def should_notify(alert_key, message):
    now = time.time()
    cooldown_seconds = int(os.getenv("NOTIFY_COOLDOWN_SECONDS", "300"))
    dedupe_seconds = int(os.getenv("NOTIFY_DEDUPE_SECONDS", "1800"))

    path = _state_file()
    state = _read_state(path)
    events = state["events"]
    record = events.get(alert_key, {})
    last_ts = float(record.get("last_sent_ts", 0))
    last_fp = record.get("last_fingerprint", "")
    current_fp = _fingerprint(message)
    elapsed = now - last_ts

    if last_ts > 0 and elapsed < cooldown_seconds:
        return False, f"cooldown<{cooldown_seconds}s"
    if last_fp == current_fp and last_ts > 0 and elapsed < dedupe_seconds:
        return False, f"duplicate<{dedupe_seconds}s"
    return True, "ok"


def notify_with_guard(alert_key, message):
    allowed, reason = should_notify(alert_key, message)
    if not allowed:
        return {"sent": False, "reason": reason, "results": None}
    results = broadcast(message)
    success = any(item[0] for item in results.values() if isinstance(item, tuple) and item)
    if not success:
        return {"sent": False, "reason": "all_channels_failed", "results": results}
    path = _state_file()
    state = _read_state(path)
    state["events"][alert_key] = {
        "last_sent_ts": time.time(),
        "last_fingerprint": _fingerprint(message),
    }
    _write_state(path, state)
    return {"sent": True, "reason": "sent", "results": results}


def _build_dingding_signed_webhook(webhook, secret):
    if not secret or not webhook:
        return webhook
    timestamp = str(int(time.time() * 1000))
    string_to_sign = f"{timestamp}\n{secret}"
    sign = base64.b64encode(
        hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    ).decode("utf-8")
    parsed = urlparse(webhook)
    query = parse_qs(parsed.query)
    query["timestamp"] = [timestamp]
    query["sign"] = [sign]
    # parse_qs returns list values; flatten into first item for urlencode.
    flat = {k: v[0] for k, v in query.items()}
    return urlunparse(parsed._replace(query=urlencode(flat)))


def send_dingding(message):
    webhook = os.getenv("DINGDING_WEBHOOK", "").strip()
    secret = os.getenv("DINGDING_SECRET", "").strip()
    webhook = _build_dingding_signed_webhook(webhook, secret)
    payload = {"msgtype": "text", "text": {"content": message}}
    ok, resp = _post_json(webhook, payload)
    if not ok:
        return False, resp
    try:
        data = json.loads(resp)
    except Exception:
        # Keep backward-compatible behavior for non-JSON proxies.
        return True, resp[:120]
    if isinstance(data, dict) and data.get("errcode", 0) not in (0, "0", None):
        errcode = data.get("errcode")
        errmsg = str(data.get("errmsg", "unknown"))
        return False, f"dingtalk_errcode={errcode} errmsg={errmsg}"[:220]
    return True, str(data)[:220]


def send_pushdeer(message):
    pushkey = os.getenv("PUSHDEER_PUSHKEY", "").strip() or os.getenv("PUSHDEER_KEY", "").strip()
    if not pushkey:
        return False, "missing pushkey"
    server = os.getenv("PUSHDEER_SERVER", "https://api2.pushdeer.com").rstrip("/")
    url = f"{server}/message/push"
    try:
        response = requests.get(
            url,
            params={"pushkey": pushkey, "text": message, "type": "text"},
            timeout=8,
        )
        response.raise_for_status()
        return True, response.text[:120]
    except Exception as exc:
        return False, str(exc)


def broadcast(message):
    tasks = {
        "dingding": send_dingding,
        "pushdeer": send_pushdeer,
    }
    results = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        future_map = {pool.submit(func, message): name for name, func in tasks.items()}
        for future in as_completed(future_map):
            name = future_map[future]
            try:
                results[name] = future.result()
            except Exception as exc:
                results[name] = (False, str(exc))
    return results
