"""AI 总结模型鉴权降级测试（D53，2026-11-01）

覆盖：生产日志 "MiMo v2.5调用失败: HTTP 401 Invalid API Key" —— 密钥失效
但每次摘要仍发一次请求白等。D53 修复：401/403 后进程内禁用该模型，后续
摘要不再请求它（仅用剩余模型生成）。
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.services.ai import ai_summary as ai_mod
from quant_engine.services.ai.ai_summary import AISummary, ModelConfig


class _FakeResponse:
    def __init__(self, status_code, text='{"error": {"message": "Invalid API Key"}}'):
        self.status_code = status_code
        self.text = text

    def json(self):
        return {"choices": [{"message": {"content": "hello"}}], "usage": {"total_tokens": 12}}


class _FakeClient:
    """替换 httpx.Client"""

    def __init__(self, status_code):
        self._status = status_code

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def post(self, url, json=None, headers=None):
        return _FakeResponse(self._status)


def _make_summary(status_code=401):
    """构造一个只含单个模型的 AISummary 实例（绕过 settings 依赖）"""
    obj = object.__new__(AISummary)
    cfg = ModelConfig(name="MiMo v2.5", model_id="mimo-v2.5-pro", api_key="tp-invalid",
                      base_url="https://example.invalid/v1", auth_header="api-key")
    obj.models = {"mimo": cfg}
    obj.primary_model = "mimo"
    obj._cache = {}
    obj._cache_ttl = 300
    return obj, cfg


class TestAuthDisable(unittest.TestCase):
    def setUp(self):
        self._orig = ai_mod.httpx.Client

    def tearDown(self):
        ai_mod.httpx.Client = self._orig

    def test_401_disables_model(self):
        ai_mod.httpx.Client = lambda *a, **k: _FakeClient(401)
        summary, cfg = _make_summary(401)
        resp = summary._call_single_model(cfg, "sys", "user")
        self.assertFalse(resp.success)
        self.assertIn('鉴权失败', resp.error)
        self.assertFalse(cfg.enabled, '401 后应禁用该模型')

    def test_403_disables_model(self):
        ai_mod.httpx.Client = lambda *a, **k: _FakeClient(403)
        summary, cfg = _make_summary(403)
        summary._call_single_model(cfg, "sys", "user")
        self.assertFalse(cfg.enabled)

    def test_429_does_not_disable(self):
        """限流是可恢复的，不应禁用模型"""
        ai_mod.httpx.Client = lambda *a, **k: _FakeClient(429)
        summary, cfg = _make_summary(429)
        resp = summary._call_single_model(cfg, "sys", "user")
        self.assertTrue(cfg.enabled)
        self.assertIn('限流', resp.error)

    def test_500_does_not_disable(self):
        ai_mod.httpx.Client = lambda *a, **k: _FakeClient(500)
        summary, cfg = _make_summary(500)
        summary._call_single_model(cfg, "sys", "user")
        self.assertTrue(cfg.enabled)

    def test_enabled_models_filters_disabled(self):
        summary, cfg = _make_summary(401)
        ai_mod.httpx.Client = lambda *a, **k: _FakeClient(401)
        summary._call_single_model(cfg, "sys", "user")
        self.assertEqual(summary._enabled_models(), {})

    def test_disable_is_idempotent(self):
        summary, cfg = _make_summary(401)
        summary._disable_model(cfg)
        summary._disable_model(cfg)  # 第二次不应重复处理
        self.assertFalse(cfg.enabled)


if __name__ == '__main__':
    unittest.main()
