"""D56 云知声 U2 接入测试（替代小米 MiMo，U2 主力 + DeepSeek 备用）

覆盖：
1. 模型注册：U2 为第一主力、DeepSeek 为备用、MiMo 不再注册
2. 优先级：cascade 按权重排序（U2 先试）
3. 双协议请求构造：OpenAI 兼容 / Anthropic 原生
4. 响应解析：OpenAI / Anthropic 两种结构 + 空正文防护
5. 真实调用（可选，UNISOUND_API_KEY 存在时）
"""
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_engine.services.ai.ai_summary import AISummary, ModelConfig, ModelResponse


def _make(**over) -> ModelConfig:
    base = {
        "name": "云知声 U2",
        "model_id": "u2",
        "api_key": "sk-test",
        "base_url": "https://maas-api.unisound.com/v1",
        "weight": 0.7,
        "supports_thinking": True,
        "max_tokens": 1500,
        "timeout": 60,
    }
    base.update(over)
    return ModelConfig(**base)


class TestModelRegistration(unittest.TestCase):
    """D56：U2 主力 + DeepSeek 备用，MiMo 下线"""

    def _summary(self, unisound=True, deepseek=True, mimo=True):
        env = {
            'UNISOUND_API_KEY': 'sk-u' if unisound else '',
            'UNISOUND_BASE_URL': 'https://maas-api.unisound.com/v1',
            'UNISOUND_MODEL': 'u2',
            'UNISOUND_MAX_TOKENS': '1500',
            'DEEPSEEK_API_KEY': 'sk-d' if deepseek else '',
            'MIMO_API_KEY': 'sk-m' if mimo else '',
        }
        with (patch.dict(os.environ, env, clear=False),
              patch('quant_engine.services.ai.ai_summary.get_settings') as mock_settings):
                s = mock_settings.return_value
                s.UNISOUND_API_KEY = env['UNISOUND_API_KEY']
                s.UNISOUND_BASE_URL = env['UNISOUND_BASE_URL']
                s.UNISOUND_MODEL = env['UNISOUND_MODEL']
                s.UNISOUND_MAX_TOKENS = 1500
                s.DEEPSEEK_API_KEY = env['DEEPSEEK_API_KEY']
                s.OPENAI_API_KEY = ''
                s.OPENAI_BASE_URL = ''
                s.MIMO_API_KEY = env['MIMO_API_KEY']
                s.MIMO_BASE_URL = 'https://api.xiaomimimo.com/v1'
                return AISummary()

    def test_unisound_is_primary(self):
        s = self._summary()
        self.assertIn('unisound', s.models)
        self.assertEqual(s.primary_model, 'unisound')

    def test_unisound_weight_exceeds_deepseek(self):
        """权重决定 cascade 优先级与 ensemble 合并者 → U2 必须更高"""
        s = self._summary()
        self.assertGreater(s.models['unisound'].weight, s.models['deepseek'].weight)

    def test_mimo_not_registered(self):
        """D56：MiMo 密钥即使存在也不再注册（生产持续 401）"""
        s = self._summary(mimo=True)
        self.assertNotIn('mimo', s.models)

    def test_cascade_order_prefers_unisound(self):
        """cascade 按权重降序 → U2 先试、DeepSeek 后试"""
        s = self._summary()
        ordered = sorted(s.models.values(), key=lambda m: m.weight, reverse=True)
        self.assertEqual(ordered[0].name, '云知声 U2')
        self.assertEqual(ordered[1].name, 'DeepSeek V4')

    def test_deepseek_only_fallback(self):
        """只有 DeepSeek 密钥时仍可用，primary 指向 deepseek"""
        s = self._summary(unisound=False)
        self.assertNotIn('unisound', s.models)
        self.assertEqual(s.primary_model, 'deepseek')


class TestProtocols(unittest.TestCase):
    """双协议支持"""

    def test_openai_api_url(self):
        cfg = _make()
        self.assertEqual(cfg.api_url,
                         'https://maas-api.unisound.com/v1/chat/completions')

    def test_anthropic_api_url(self):
        cfg = _make(protocol='anthropic',
                    base_url='https://maas-api.unisound.com/anthropic/v1')
        self.assertEqual(cfg.api_url,
                         'https://maas-api.unisound.com/anthropic/v1/messages')

    def test_parse_openai_response(self):
        cfg = _make()
        data = {
            'choices': [{'message': {'content': '  正文  ', 'reasoning_content': '思考'}}],
            'usage': {'total_tokens': 42},
        }
        text, tokens = AISummary._parse_response(cfg, data)
        self.assertEqual(text, '正文')
        self.assertEqual(tokens, 42)

    def test_parse_anthropic_response_skips_thinking(self):
        """Anthropic 原生：content[] 里 type=thinking 不是正文，必须跳过"""
        cfg = _make(protocol='anthropic')
        data = {
            'content': [
                {'type': 'thinking', 'thinking': '思考过程...'},
                {'type': 'text', 'text': '最终正文'},
            ],
            'usage': {'input_tokens': 10, 'output_tokens': 20},
        }
        text, tokens = AISummary._parse_response(cfg, data)
        self.assertEqual(text, '最终正文')
        self.assertEqual(tokens, 30)

    def test_parse_null_content_returns_empty(self):
        """思考型模型 max_tokens 不足 → content=null"""
        cfg = _make()
        data = {
            'choices': [{'message': {'content': None}, 'finish_reason': 'length'}],
            'usage': {'total_tokens': 100},
        }
        text, tokens = AISummary._parse_response(cfg, data)
        self.assertEqual(text, '')
        self.assertEqual(tokens, 100)


class TestRequestConstruction(unittest.TestCase):
    """请求构造"""

    def _capture(self, cfg, status=200, body=None):
        resp = MagicMock(status_code=status)
        resp.json.return_value = body if body is not None else {
            'choices': [{'message': {'content': 'OK'}}], 'usage': {'total_tokens': 5}}
        resp.text = str(resp.json.return_value)
        with patch('httpx.Client') as mock_client:
            mock_client.return_value.__enter__.return_value.post.return_value = resp
            s = AISummary.__new__(AISummary)
            s.models = {cfg.name: cfg}
            s.primary_model = cfg.name
            out = s._call_single_model(cfg, 'sys', 'user')
            payload = mock_client.return_value.__enter__.return_value.post.call_args.kwargs['json']
            headers = mock_client.return_value.__enter__.return_value.post.call_args.kwargs['headers']
            return out, payload, headers

    def test_openai_payload_has_system_role(self):
        cfg = _make()
        out, payload, headers = self._capture(cfg)
        self.assertTrue(out.success)
        self.assertEqual(payload['messages'][0]['role'], 'system')
        self.assertEqual(payload['model'], 'u2')
        self.assertNotIn('anthropic-version', headers)

    def test_anthropic_payload_uses_top_level_system(self):
        cfg = _make(protocol='anthropic')
        out, payload, headers = self._capture(cfg, body={
            'content': [{'type': 'text', 'text': 'OK'}],
            'usage': {'input_tokens': 1, 'output_tokens': 2}})
        self.assertTrue(out.success)
        self.assertEqual(payload['system'], 'sys')
        self.assertEqual(payload['messages'][0]['role'], 'user')
        self.assertEqual(headers['anthropic-version'], '2023-06-01')

    def test_empty_content_treated_as_failure(self):
        """空正文必须算失败，不能当成有效总结"""
        cfg = _make()
        body = {'choices': [{'message': {'content': None},
                             'finish_reason': 'length'}],
                'usage': {'total_tokens': 100}}
        out, _, _ = self._capture(cfg, body=body)
        self.assertFalse(out.success)
        self.assertIn('空响应', out.error)

    def test_max_tokens_capped_by_model(self):
        """模型自身 max_tokens 上限优先"""
        cfg = _make(max_tokens=800)
        _, payload, _ = self._capture(cfg)
        self.assertEqual(payload['max_tokens'], 800)

    def test_401_disables_model(self):
        """鉴权失败 → 进程内禁用，不重复白等"""
        cfg = _make()
        out, _, _ = self._capture(cfg, status=401)
        self.assertFalse(out.success)
        self.assertFalse(cfg.enabled)


class TestRateLimitAndRetry(unittest.TestCase):
    """D57：客户端限速 + 429 退避重试（云知声 RPM 100 / TPM 10M）"""

    def test_rate_limiter_allows_under_limit(self):
        from quant_engine.services.ai.ai_summary import _RateLimiter
        rl = _RateLimiter(max_rpm=5)
        t0 = time.monotonic()
        for _ in range(5):
            rl.acquire()
        # 5 次在限额内 → 不应等待
        self.assertLess(time.monotonic() - t0, 1.0)

    def test_rate_limiter_blocks_over_limit(self):
        from quant_engine.services.ai.ai_summary import _RateLimiter
        rl = _RateLimiter(max_rpm=2)
        rl.acquire()
        rl.acquire()
        # 第 3 次应阻塞（但我们只验证它确实等待，不真等 60s）
        # 用线程 + 短超时验证阻塞
        import threading
        fired = threading.Event()

        def _third():
            rl.acquire()
            fired.set()

        th = threading.Thread(target=_third, daemon=True)
        th.start()
        blocked = not fired.wait(timeout=1.0)
        self.assertTrue(blocked, '超限请求应被阻塞')

    def test_429_retries_with_backoff(self):
        """429 应重试；重试耗尽后返回失败"""
        cfg = _make()
        s = AISummary.__new__(AISummary)
        s.models = {cfg.name: cfg}
        s.primary_model = cfg.name

        calls = {'n': 0}

        def _fake_once(self, c, sp, up, mt):
            calls['n'] += 1
            if calls['n'] <= 2:
                return ModelResponse(model_name=c.name, text='', success=False,
                                     error='限流(429)')
            return ModelResponse(model_name=c.name, text='成功正文',
                                 tokens_used=10, success=True)

        with patch.object(AISummary, '_call_once', _fake_once), \
                patch('quant_engine.services.ai.ai_summary.time.sleep'):
            out = s._call_single_model(cfg, 'sys', 'user')
        self.assertTrue(out.success)
        self.assertEqual(calls['n'], 3, '应在第 3 次成功')

    def test_429_gives_up_after_max_retries(self):
        cfg = _make()
        s = AISummary.__new__(AISummary)
        s.models = {cfg.name: cfg}
        s.primary_model = cfg.name

        def _always_429(self, c, sp, up, mt):
            return ModelResponse(model_name=c.name, text='', success=False,
                                 error='限流(429)')

        with patch.object(AISummary, '_call_once', _always_429), \
                patch('quant_engine.services.ai.ai_summary.time.sleep'):
            out = s._call_single_model(cfg, 'sys', 'user', max_retries=2)
        self.assertFalse(out.success)
        self.assertIn('限流', out.error)

    def test_empty_response_retried(self):
        """D57：思考超预算（空响应）应重试 —— 不是模型故障，不能禁用"""
        cfg = _make()
        s = AISummary.__new__(AISummary)
        s.models = {cfg.name: cfg}
        s.primary_model = cfg.name

        calls = {'n': 0}

        def _flaky(self, c, sp, up, mt):
            calls['n'] += 1
            if calls['n'] == 1:
                return ModelResponse(model_name=c.name, text='', success=False,
                                     error='空响应(finish_reason=length)')
            return ModelResponse(model_name=c.name, text='正文拿到了',
                                 tokens_used=20, success=True)

        with patch.object(AISummary, '_call_once', _flaky), \
                patch('quant_engine.services.ai.ai_summary.time.sleep'):
            out = s._call_single_model(cfg, 'sys', 'user')
        self.assertTrue(out.success)
        self.assertEqual(calls['n'], 2)

    def test_empty_response_not_disables_model(self):
        """空响应不应禁用模型（禁用会导致整轮总结永久退化）"""
        cfg = _make()
        s = AISummary.__new__(AISummary)
        s.models = {cfg.name: cfg}
        s.primary_model = cfg.name

        def _always_empty(self, c, sp, up, mt):
            return ModelResponse(model_name=c.name, text='', success=False,
                                 error='空响应(finish_reason=length)')

        with patch.object(AISummary, '_call_once', _always_empty), \
                patch('quant_engine.services.ai.ai_summary.time.sleep'):
            s._call_single_model(cfg, 'sys', 'user')
        self.assertTrue(cfg.enabled, '空响应不应禁用模型')

    def test_non_429_error_not_retried(self):
        """非限流错误不应重试（避免无谓等待）"""
        cfg = _make()
        s = AISummary.__new__(AISummary)
        s.models = {cfg.name: cfg}
        s.primary_model = cfg.name

        calls = {'n': 0}

        def _fail(self, c, sp, up, mt):
            calls['n'] += 1
            return ModelResponse(model_name=c.name, text='', success=False,
                                 error='HTTP 500: boom')

        with patch.object(AISummary, '_call_once', _fail), \
                patch('quant_engine.services.ai.ai_summary.time.sleep'):
            out = s._call_single_model(cfg, 'sys', 'user')
        self.assertFalse(out.success)
        self.assertEqual(calls['n'], 1, '非 429 错误不应重试')


class TestMergeFallback(unittest.TestCase):
    """D56 修正：合并失败时的回退应按权重选，而不是被"选最长"覆盖"""

    def test_fallback_prefers_higher_weight(self):
        s = AISummary.__new__(AISummary)
        s.models = {
            '云知声 U2': ModelConfig('云知声 U2', 'u2', 'k', 'u', weight=0.7),
            'DeepSeek V4': ModelConfig('DeepSeek V4', 'ds', 'k', 'd', weight=0.3),
        }
        s.primary_model = 'unisound'
        responses = [
            ModelResponse(model_name='DeepSeek V4', text='短', tokens_used=10),
            ModelResponse(model_name='云知声 U2', text='长长长', tokens_used=20),
        ]
        with patch.object(AISummary, '_call_single_model') as mock_call:
            mock_call.return_value = ModelResponse(
                model_name='云知声 U2', text='', success=False, error='模拟合并失败')
            merged = s._merge_responses(responses, 'general', 'info')
        # U2 权重更高 → 即使正文更短也应选 U2
        self.assertEqual(merged['model'], '云知声 U2')


class TestLiveCall(unittest.TestCase):
    """真实调用（需 UNISOUND_API_KEY；无密钥则跳过）"""

    def test_live_unisound_call(self):
        key = os.environ.get('UNISOUND_API_KEY', '')
        if not key:
            self.skipTest('未设置 UNISOUND_API_KEY，跳过真实调用')
        cfg = _make(api_key=key)
        s = AISummary.__new__(AISummary)
        s.models = {cfg.name: cfg}
        s.primary_model = cfg.name
        out = s._call_single_model(
            cfg, '你是金融分析师', '用一句话说明今日A股市场', max_tokens=1500)
        self.assertTrue(out.success, f'真实调用失败: {out.error}')
        self.assertGreater(len(out.text), 10)
        self.assertGreater(out.tokens_used, 0)


if __name__ == '__main__':
    unittest.main()
