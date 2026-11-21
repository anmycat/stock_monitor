"""API_AUTH_TOKEN 前后端契约静态护栏（P0-1，2026-11-21）

后端 verify_api_key / verify_websocket_token 已在 api/auth.py 实现，
前端 fetchAPI 带 X-API-Key、connectWS 带 ?token= 也已实现。
本测试防止后续改动意外绕过鉴权头，导致「后端开了 token、前端全 401」
或「前端不带 key」的断裂。

不启用真 token（那是部署动作，见 docs/38），只验证契约完整性。
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quant_engine.api import auth

ROOT = Path(__file__).resolve().parent.parent
HTML = ROOT / 'UX' / 'static' / 'index.html'


class TestAuthContract(unittest.TestCase):
    def test_backend_exempt_only_health(self):
        self.assertEqual(set(auth.AUTH_EXEMPT_PATHS), {'/api/health'})

    def test_backend_has_both_verifiers(self):
        self.assertTrue(callable(auth.verify_api_key))
        self.assertTrue(callable(auth.verify_websocket_token))

    def test_frontend_sends_api_key_header(self):
        html = HTML.read_text(encoding='utf-8')
        self.assertIn("'X-API-Key': API_KEY", html)
        self.assertIn('headers: apiHeaders()', html)
        self.assertIn("localStorage.getItem('sm_api_key')", html)

    def test_frontend_ws_sends_token(self):
        html = HTML.read_text(encoding='utf-8')
        self.assertIn("'?token='+encodeURIComponent(API_KEY)", html)

    def test_no_unguarded_fetch_in_frontend(self):
        """除 fetchAPI 内部那一行，不得有裸 fetch( 绕过鉴权头。"""
        html = HTML.read_text(encoding='utf-8')
        bare = [ln.strip() for ln in html.splitlines()
                if 'fetch(' in ln and 'fetchAPI' not in ln
                and 'function fetch' not in ln]
        self.assertEqual(len(bare), 1, f'发现绕过鉴权头的裸 fetch: {bare}')
        self.assertIn('apiHeaders()', bare[0])


class TestAuthEnabledEndToEnd(unittest.TestCase):
    """启用态端到端：API_AUTH_TOKEN 配置后 401/200 全链路（E3 白名单挂空的替代保护）"""

    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient

        from quant_engine.config import get_settings
        from quant_engine.main import app
        get_settings.cache_clear()
        os.environ['API_AUTH_TOKEN'] = 'e2e-test-token-123'
        get_settings.cache_clear()
        cls.client = TestClient(app)
        cls.app = app

    @classmethod
    def tearDownClass(cls):
        os.environ.pop('API_AUTH_TOKEN', None)
        from quant_engine.config import get_settings
        get_settings.cache_clear()

    def test_health_exempt_anonymous(self):
        r = self.client.get('/api/health')
        self.assertNotEqual(r.status_code, 401)

    def test_protected_endpoint_401_without_key(self):
        r = self.client.get('/api/market')
        self.assertEqual(r.status_code, 401)

    def test_protected_endpoint_401_wrong_key(self):
        r = self.client.get('/api/market', headers={'X-API-Key': 'wrong'})
        self.assertEqual(r.status_code, 401)

    def test_protected_endpoint_200_with_key(self):
        r = self.client.get('/api/market', headers={'X-API-Key': 'e2e-test-token-123'})
        self.assertNotEqual(r.status_code, 401)

    def test_ws_token_verification(self):
        self.assertTrue(auth.verify_websocket_token('e2e-test-token-123'))
        self.assertFalse(auth.verify_websocket_token('wrong'))
        self.assertFalse(auth.verify_websocket_token(None))


if __name__ == '__main__':
    unittest.main(verbosity=2)
