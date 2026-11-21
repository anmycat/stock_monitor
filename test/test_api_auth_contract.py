"""API_AUTH_TOKEN 前后端契约静态护栏（P0-1，2026-11-21）

后端 verify_api_key / verify_websocket_token 已在 api/auth.py 实现，
前端 fetchAPI 带 X-API-Key、connectWS 带 ?token= 也已实现。
本测试防止后续改动意外绕过鉴权头，导致「后端开了 token、前端全 401」
或「前端不带 key」的断裂。

不启用真 token（那是部署动作，见 docs/38），只验证契约完整性。
"""
from __future__ import annotations

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


if __name__ == '__main__':
    unittest.main(verbosity=2)
