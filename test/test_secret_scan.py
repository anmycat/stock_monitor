"""敏感信息扫描器测试（D59）

验证 research/scripts/scan_secrets.py 能识别真实密钥、
且不误报占位符/公开参数/第三方文件。
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'research', 'scripts'))

from scan_secrets import (
    EXEMPT_FILES,
    EXEMPT_PATHS,
    KNOWN_PUBLIC_UT,
    scan_text,
    scan_worktree,
)


class TestSecretDetection(unittest.TestCase):
    """真实密钥必须被抓到"""

    def test_detects_unisound_key(self):
        """云知声 sk- 密钥"""
        text = 'UNISOUND_API_KEY=sk-FAKE0000000000000000000000000000000000'
        hits = scan_text(text)
        self.assertTrue(hits, '云知声密钥应被检出')
        self.assertIn('OpenAI/云知声类密钥', hits[0][0])

    def test_detects_generic_sk_key(self):
        text = 'key = sk-abcdefghijklmnopqrstuvwxyz0123456789ABCD'
        self.assertTrue(scan_text(text))

    def test_detects_fuyao_key(self):
        text = 'HEXIN_V=sk_fuyao-abcdefghijklmnop1234567890'
        self.assertTrue(scan_text(text))

    def test_detects_baostock_key(self):
        text = 'api=bs-FAKE22222222222222222222222222'
        self.assertTrue(scan_text(text))

    def test_detects_dingtalk_webhook(self):
        text = ('https://oapi.dingtalk.com/robot/send?'
                'access_token=abcdef0123456789abcdef0123456789')
        self.assertTrue(scan_text(text))

    def test_detects_github_token(self):
        text = 'token=ghp_abcdefghijklmnopqrstuvwxyz0123456789'
        self.assertTrue(scan_text(text))

    def test_detects_aws_key(self):
        text = 'AKIAIOSFODNN7EXAMPLE'
        self.assertTrue(scan_text(text))

    def test_detects_ssh_private_key(self):
        text = '-----BEGIN RSA PRIVATE KEY-----'
        self.assertTrue(scan_text(text))


class TestNoFalsePositives(unittest.TestCase):
    """占位符 / 公开参数 / 第三方文件不得误报"""

    def test_placeholder_your_key(self):
        self.assertFalse(scan_text('UNISOUND_API_KEY=your_unisound_api_key_here'))

    def test_placeholder_chinese(self):
        self.assertFalse(scan_text(
            'DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?'
            'access_token=你的token'))

    def test_placeholder_angles(self):
        self.assertFalse(scan_text("REMOTE_PASSWORD='<服务器密码>'"))

    def test_placeholder_redacted(self):
        self.assertFalse(scan_text('DINGTALK_SECRET=REDACTED'))

    def test_public_ut_parameter(self):
        """东财公开接口的固定 ut 参数不是凭据"""
        for ut in KNOWN_PUBLIC_UT:
            self.assertFalse(scan_text(f'ut={ut}'), f'公开参数 {ut} 不应误报')

    def test_git_sha_not_flagged(self):
        sha = '5df5e48770dcbc1eb9dd4a1d3f5e2b8c9d0e1f2a'
        self.assertFalse(scan_text(f'commit {sha}'))

    def test_env_example_is_clean(self):
        """本项目的 .env.example 必须零告警"""
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        example = os.path.join(root, 'quant_engine', '.env.example')
        if not os.path.exists(example):
            self.skipTest('.env.example 不存在')
        with open(example, encoding='utf-8') as f:
            hits = scan_text(f.read())
        self.assertFalse(hits, f'.env.example 有疑似敏感信息: {hits}')

    def test_exempt_paths_configured(self):
        """豁免路径必须覆盖第三方 bundle 与抓取内容"""
        joined = ' '.join(EXEMPT_PATHS)
        self.assertIn('research/raw', joined)
        self.assertIn('research/docs', joined)
        self.assertIn('quant_engine/data/sources/assets', joined)
        # 扫描器自测文件（含密钥样例）也必须豁免
        self.assertIn('test/test_secret_scan.py', EXEMPT_FILES)


class TestWorktreeScan(unittest.TestCase):
    """工作树扫描集成"""

    def test_worktree_has_no_secrets(self):
        """当前仓库工作树必须零告警（核心回归）

        只扫工作树（快，~1s）；历史扫描由 research/scripts/scan_secrets.py
        在推送前手动/部署前执行（全历史扫描约 18s，不适合放进单测）。
        """
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        findings = scan_worktree(root)
        self.assertFalse(
            findings,
            '工作树发现敏感信息:\n' + '\n'.join(
                f'  {f}:{ln} {n}' for f, n, ln in findings[:20]))


if __name__ == '__main__':
    unittest.main()
