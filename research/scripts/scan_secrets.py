"""敏感信息扫描器（D59）—— 防密钥误入 git

用途：
1. 提交前 / 部署前扫描工作树与 git 历史，发现真实密钥立即报错
2. 集成到测试套件（test_secret_scan.py），CI 里自动拦截

实现要点（D59 性能优化）：
- 工作树扫描用 `git grep`（git 自己遍历，~0.5s），
  而非 Python 逐文件读（403 文件 × 40ms 沙箱 FS 开销 = 16s）
- 历史扫描用 `git log -p` + 路径排除（跳过 research/raw 等大文件）

识别模式（高置信度，避免误报）：
- sk-<32+字符>       OpenAI / 云知声 / Anthropic 类
- sk_fuyao-<...>     同花顺扶摇
- bs-<24+字符>       baostock
- oapi.dingtalk.com 的 access_token
- AWS Access Key / GitHub token / SSH 私钥块
- 长十六进制（>=32，排除 git SHA、纯数字、已核实的公开 ut 参数）

白名单（占位符/示例/公开参数，不算泄露）：
  your_xxx / 你的 / xxx / <...> / REDACTED / example / placeholder / fake
  东财公开接口固定 ut 参数（网页抓包可见，非凭据）
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

# 高置信度密钥模式
_PATTERNS: list[tuple[str, str]] = [
    ('OpenAI/云知声类密钥', r'sk-[A-Za-z0-9_-]{32,}'),
    ('同花顺扶摇密钥', r'sk_fuyao-[A-Za-z0-9_-]{16,}'),
    ('baostock 密钥', r'bs-[A-Za-z0-9]{24,}'),
    ('钉钉 webhook token',
     r'oapi\.dingtalk\.com/robot/send\?access_token=[A-Za-z0-9]{16,}'),
    ('SSH 私钥', r'-----BEGIN [A-Z ]*PRIVATE KEY-----'),
    ('AWS Access Key', r'AKIA[0-9A-Z]{16}'),
    ('GitHub token', r'gh[pousr]_[A-Za-z0-9]{36,}'),
]
_COMBINED = '|'.join(f'({p})' for _, p in _PATTERNS)
_COMPILED = [(name, re.compile(p)) for name, p in _PATTERNS]

# 明确豁免的路径（第三方 bundle / 抓取的原始内容 / 调研归档）
EXEMPT_PATHS = (
    ':(exclude)research/raw',
    ':(exclude)research/docs',
    ':(exclude)quant_engine/data/sources/assets',
)

# 扫描器自身的测试文件（必须包含密钥样例来验证检测能力，
# 样例一律用 sk-FAKE... 假密钥，但模式与真密钥完全一致 → 豁免该文件）
EXEMPT_FILES = (
    'test/test_secret_scan.py',
)

# 占位符整串匹配（不做子串匹配，避免误伤含 sample/mock 的真密钥）
_PLACEHOLDER_FULL = re.compile(
    r'your[_-]?[\w-]*|你的[\w一-鿿]*|xxx+|example|placeholder|redacted|'
    r'dummy|fake|mock|sample|测试[\w一-鿿]*|<[^>]+>|\*+|\.\.\.',
    re.IGNORECASE)

# 明确的测试用假密钥（整串完全匹配）
EXACT_TEST_KEYS = {'sk-test', 'sk-u', 'sk-d', 'sk-m', 'sk-test-key'}

# 已核实的公开参数白名单（网页抓包可见，非凭据）
KNOWN_PUBLIC_UT = {
    '7eea3edcaed734bea9cbfc24409ed989',   # 东财 push2/push2ex 公共 ut
    'bd1d9ddb04089700cf9c27f6f7426281',   # 东财旧版 ut（历史 modules/ 用）
}

# 长十六进制（排除 git SHA / 纯数字 / 公开 ut）
_HEX_PAT = re.compile(r'\b[0-9a-f]{32,64}\b')
_SHA_LIKE = re.compile(r'^[0-9a-f]{40}$')


def _is_placeholder(text: str) -> bool:
    """整串是否就是占位符"""
    s = text.strip()
    for q in ('"', "'"):
        s = s.strip(q)
    if s.lower() in ('xxx', '...', '***', '-', ''):
        return True
    if s.startswith('<') and s.endswith('>'):
        return True
    if s and set(s) <= {'*'}:
        return True
    if s in EXACT_TEST_KEYS:
        return True
    return bool(_PLACEHOLDER_FULL.fullmatch(s))


def scan_text(text: str) -> list[tuple[str, str]]:
    """扫描一段文本 → [(类型, 脱敏后的匹配串)]"""
    hits: list[tuple[str, str]] = []
    for name, pat in _COMPILED:
        for m in pat.finditer(text):
            raw = m.group(0)
            if raw in EXACT_TEST_KEYS or _is_placeholder(raw):
                continue
            hits.append((name, raw[:8] + '…' + raw[-4:]))
    for m in _HEX_PAT.finditer(text):
        s = m.group(0)
        if _SHA_LIKE.match(s) or s.isdigit() or s in KNOWN_PUBLIC_UT:
            continue
        if _is_placeholder(s):
            continue
        hits.append(('疑似十六进制密钥', s[:8] + '…' + s[-4:]))
    return hits


def scan_worktree(root: Path) -> list[tuple[str, str, int]]:
    """扫描工作树（已跟踪 + 未跟踪文件）→ [(文件, 类型, 行号)]

    两段扫描：
    ① 已跟踪文件用 `git grep`（git 自己遍历，比 Python 逐文件读快 30 倍：
       沙箱文件系统每文件约 40ms 开销，403 文件 = 16s）
    ② 未跟踪文件用 Python 扫（git grep 不覆盖未跟踪文件 —— D59 实测：
       我写测试时误把真实密钥放进新文件，git grep 完全看不见！
       未跟踪文件正是最容易误提交的场景，必须覆盖）
    """
    findings: list[tuple[str, str, int]] = []

    # ① 已跟踪文件
    try:
        proc = subprocess.run(
            ['git', 'grep', '-nIE', _COMBINED, '--', '.', *EXEMPT_PATHS,
             *(f':(exclude){f}' for f in EXEMPT_FILES)],
            cwd=root, capture_output=True, check=False)
    except OSError:
        proc = None
    if proc is not None:
        out = proc.stdout.decode('utf-8', errors='replace')
        for line in out.splitlines():
            parts = line.split(':', 2)   # file:lineno:content
            if len(parts) < 3:
                continue
            rel, lineno, content = parts[0], parts[1], parts[2]
            for name, matched in scan_text(content):
                findings.append((rel, name, int(lineno)))

    # ② 未跟踪文件（git grep 覆盖不到）
    for rel in _untracked_files(root):
        p = Path(root) / rel
        try:
            if p.stat().st_size > 2_000_000:
                continue
            with p.open('r', encoding='utf-8', errors='ignore') as f:
                for lineno, line in enumerate(f, 1):
                    for name, matched in scan_text(line[:4000]):
                        findings.append((rel, name, lineno))
        except OSError:
            continue
    return findings


def _untracked_files(root: Path) -> list[str]:
    """未跟踪且未忽略的文件列表（排除豁免路径）"""
    try:
        proc = subprocess.run(
            ['git', 'ls-files', '--others', '--exclude-standard'],
            cwd=root, capture_output=True, check=False)
    except OSError:
        return []
    out = proc.stdout.decode('utf-8', errors='replace')
    files = []
    for rel in out.splitlines():
        if not rel:
            continue
        if any(excl.removeprefix(':(exclude)') in rel
               for excl in EXEMPT_PATHS if excl.startswith(':(exclude)')):
            continue
        if rel in EXEMPT_FILES:
            continue
        files.append(rel)
    return files


def scan_history(root: Path, max_commits: int = 400) -> list[tuple[str, str]]:
    """扫描 git 历史的新增行 → [(commit, 描述)]"""
    try:
        proc = subprocess.run(
            ['git', 'log', '--all', '-p', '--no-color', f'-{max_commits}',
             '--', '.', *EXEMPT_PATHS,
             *(f':(exclude){f}' for f in EXEMPT_FILES)],
            cwd=root, capture_output=True, check=False)
        out = proc.stdout.decode('utf-8', errors='replace')
    except OSError:
        return []
    findings: list[tuple[str, str]] = []
    commit = ''
    current_file = ''
    for line in out.splitlines():
        if line.startswith('commit '):
            commit = line[7:17]
            continue
        if line.startswith('+++ b/'):
            current_file = line[6:]
            continue
        if not line.startswith('+'):
            continue
        for name, matched in scan_text(line[1:]):
            findings.append((commit, f'{name} ({matched}) [{current_file}]'))
    return findings


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    print('=' * 70)
    print('敏感信息扫描（D59）')
    print('=' * 70)

    wt = scan_worktree(root)
    print(f'\n[1/2] 工作树扫描：{len(wt)} 处可疑')
    for rel, name, lineno in wt[:30]:
        print(f'    ❌ {rel}:{lineno}  {name}')

    hist = scan_history(root)
    print(f'\n[2/2] git 历史扫描：{len(hist)} 处可疑')
    for commit, desc in hist[:30]:
        print(f'    ❌ {commit}  {desc}')

    if not wt and not hist:
        print('\n✅ 未发现敏感信息 —— 可安全推送')
        return 0
    print('\n⚠️  发现敏感信息！处理建议：')
    print('   1. 立即在服务商控制台吊销该密钥并重新生成')
    print('   2. 若已推送到 GitHub：用 git filter-repo / BFG 清理历史，')
    print('      并 force-push（协作仓库需先通知他人）')
    print('   3. 改用环境变量注入，本地 .env 保存（已在 .gitignore）')
    return 1


if __name__ == '__main__':
    sys.exit(main())
