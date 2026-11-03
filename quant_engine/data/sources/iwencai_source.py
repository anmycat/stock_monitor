"""问财（iwencai）选股源（D53 遗留修复 #3，D55 重写）

定位：**盘后一次性选股**，不进盘中主循环（调研：同 IP 高频即屏蔽/空结果）。

接口（2026-11-03 生产实测确认）：
  POST https://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data
  旧路径 /unifiedwap/home/v3/stockSelection 已 404

────────────────────────────────────────────────────────────────────────
【D55 实测结论：不需要浏览器 Cookie】
D53 最初的设计是"读环境变量 HEXIN_V + IWENCAI_COOKIE，两者缺一即降级"。
D55 在生产 ECS 上做了四组单变量对照实验，结论与原设计相反：

  ┌──────────────────────┬──────────┬────────────────────────────┐
  │ 组合                 │ HTTP     │ 结果                       │
  ├──────────────────────┼──────────┼────────────────────────────┤
  │ 无 hexin-v + 无 Cookie│ 403      │ Nginx forbidden ❌          │
  │ 有 hexin-v + 无 Cookie│ 200      │ status_code=0 + 真实数据 ✅ │
  │ 无 hexin-v + 有 Cookie│ 200      │ status_code=0 + 真实数据 ✅ │
  │ 有 hexin-v + 有 Cookie│ 200      │ status_code=0 + 真实数据 ✅ │
  └──────────────────────┴──────────┴────────────────────────────┘

**Hexin-V 或 Cookie 任意一个即可过 Nginx 风控**，两者作用等价（二选一）。
而 Hexin-V 可以**全自动生成**（pywencai 的 hexin-v.bundle.js 用 node 跑
一次即得 60 字符 token，耗时 <1s，无需登录态、无需浏览器）→

  **最终方案：只需 node，自动生成 Hexin-V，零人工凭据。**
  Cookie 改为**可选增强**（环境变量 IWENCAI_COOKIE，有则带上，进一步降低
  被风控概率；无则完全不影响可用性）。

【踩坑记录（重要）】
1. **Content-Type 必须是 form-urlencoded，不是 JSON**：
   传 json= 时服务端返回 200 但 `status_code=-302 缺少必要参数`，
   极易被误判为"凭据问题"→ D53 我正是这样误判的。
   正确形式：`requests.post(url, data={...}, headers={'Content-Type':
   'application/x-www-form-urlencoded'})`。
2. **payload 必须含 `iwcpro: '1'`** + `add_info` + `log_info` + `version: '2.0'`
   + `source: 'Ths_iwencai_Xuangu'`，缺字段同样报 -302。
3. **响应结构极深**：`data.answer[0].txt[0].content.components[0].data.datas`，
   字段名随 query 变化（如"最新价"/"最新涨跌幅"/"a股市值(不含限售股)[日期]"）。
4. hexin-v token 每次 node 运行都不同，但**同一天内可复用**（实测连续 3 次
   请求均 200 有数据）→ 无需每请求都起 node 进程，按天缓存即可。

限速：≥2s + 随机抖动（调研：间隔 <2s 会被屏蔽）。
"""
from __future__ import annotations

import os
import random
import subprocess
import threading
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

_SESSION = requests.Session()

_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')

_HEADERS = {
    'User-Agent': _UA,
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.iwencai.com/unifiedwap/home',
    'Connection': 'keep-alive',
}

# 离线资源：pywencai 的 hexin-v.bundle.js（MIT）。放在项目内自带一份，
# 避免生产依赖 pip 安装 pywencai（它会拉 pydash/fake_useragent 等一串包）。
_BUNDLE_CANDIDATES = (
    Path(__file__).with_name('assets') / 'hexin-v.bundle.js',
    Path(__file__).with_name('hexin-v.bundle.js'),
)

# 共享给全项目（含 research 脚本）的离线 JS 副本
_RESEARCH_BUNDLE = (Path(__file__).resolve().parents[3]
                     / 'research' / 'scripts' / 'hexin-v.bundle.js')

_TOKEN_CACHE: dict[str, str] = {}
_TOKEN_LOCK = threading.Lock()
_WARNED: set[str] = set()


class _Throttle:
    """问财限速：≥2s + 随机抖动"""

    MIN_INTERVAL = 2.0
    JITTER_RANGE = (0.3, 1.0)

    def __init__(self):
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last
            base = max(0.0, self.MIN_INTERVAL - elapsed)
            time.sleep(base + random.uniform(*self.JITTER_RANGE))
            self._last = time.monotonic()


_THROTTLE = _Throttle()


def _find_bundle() -> Path | None:
    """定位 hexin-v.bundle.js（项目内自带优先，其次 pywencai 安装位置）"""
    for p in _BUNDLE_CANDIDATES:
        if p.exists():
            return p
    if _RESEARCH_BUNDLE.exists():
        return _RESEARCH_BUNDLE
    try:
        import pywencai
        p = Path(pywencai.__file__).with_name('hexin-v.bundle.js')
        if p.exists():
            return p
    except ImportError:
        return None
    return None


def generate_hexin_v() -> str:
    """用 node 跑 hexin-v.bundle.js 生成签名 token（约 0.5s）

    返回空串表示无法生成（无 node / 无 JS 文件）→ 调用方应降级。
    """
    bundle = _find_bundle()
    if bundle is None:
        return ''
    try:
        result = subprocess.run(
            ['node', str(bundle)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=30, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ''
    token = result.stdout.decode('utf-8', 'replace').strip()
    return token if token else ''


class IwencaiSource:
    """问财选股适配层（凭据缺失即降级；D55 起无需浏览器 Cookie）"""

    QUERY_URL = ('https://www.iwencai.com/unifiedwap/unified-wap/v2/'
                 'result/get-robot-data')

    @staticmethod
    def _cookie() -> str:
        """可选的浏览器 Cookie（增强用，非必需）"""
        return os.environ.get('IWENCAI_COOKIE', '').strip()

    @staticmethod
    def _token(cached: bool = True) -> str:
        """取 Hexin-V：优先环境变量，其次 node 自动生成（按天缓存）"""
        env = os.environ.get('HEXIN_V', '').strip()
        if env:
            return env
        if not cached:
            return generate_hexin_v()
        today = date.today().isoformat()
        with _TOKEN_LOCK:
            tok = _TOKEN_CACHE.get(today)
            if tok:
                return tok
        tok = generate_hexin_v()
        if tok:
            with _TOKEN_LOCK:
                _TOKEN_CACHE[today] = tok
        return tok

    @staticmethod
    def available() -> bool:
        """是否可用：有 node 即可（Cookie 非必需）"""
        if os.environ.get('HEXIN_V', '').strip():
            return True
        return _find_bundle() is not None and generate_hexin_v() != ''

    @staticmethod
    def _warn_missing() -> None:
        """不可用告警（只告警一次）"""
        if 'missing' in _WARNED:
            return
        _WARNED.add('missing')
        logger.warning(
            '问财不可用：未找到 node 或 hexin-v.bundle.js → 选股功能降级。'
            '修复：① 安装 node（apt-get install -y nodejs）；'
            '② 或设置环境变量 HEXIN_V；'
            '③ 浏览器 Cookie（IWENCAI_COOKIE）可选，非必需')

    @staticmethod
    def _payload(query: str, per_page: int) -> dict[str, str]:
        """构造 payload（D55 实测：必须 form 编码 + iwcpro/add_info/log_info/version）"""
        return {
            'question': query,
            'perpage': str(int(per_page)),
            'page': '1',
            'secondary_intent': 'stock',
            'iwcpro': '1',
            'source': 'Ths_iwencai_Xuangu',
            'version': '2.0',
            'add_info': ('{"urp":{"scene":1,"company":1,"business":1},'
                         '"contentType":"json","searchInfo":true}'),
            'log_info': '{"input_type":"click"}',
        }

    @staticmethod
    def _extract_datas(data: dict) -> list[dict]:
        """从深层响应结构中取出 datas 列表

        结构：data.answer[0].txt[0].content.components[0].data.datas
        （实测 2026-11-03；做了多层兜底以容忍结构变动）
        """
        node = (data.get('data') or {}).get('answer')
        if not isinstance(node, list) or not node:
            return []
        for answer in node:
            if not isinstance(answer, dict):
                continue
            txt = answer.get('txt')
            if not isinstance(txt, list):
                continue
            for t in txt:
                if not isinstance(t, dict):
                    continue
                content = t.get('content')
                if not isinstance(content, dict):
                    continue
                comps = content.get('components')
                if not isinstance(comps, list):
                    continue
                for comp in comps:
                    if not isinstance(comp, dict):
                        continue
                    payload = comp.get('data')
                    if isinstance(payload, dict):
                        datas = payload.get('datas')
                        if isinstance(datas, list):
                            return [d for d in datas if isinstance(d, dict)]
        return []

    @staticmethod
    def select(query: str, per_page: int = 50) -> pd.DataFrame:
        """按自然语言选股

        Args:
            query: 如 '今日涨停' / '连续3日涨停' / '均线多头排列'
            per_page: 每页条数
        Returns:
            DataFrame[code/name/...]; 不可用或失败返回空 DataFrame
        """
        token = IwencaiSource._token()
        cookie = IwencaiSource._cookie()
        if not token and not cookie:
            IwencaiSource._warn_missing()
            return pd.DataFrame()
        headers = dict(_HEADERS)
        if token:
            headers['Hexin-V'] = token
        if cookie:
            headers['Cookie'] = cookie
        try:
            _THROTTLE.wait()
            resp = _SESSION.post(IwencaiSource.QUERY_URL,
                                 data=IwencaiSource._payload(query, per_page),
                                 headers=headers, timeout=30)
            if resp.status_code != 200:
                logger.warning(f'问财 HTTP {resp.status_code}（可能被限流，稍后再试）')
                return pd.DataFrame()
            body = resp.json()
        except Exception as e:
            logger.warning(f'问财查询失败: {type(e).__name__}: {e}')
            return pd.DataFrame()

        if body.get('status_code') not in (0, None):
            logger.warning(f'问财业务错误 status_code={body.get("status_code")}: '
                           f'{body.get("status_msg")}')
            return pd.DataFrame()

        datas = IwencaiSource._extract_datas(body)
        if not datas:
            return pd.DataFrame()
        records = []
        for item in datas:
            rec = {'code': '', 'name': ''}
            # 问财字段名随 query 变化，尽量从常见键里取代码/名称
            for key in ('股票代码', 'code', '股票代码_代码'):
                if key in item:
                    rec['code'] = str(item[key])
                    break
            for key in ('股票简称', 'name', '股票简称_简称'):
                if key in item:
                    rec['name'] = str(item[key])
                    break
            rec.update({k: v for k, v in item.items()
                        if k not in ('code', 'name')})
            records.append(rec)
        return pd.DataFrame(records)

    @staticmethod
    def select_codes(query: str, per_page: int = 50) -> list[str]:
        """只取选出的代码列表（归一化为标准代码，如 '600519.SH'）"""
        df = IwencaiSource.select(query, per_page=per_page)
        if df.empty or 'code' not in df.columns:
            return []
        out = []
        for c in df['code'].tolist():
            sym = str(c).split('.')[0]
            if not sym.isdigit() or len(sym) != 6:
                continue
            if sym.startswith('6'):
                out.append(f'{sym}.SH')
            elif sym.startswith(('4', '8', '92')):
                out.append(f'{sym}.BJ')
            else:
                out.append(f'{sym}.SZ')
        return out
