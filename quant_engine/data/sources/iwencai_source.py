"""问财（iwencai）选股源（D53 遗留修复 #3）

定位：**盘后一次性选股**，不进盘中主循环（调研：同 IP 高频即屏蔽/空结果）。

接口（2026-11-03 调研确认）：
  POST https://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data
  旧路径 /unifiedwap/home/v3/stockSelection 已 404

两个必需凭据：
  1. Cookie —— 缺 Cookie 直接 Nginx forbidden
  2. Hexin-V —— 浏览器指纹签名头。官方 SDK（pywencai）靠 Node.js 跑
     hexin-v.js 现场生成；本项目不内置 Node 脚本，改为**从环境变量读**：
       HEXIN_V=<pywencai 生成的 hexin-v 值>
       IWENCAI_COOKIE=<浏览器 devtools 复制的 Cookie>
     任一缺失 → 明确降级（返回空 + 一次性告警说明如何配置），不抛异常。

限速：≥2s + 随机抖动（调研：间隔 <2s 会被屏蔽）。
"""
from __future__ import annotations

import os
import random
import threading
import time

import pandas as pd
import requests
from loguru import logger

_SESSION = requests.Session()

_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
    'Content-Type': 'application/json',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.iwencai.com/unifiedwap/home',
    'Connection': 'keep-alive',
}

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


class IwencaiSource:
    """问财选股适配层（凭据缺失即降级）"""

    QUERY_URL = 'https://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data'

    @staticmethod
    def _credentials() -> tuple[str, str]:
        """取 (hexin_v, cookie)；缺失返回空串"""
        return (os.environ.get('HEXIN_V', '').strip(),
                os.environ.get('IWENCAI_COOKIE', '').strip())

    @staticmethod
    def available() -> bool:
        """凭据是否齐备"""
        hexin_v, cookie = IwencaiSource._credentials()
        return bool(hexin_v and cookie)

    @staticmethod
    def _warn_missing() -> None:
        """缺凭据告警（只告警一次）"""
        if 'missing' in _WARNED:
            return
        _WARNED.add('missing')
        logger.warning(
            '问财凭据缺失（HEXIN_V / IWENCAI_COOKIE）→ 选股功能降级为不可用。'
            '获取方式：pip install pywencai 后用其 hexin-v.js 生成 Hexin-V，'
            'Cookie 从浏览器 devtools 复制（需已登录 iwencai.com）')

    @staticmethod
    def select(query: str, per_page: int = 50) -> pd.DataFrame:
        """按自然语言选股

        Args:
            query: 如 '今日涨停' / '连续3日涨停' / '均线多头排列'
            per_page: 每页条数
        Returns:
            DataFrame[code/name/...]; 凭据缺失或失败返回空 DataFrame
        """
        hexin_v, cookie = IwencaiSource._credentials()
        if not (hexin_v and cookie):
            IwencaiSource._warn_missing()
            return pd.DataFrame()
        headers = dict(_HEADERS)
        headers['Hexin-V'] = hexin_v
        headers['Cookie'] = cookie
        payload = {
            'question': query,
            'perpage': int(per_page),
            'page': 1,
            'secondary_intent': 'stock',
            'log_info': {'input': query},
            'query_area': '',
            'block_list': [],
            'add_list': [],
            'query_type': 'pc',
        }
        try:
            _THROTTLE.wait()
            resp = _SESSION.post(IwencaiSource.QUERY_URL, json=payload,
                                 headers=headers, timeout=25)
            if resp.status_code != 200:
                logger.warning(f'问财 HTTP {resp.status_code}（可能被限流，稍后再试）')
                return pd.DataFrame()
            data = resp.json()
        except Exception as e:
            logger.warning(f'问财查询失败: {type(e).__name__}: {e}')
            return pd.DataFrame()

        datas = ((data.get('data') or {}).get('datas') or [])
        records = []
        for item in datas:
            if not isinstance(item, dict):
                continue
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
            rec.update({k: v for k, v in item.items() if k not in ('code', 'name')})
            records.append(rec)
        if not records:
            return pd.DataFrame()
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
