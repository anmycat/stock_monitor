"""巨潮资讯（cninfo）公告事件源（D53 遗留修复 #3）

定位：**不是行情源，是公告/事件源**——补上项目当前缺失的维度
（ST/退市/风险警示/重大事项公告）。

接口（2026-11-03 实测）：
  公告查询 POST http://www.cninfo.com.cn/new/hisAnnouncement/query
    必填 stock="代码,orgId"（只传代码返回 0 条 —— 这是最常见的踩坑）
    可选：pageNum/pageSize/column=szse|szse（沪市用 'sse'）/tabName=fulltext
           plate/  seDate（YYYY-MM-DD~YYYY-MM-DD）/searchkey/secid/category
  代码表   POST http://www.cninfo.com.cn/new/data/szse_stock.json（深市）
           POST http://www.cninfo.com.cn/new/data/sse_stock.json（沪市）
  PDF      GET  http://static.cninfo.com.cn/{adjunctUrl}

反爬：限速 ≥1.5s（调研实测），返回体需带浏览器 UA/Referer。
本模块限速 1.6s + 随机抖动，且复用同一 requests.Session（连接复用）。
"""
from __future__ import annotations

import random
import threading
import time
from typing import ClassVar

import pandas as pd
import requests
from loguru import logger

_SESSION = requests.Session()

_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
    'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
}


class _Throttle:
    """巨潮限速：≥1.6s + 随机抖动（反爬较严）"""

    MIN_INTERVAL = 1.6
    JITTER_RANGE = (0.2, 0.8)

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

#: orgId 缓存（代码 → orgId），进程内复用避免重复拉代码表
_ORG_ID_CACHE: dict[str, str] = {}


class CninfoSource:
    """巨潮资讯公告源（可选依赖，失败即降级）"""

    QUERY_URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    STOCK_TABLE_URLS: ClassVar[dict[str, str]] = {
        'szse': 'http://www.cninfo.com.cn/new/data/szse_stock.json',
        'sse': 'http://www.cninfo.com.cn/new/data/sse_stock.json',
    }
    PDF_BASE = 'http://static.cninfo.com.cn/'

    # ---------------- 代码表（orgId）----------------

    @classmethod
    def _load_org_ids(cls) -> dict[str, str]:
        """拉取沪深代码表 → {代码: orgId}（带缓存）"""
        if _ORG_ID_CACHE:
            return _ORG_ID_CACHE
        for _market, url in cls.STOCK_TABLE_URLS.items():
            try:
                _THROTTLE.wait()
                resp = _SESSION.get(url, headers=_HEADERS, timeout=20)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                for item in (data.get('stockList') or []):
                    code = str(item.get('code') or '')
                    org = str(item.get('orgId') or '')
                    if code and org:
                        _ORG_ID_CACHE[code] = org
            except Exception as e:  # 外部接口失败降级
                logger.debug(f'cninfo 代码表拉取失败[{_market}]: {type(e).__name__}: {e}')
        return _ORG_ID_CACHE

    @classmethod
    def get_org_id(cls, code: str) -> str:
        """取某只票的 orgId（取不到返回空）"""
        sym = str(code or '').split('.')[0]
        if not sym:
            return ''
        orgs = cls._load_org_ids()
        return orgs.get(sym, '')

    # ---------------- 公告查询 ----------------

    @classmethod
    def get_announcements(cls, code: str, days: int = 30,
                          page_size: int = 30) -> pd.DataFrame:
        """查询个股公告

        Args:
            code: 标准代码或裸代码（如 '600519' / '600519.SH'）
            days: 回溯天数
            page_size: 每页条数（上限 30/页，需翻页）
        Returns:
            DataFrame[code/name/title/announce_time/pdf_url]
            取不到 orgId 时返回空 DataFrame（不抛异常）
        """
        sym = str(code or '').split('.')[0]
        if not sym or not sym.isdigit():
            return pd.DataFrame()
        org_id = cls.get_org_id(sym)
        if not org_id:
            logger.debug(f'cninfo: {sym} 无 orgId（代码表未覆盖），跳过公告查询')
            return pd.DataFrame()

        end = time.strftime('%Y-%m-%d')
        start = time.strftime('%Y-%m-%d', time.localtime(time.time() - days * 86400))
        payload = {
            'stock': f'{sym},{org_id}',
            'tabName': 'fulltext',
            'pageSize': int(page_size),
            'pageNum': 1,
            'column': 'szse' if not sym.startswith('6') else 'sse',
            'category': '',
            'plate': '',
            'seDate': f'{start}~{end}',
            'searchkey': '',
            'secid': '',
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true',
        }
        try:
            _THROTTLE.wait()
            resp = _SESSION.post(cls.QUERY_URL, data=payload, headers=_HEADERS, timeout=20)
            if resp.status_code != 200:
                logger.debug(f'cninfo 公告查询 HTTP {resp.status_code}')
                return pd.DataFrame()
            data = resp.json()
        except Exception as e:
            logger.debug(f'cninfo 公告查询失败[{sym}]: {type(e).__name__}: {e}')
            return pd.DataFrame()

        items = data.get('announcements') or []
        records = []
        for it in items:
            ts = it.get('announcementTime')
            try:
                announce_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts / 1000))
            except (TypeError, ValueError):
                announce_time = str(ts or '')
            adjunct = it.get('adjunctUrl') or ''
            records.append({
                'code': f"{sym}.{'SH' if sym.startswith('6') else 'SZ'}",
                'name': it.get('secName') or '',
                'title': (it.get('announcementTitle') or '').strip(),
                'announce_time': announce_time,
                'pdf_url': (cls.PDF_BASE + adjunct) if adjunct else '',
                'id': str(it.get('announcementId') or ''),
            })
        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records).sort_values('announce_time', ascending=False).reset_index(drop=True)

    @classmethod
    def get_announcement_titles(cls, code: str, days: int = 30) -> list[str]:
        """只取公告标题列表（便于做关键词扫描，如 ST/退市/风险警示）"""
        df = cls.get_announcements(code, days=days)
        if df.empty:
            return []
        return [str(t) for t in df['title'].tolist()]

    @classmethod
    def scan_keyword(cls, codes: list[str], keyword: str, days: int = 30) -> pd.DataFrame:
        """批量扫描关键词（如 'ST' / '退市' / '风险警示'）

        限速 1.6s/次 → 批量扫描较慢，建议盘后低峰跑。
        """
        hits = []
        for code in (codes or []):
            for title in cls.get_announcement_titles(code, days=days):
                if keyword in title:
                    hits.append({'code': code, 'title': title})
        return pd.DataFrame(hits)
