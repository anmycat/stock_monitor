"""同花顺网页端数据源（D53，2026-11-01）

免费、免注册、免 Key 的第三路行情备源（调研：research/docs/29 或
docs/17 免费数据源降级备路调研报告）。实测可用端点：

  日K    http://d.10jqka.com.cn/v6/line/{mkt}_{code}/01/last.js
         JSONP：quotebridge_v6_line_hs_600519_01_last({...})
         data 字段为 ";" 分隔的行，每行 12 列：
         date,open,high,low,close,volume,amount,turnoverRate,?,?,?,?
  实时快照 https://d.10jqka.com.cn/v2/realhead/{mkt}_{code}/last.js
         JSONP：quotebridge_v2_realhead_hs_600519_last({"items":{...}})
         items: 5=代码 6=今开 7=昨收 8=最高 9=最低 10=最新价
                13=成交量(股) 19=成交额(元) 69=涨停价 70=跌停价

与腾讯实测逐字段对齐（2026-10-30 600519）：
  THS  close=1392.05 volume=5,992,729(股)
  腾讯  close=1392.05 volume=59,927(手)×100=5,992,700(股)
→ **THS 的 volume 单位就是"股"**，与项目落库口径一致，无需换算
  （腾讯 688/689 返回股、其他板返回手的分裂问题在本源不存在）。

工程约束（调研实测结论）：
  - 必须带浏览器 UA + Referer + Cookie（缺 Cookie 被拒）
  - urllib 默认头会被 502，必须用 requests
  - 无公开频控但"偶发 502" → 同域串行 + 0.3~1s 间隔 + 连接复用
  - **单标的单请求**（无批量接口）→ 仅用于 daily 降级备路，
    不接入 realtime 全市场链路（否则 5000+ 请求不可接受）
"""
from __future__ import annotations

import json
import random
import re
import threading
import time

import pandas as pd
import requests
from loguru import logger

_JSONP_RE = re.compile(r'^\s*[\w.]+\((.*)\)\s*;?\s*$', re.DOTALL)


class _Throttle:
    """同花顺限速：同域串行 + 0.3~1s 随机间隔（调研实测：偶发 502）"""

    MIN_INTERVAL = 0.35
    JITTER_RANGE = (0.15, 0.65)

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
_SESSION = requests.Session()


class ThsWebSource:
    """同花顺网页端行情源（免费备路）"""

    HEADERS = {  # noqa: RUF012 类级只读常量，运行期无原地修改
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Referer': 'http://stockpage.10jqka.com.cn/',
        'Cookie': 'v=AAAA; hexin-v=AAAA',
        'Accept': '*/*',
        'Connection': 'keep-alive',
    }

    LINE_URL = 'http://d.10jqka.com.cn/v6/line/{mkt}{code}/01/last.js'
    HEAD_URL = 'https://d.10jqka.com.cn/v2/realhead/{mkt}{code}/last.js'

    # ---------- 内部工具 ----------

    @staticmethod
    def _market_prefix(code: str) -> str:
        """标准代码 → THS 市场前缀（hs_/sz_/bj_）

        注意：THS 的前缀并非严格路由（实测 hs_ 与 sz_ 对 000001 返回同一支
        平安银行），按代码段选择即可，取不到由上层降级。
        """
        sym = str(code or '').split('.')[0]
        if not sym.isdigit() or len(sym) != 6:
            return 'hs_'
        if sym[0] == '6':
            return 'hs_'
        if sym[0] in ('4', '8') or sym.startswith('92'):
            return 'bj_'
        return 'sz_'

    @staticmethod
    def _get_jsonp(url: str) -> dict | None:
        """请求并解析 JSONP 包装（失败返 None，不抛异常）"""
        try:
            _THROTTLE.wait()
            resp = _SESSION.get(url, headers=ThsWebSource.HEADERS, timeout=15)
            if resp.status_code != 200:
                logger.debug(f"THS web HTTP {resp.status_code}: {url}")
                return None
            m = _JSONP_RE.match(resp.text)
            if not m:
                return None
            return json.loads(m.group(1))
        except Exception as e:  # 备路失败静默降级
            logger.debug(f"THS web request failed: {type(e).__name__}: {e}")
            return None

    # ---------- 数据接口 ----------

    @staticmethod
    def get_daily_data(code: str, days: int = 120) -> pd.DataFrame:
        """获取日线数据（前复权口径由同花顺服务端提供）

        Returns:
            DataFrame[date/open/high/low/close/volume/amount/turnover_rate]
            volume 单位为「股」，与项目落库口径一致。
        """
        sym = str(code or '').split('.')[0]
        mkt = ThsWebSource._market_prefix(code)
        payload = ThsWebSource._get_jsonp(ThsWebSource.LINE_URL.format(mkt=mkt, code=sym))
        if not payload:
            return pd.DataFrame()
        raw = payload.get('data') or ''
        if not raw:
            return pd.DataFrame()
        records = []
        for line in raw.split(';'):
            parts = line.split(',')
            if len(parts) < 8:
                continue
            try:
                d = parts[0]
                if len(d) == 8:
                    date = f'{d[:4]}-{d[4:6]}-{d[6:]}'
                else:
                    date = d
                records.append({
                    'date': date,
                    'open': float(parts[1] or 0),
                    'high': float(parts[2] or 0),
                    'low': float(parts[3] or 0),
                    'close': float(parts[4] or 0),
                    'volume': float(parts[5] or 0),        # 股
                    'amount': float(parts[6] or 0),       # 元
                    'turnover_rate': float(parts[7] or 0),
                })
            except (ValueError, IndexError):
                continue
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df = df[df['close'] > 0].sort_values('date').reset_index(drop=True)
        if days and len(df) > days:
            df = df.tail(days).reset_index(drop=True)
        return df

    @staticmethod
    def get_realtime_quote(codes: list[str]) -> pd.DataFrame:
        """获取实时快照（单标的单请求——只适合小批量）"""
        records = []
        for code in (codes or [])[:20]:
            sym = str(code).split('.')[0]
            mkt = ThsWebSource._market_prefix(code)
            payload = ThsWebSource._get_jsonp(ThsWebSource.HEAD_URL.format(mkt=mkt, code=sym))
            if not payload:
                continue
            items = payload.get('items') or {}
            try:
                price = float(items.get('10') or 0)
                prev = float(items.get('7') or 0)
                records.append({
                    'code': f'{sym}.{ThsWebSource._market_suffix(code)}',
                    'name': '',
                    'price': price,
                    'prev_close': prev,
                    'change_pct': round((price - prev) / prev * 100, 4) if prev else 0.0,
                    'volume': float(items.get('13') or 0),      # 股
                    'amount': float(items.get('19') or 0),      # 元
                    'high': float(items.get('8') or 0),
                    'low': float(items.get('9') or 0),
                    'open': float(items.get('6') or 0),
                })
            except (ValueError, TypeError):
                continue
        return pd.DataFrame(records)

    @staticmethod
    def _market_suffix(code: str) -> str:
        """标准代码 → 市场后缀（SH/SZ/BJ）"""
        sym = str(code or '').split('.')[0]
        if sym.isdigit() and len(sym) == 6:
            if sym[0] == '6':
                return 'SH'
            if sym[0] in ('4', '8') or sym.startswith('92'):
                return 'BJ'
            if sym[0] in ('0', '3'):
                return 'SZ'
        return 'SH'

    @staticmethod
    def get_stock_list() -> pd.DataFrame:
        """同花顺网页端无全市场列表接口（股票池仍由新浪 hs_a 主导）"""
        return pd.DataFrame()
