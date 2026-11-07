"""东方财富数据源"""
from __future__ import annotations

import random
import re
import threading
import time

import pandas as pd
import requests
from loguru import logger

from ...utils.code_utils import market_by_symbol, to_secid


class _Throttle:
    """东财"三招"之一：限速 + 随机抖动（对东财，慢，就是快）

    东财对高频同源请求敏感：从生产 ECS 访问 push2his / push2 曾因
    IP 级风控被 Connection abort（capital_flow_5d 仅 3/33 非空）。
    对策（用户 2026-11-01 指示）：
      ① 限速：两次请求至少间隔 MIN_INTERVAL 秒
      ② 随机抖动：每次请求额外加随机延迟，避免固定节奏像机器人
      ③ 连接复用：所有请求共用一个 Session（keep-alive，见 _SESSION）

    抖动叠加在限速之上（而非替代），保证间隔 ≥1s 且节奏不可预测。
    """

    MIN_INTERVAL = 1.0            # 两次请求最小间隔（秒）
    JITTER_RANGE = (0.15, 0.6)    # 额外随机抖动区间（秒）

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

# 三招之二/之三：所有东财请求共用一个 Session（连接复用 + keep-alive）
_SESSION = requests.Session()


class EastMoneySource:
    """东方财富数据源"""

    BASE_URL = "https://push2.eastmoney.com/api/qt/clist/get"

    HEADERS = {  # noqa: RUF012 类级只读常量，运行期无原地修改
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
    }

    # 全市场列表 fs 过滤（深主板+创业板+沪主板+科创）
    # 注意：此 fs 不含 ETF/LOF 基金（m:1+t:3）与北交所（m:2+t:8）。
    # 覆盖率扩展需实测确认后启用；代码解析的正确性已由 _market_from_em 保证，
    # 北交所代码不会因本 fs 缺失而被错判（查不到即不返回，不会错标市场）。
    CLIST_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"

    @staticmethod
    def _market_from_em(f12: str, f13=None) -> str | None:
        """由东财字段推断标准市场后缀

        优先使用 f13（东财市场字段：1=沪市, 0=深市场组）；但北交所在东财
        体系 f13 同样为 0（920819 实测），因此 f13=0 时须按代码前缀
        规则细分 BJ/SZ，不能一概判 SZ（历史 bug：920819 被错判）。
        f13 缺失时直接走前缀规则兜底（ETF 5 开头→SH、科创 688→SH）。
        """
        symbol = str(f12 or '').strip()
        if not re.match(r'^\d{6}$', symbol):
            return None
        by_prefix = market_by_symbol(symbol)
        if f13 is None:
            return by_prefix
        try:
            f13i = int(f13)
        except (TypeError, ValueError):
            return by_prefix
        if f13i == 1:
            return 'SH'
        if f13i == 0:
            # 北交所 4/8/92 开头在东财归 0 组，按前缀规则细分 SZ/BJ
            return by_prefix
        return by_prefix

    @staticmethod
    def _get(url: str, params: dict, headers: dict | None = None, timeout: int = 15):
        """统一请求入口：限速 + 随机抖动 + 连接复用

        三招合一——所有东财请求必须经此函数，不得直接 requests.get。
        """
        _THROTTLE.wait()
        merged = dict(EastMoneySource.HEADERS)
        if headers:
            merged.update(headers)
        return _SESSION.get(url, params=params, headers=merged, timeout=timeout)

    @staticmethod
    def _get_json(url: str, params: dict, tries: int = 3, headers: dict | None = None):
        """带重试的 GET+JSON 解析（代理下 push2 偶发 RemoteDisconnected，
        实测重试 1-2 次即恢复；M2' 备路依赖本方法稳定性）"""
        last_err = None
        for i in range(tries):
            try:
                response = EastMoneySource._get(url, params, headers=headers)
                if response.status_code == 200:
                    return response.json()
                last_err = f"HTTP {response.status_code}"
            except Exception as e:
                last_err = e
                time.sleep(0.5 * (i + 1))
        logger.warning(f"EM request failed after {tries} tries: {last_err}")
        return None

    @staticmethod
    def get_all_stocks() -> pd.DataFrame:
        """获取所有A股实时行情"""
        try:
            params = {
                "pn": "1",
                "pz": "5000",
                "po": "1",
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": EastMoneySource.CLIST_FS,
                "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18",
            }
            data = EastMoneySource._get_json(
                "https://push2.eastmoney.com/api/qt/clist/get", params)
            if data and 'data' in data and data['data'] is not None:
                items = data['data'].get('diff', [])
                records = []
                for item in items:
                    f12 = item.get('f12', '')
                    market = EastMoneySource._market_from_em(f12, item.get('f13'))
                    if not market:
                        logger.warning(f"EM clist: unrecognized code {f12!r}, skipped")
                        continue
                    records.append({
                        'code': f"{f12}.{market}",
                        'name': item.get('f14', ''),
                        'price': item.get('f2', 0),
                        'change_pct': item.get('f3', 0),
                        'volume': item.get('f5', 0),
                        'amount': item.get('f6', 0),
                    })
                return pd.DataFrame(records)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"EastMoney get all stocks error: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_realtime_quote(codes: list[str]) -> pd.DataFrame:
        """获取指定股票实时行情"""
        try:
            # 转换代码格式（统一走 code_utils，支持 SH/SZ/BJ）
            em_codes = []
            for code in codes:
                secid = to_secid(code)
                if not secid:
                    logger.warning(f"EM realtime: invalid code {code!r}, skipped")
                    continue
                em_codes.append(secid)

            if not em_codes:
                return pd.DataFrame()

            url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
            params = {
                "fltt": "2",
                "fields": "f2,f3,f4,f5,f6,f12,f13,f14",
                "secids": ",".join(em_codes),
            }
            response = EastMoneySource._get(url, params)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data'] is not None:
                    items = data['data'].get('diff', [])
                    records = []
                    for item in items:
                        f12 = item.get('f12', '')
                        market = EastMoneySource._market_from_em(f12, item.get('f13'))
                        if not market:
                            logger.warning(f"EM ulist: unrecognized code {f12!r}, skipped")
                            continue
                        records.append({
                            'code': f"{f12}.{market}",
                            'name': item.get('f14', ''),
                            'price': item.get('f2', 0),
                            'change_pct': item.get('f3', 0),
                            'volume': item.get('f5', 0),
                        })
                    return pd.DataFrame(records)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"EastMoney get realtime quote error: {e}")
            return pd.DataFrame()

    # 日K（push2his，2026-11-06 实测：status 200 且含当日 bar）
    KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

    @staticmethod
    def get_daily_data(code: str, days: int = 120) -> pd.DataFrame:
        """日K（D66：补齐 daily 优先级里的死配置 + 作为跨源复核源）

        背景：``_PRIORITY['daily']`` 长期列有 ``eastmoney``，但本类从未实现
        ``get_daily_data``，``execute_with_fallback`` 的 ``getattr(..., None)``
        会静默跳过 —— 腾讯限流(501) → 新浪(签名错误) 之后，实际只剩同花顺
        一根独苗。本方法把该备路补实，同时供
        ``utils/daily_cross_check`` 做「末根日期/收盘价」针对性复核。

        口径：``fqt=0`` 不复权（prd/02 §1 规定），``klt=101`` 日线。
        返回列与其余日K源一致：date/open/high/low/close/volume。
        """
        try:
            secid = to_secid(str(code))
            params = {
                'secid': secid,
                'fields1': 'f1,f2,f3,f4,f5,f6',
                'fields2': 'f51,f52,f53,f54,f55,f56,f57',
                'klt': '101',
                'fqt': '0',
                'end': '20500101',
                'lmt': str(min(int(days), 1024)),
            }
            data = EastMoneySource._get_json(EastMoneySource.KLINE_URL, params)
            node = (data or {}).get('data') or {}
            klines = node.get('klines') or []
            records = []
            for line in klines:
                parts = str(line).split(',')
                if len(parts) < 6:
                    continue
                try:
                    records.append({
                        'date': parts[0],
                        'open': float(parts[1] or 0),
                        'close': float(parts[2] or 0),
                        'high': float(parts[3] or 0),
                        'low': float(parts[4] or 0),
                        'volume': float(parts[5] or 0),
                    })
                except (ValueError, IndexError):
                    continue
            if not records:
                return pd.DataFrame()
            df = pd.DataFrame(records)
            return df[df['close'] > 0].sort_values('date').reset_index(drop=True)
        except Exception as e:  # 备源异常不得影响主链
            logger.warning(f"EM daily {code} error: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_capital_flow(code: str, days: int = 5) -> float | None:
        """获取近 N 个交易日主力资金净流入累计（元）

        东财个股资金流 K 线（fflow/kline，klt=101 日频）：
        fields2 f51=日期, f52=主力净流入, f53=小单, f54=中单,
        f55=大单, f56=超大单（单位: 元）。
        主用 push2his（约 120 根日频历史），失败降级 push2（仅当日）。
        按日期排序后取最近 days 条累加 f52（含当日，盘中为当日累计值）。

        Args:
            code: 标准代码（如 "300450.SZ"）
            days: 累计交易日数（默认 5）

        Returns:
            近 N 日主力净流入累计（元）；接口失败/解析异常返回 None（降级）
        """
        secid = to_secid(code)
        if not secid:
            logger.warning(f"EM capital flow: invalid code {code!r}")
            return None
        params = {
            "lmt": "0",
            "klt": "101",
            "secid": secid,
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56",
        }
        # 资金流接口属 data.eastmoney.com 域，Referer 与行情站不同
        headers = dict(EastMoneySource.HEADERS)
        headers['Referer'] = 'https://data.eastmoney.com/'
        try:
            data = None
            for host in ("push2his.eastmoney.com", "push2.eastmoney.com"):
                data = EastMoneySource._get_json(
                    f"https://{host}/api/qt/stock/fflow/kline/get", params,
                    headers=headers)
                if data and data.get('data') is not None and \
                        data['data'].get('klines'):
                    break
                data = None
            if not data:
                return None
            klines = data['data'].get('klines') or []
            if not klines:
                return None
            # 解析 (日期, 主力净流入) 并按日期排序，取最近 days 条（不依赖返回顺序）
            rows: list[tuple[str, float]] = []
            for line in klines:
                parts = line.split(',')
                if len(parts) < 2 or parts[1] in ('', '-'):
                    continue
                rows.append((parts[0], float(parts[1])))
            if not rows:
                return None
            rows.sort(key=lambda r: r[0])
            head = rows[-days:] if days > 0 else rows
            return sum(v for _, v in head)
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"EM capital flow parse error for {code}: {e}")
            return None
