"""天天基金基金持仓数据源 - ETF/普通基金季度股票投资明细

数据来源：东方财富天天基金 F10 基金档案-投资组合-基金持仓
接口：https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code=<基金代码>&topline=<N>&year=<年份>

实测口径（2026-10-18）：
- 一次请求返回指定年度的全部季度（每个季度一个 box），按季度倒序（最新在前）
- topline=100 稳定返回每季前 100 大持仓；topline>=200 会被服务端降级为 10（坑）
- 不带 topline 时只返回季度表头、tbody 为空（懒加载，勿用）
- 仅最新季度的行含"最新价/涨跌幅"两列（且为 JS 占位 span，无数值）；历史季度只有
  占净值比例/持股数(万股)/持仓市值(万元) 三列
- 指数型 ETF 季报披露全部持仓，本接口取前 N 大；主动权益基金仅十大重仓
- 需要 Referer: https://fundf10.eastmoney.com/ccmx_<code>.html，否则可能被拒
"""
from __future__ import annotations

import re
import time

import pandas as pd
import requests
from loguru import logger


class EastMoneyFundHoldingsSource:
    """天天基金-基金季度持仓数据源"""

    BASE_URL = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    DEFAULT_TOPLINE = 100  # 服务端上限实测约 100，>=200 会被降级为 10

    _max_retries = 2
    _retry_sleep = 0.8

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        })

    # ------------------------------------------------------------------ 公共

    def get_holdings(self, code: str, years: list[int] | None = None,
                     topline: int = DEFAULT_TOPLINE) -> pd.DataFrame:
        """获取基金多个年度的季度持仓明细

        Args:
            code: 基金代码（6位数字，如 "510300"）
            years: 年份列表；None 时取当前年与上一年
            topline: 每季返回的最大持仓数（默认100，勿超过100）

        Returns:
            DataFrame，列：quarter(如2026Q2)/report_date(截止日)/seq/code/name/
            weight(占净值比例%)/shares(持股数万股)/market_value(持仓市值万元)
            无数据时返回空 DataFrame
        """
        code = str(code).split('.')[0]
        if years is None:
            this_year = pd.Timestamp.now().year
            years = [this_year - 1, this_year]

        # M4：ETF 持仓抓取整体走子进程硬超时（多年份串行 + 限流，最坏
        # 2 年 × 请求超时；20s 覆盖正常路径，卡死则杀掉不影响 worker）
        # 注意：本模块在 quant_engine/data/sources/ 下，相对导入须三点（...utils）；
        # 两点会解析到不存在的 quant_engine.data.utils（D50 同款事故，见 D52 登记）
        from ...utils.fetch_guard import guarded_fetch

        def _collect() -> pd.DataFrame:
            frames = []
            for year in years:
                df = self._fetch_year(code, year, topline)
                if df is not None and not df.empty:
                    frames.append(df)
                time.sleep(0.3)  # 轻微限流，避免触发风控
            if not frames:
                return pd.DataFrame(
                    columns=['quarter', 'report_date', 'seq', 'code', 'name',
                             'weight', 'shares', 'market_value']
                )
            result = pd.concat(frames, ignore_index=True)
            result = result.drop_duplicates(subset=['quarter', 'code'], keep='first')
            return result.sort_values(['report_date', 'seq'],
                                      ascending=[False, True]).reset_index(drop=True)

        guarded = guarded_fetch(_collect, timeout=20.0, name=f'etf.{code}')
        if guarded is None:
            logger.warning(f"[FundHoldings] {code} 抓取超时或失败，返回空表")
            return pd.DataFrame(
                columns=['quarter', 'report_date', 'seq', 'code', 'name',
                         'weight', 'shares', 'market_value']
            )
        return guarded

    def get_available_years(self, code: str) -> list[int]:
        """获取该基金有持仓数据的年份列表（降序）"""
        code = str(code).split('.')[0]
        params = {
            'type': 'jjcc', 'code': code,
            'topline': str(self.DEFAULT_TOPLINE), 'year': str(pd.Timestamp.now().year),
        }
        headers = {'Referer': f'https://fundf10.eastmoney.com/ccmx_{code}.html'}
        try:
            resp = self.session.get(self.BASE_URL, params=params,
                                    headers=headers, timeout=15)
            if resp.status_code != 200:
                return []
            m = re.search(r'arryear:\[([\d,]*)\]', resp.text)
            if not m:
                return []
            return [int(y) for y in m.group(1).split(',') if y.strip()]
        except Exception as e:
            logger.warning(f"Fund available years failed {code}: {e}")
            return []

    # ------------------------------------------------------------------ 内部

    def _fetch_year(self, code: str, year: int, topline: int) -> pd.DataFrame | None:
        content = self._request(code, year=year, topline=topline)
        if not content:
            return None
        try:
            return self._parse(content)
        except Exception as e:
            logger.warning(f"Fund holdings parse error {code} {year}: {e}")
            return None

    def _request(self, code: str, year: int, topline: int) -> str | None:
        """请求接口，返回 apidata.content 原文（失败返回None）"""
        params = {
            'type': 'jjcc',
            'code': code,
            'topline': str(int(topline)),
            'year': str(int(year)),
        }
        headers = {'Referer': f'https://fundf10.eastmoney.com/ccmx_{code}.html'}

        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self.session.get(self.BASE_URL, params=params,
                                        headers=headers, timeout=15)
                if resp.status_code != 200:
                    raise ValueError(f"HTTP {resp.status_code}")
                text = resp.text
                if 'apidata' not in text:
                    raise ValueError("unexpected response (no apidata)")
                # var apidata={ content:"...",arryear:[...],curyear:...};
                m = re.search(r'content:"(.*)",arryear', text, re.DOTALL)
                if not m:
                    raise ValueError("content field not found")
                return m.group(1)
            except Exception as exc:
                last_error = exc
                if attempt < self._max_retries:
                    time.sleep(self._retry_sleep * attempt)

        logger.warning(f"Fund holdings request failed {code} {year}: "
                       f"{type(last_error).__name__}: {last_error}")
        return None

    def _parse(self, content: str) -> pd.DataFrame:
        """解析多季度HTML为DataFrame"""
        records = []
        # 按 <div class='box'> 拆分为多个季度区块
        boxes = re.split(r"<div class='boxitem", content)
        for box in boxes:
            quarter, report_date = self._parse_quarter_header(box)
            if not quarter:
                continue
            records.extend(self._parse_rows(box, quarter, report_date))

        if not records:
            return pd.DataFrame(
                columns=['quarter', 'report_date', 'seq', 'code', 'name',
                         'weight', 'shares', 'market_value']
            )
        return pd.DataFrame(records)

    def _parse_quarter_header(self, box: str):
        """从区块头部解析季度标签与截止日期"""
        m = re.search(r'(20\d\d)年([1-4])季度股票投资明细', box)
        if not m:
            return None, None
        year, q = int(m.group(1)), int(m.group(2))
        quarter = f"{year}Q{q}"
        d = re.search(r"截止至：<font class='px12'>([\d-]+)</font>", box)
        report_date = d.group(1) if d else ''
        return quarter, report_date

    def _parse_rows(self, box: str, quarter: str, report_date: str) -> list[dict]:
        """解析一个季度区块内的持仓行"""
        records = []
        for tr in re.findall(r'<tr>(.*?)</tr>', box, re.DOTALL):
            code_m = re.search(r"unify/r/[01]\.(\d{6})", tr)
            name_m = re.search(r"class='tol'><a [^>]*>([^<]+)</a>", tr)
            if not code_m or not name_m:
                continue  # 表头行或无代码行
            # tor 单元格：过滤掉空值（最新季度的价格/涨跌幅为JS占位span），
            # 最后三个有效值依次为 占净值比例/持股数/持仓市值
            nums = []
            for td in re.findall(r"<td class='tor'>(.*?)</td>", tr, re.DOTALL):
                val = re.sub(r'<[^>]+>', '', td).strip()
                if val:
                    nums.append(val)
            if len(nums) < 3:
                continue
            weight, shares, mv = nums[-3], nums[-2], nums[-1]
            seq_m = re.search(r'<td>(\d+)</td>', tr)
            records.append({
                'quarter': quarter,
                'report_date': report_date,
                'seq': int(seq_m.group(1)) if seq_m else 0,
                'code': self._normalize_code(code_m.group(1)),
                'name': name_m.group(1).strip(),
                'weight': self._to_float(weight),
                'shares': self._to_float(shares),
                'market_value': self._to_float(mv),
            })
        return records

    @staticmethod
    def _normalize_code(symbol: str) -> str:
        """东财市场前缀码 -> 项目标准码（600519.SH / 300750.SZ / 920819.BJ）

        统一走 utils.code_utils.normalize_code，覆盖全部代码段
        （含 92 开头北交所新段与 5 开头沪市基金）；无法识别时原样
        返回（由 CodeGuard 盘前校验发现并告警）。
        """
        from ...utils.code_utils import normalize_code
        return normalize_code(symbol) or symbol

    @staticmethod
    def _to_float(text: str) -> float:
        """'4.75%' / '4,371.20' -> float；失败返回0.0"""
        try:
            return float(str(text).replace('%', '').replace(',', '').strip())
        except (ValueError, AttributeError):
            return 0.0
