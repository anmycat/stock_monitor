"""腾讯财经数据源"""
import requests
import random
import pandas as pd
from typing import List, Dict, Optional, Tuple
from loguru import logger

from ...utils.code_utils import (to_tencent_code, normalize_code,
                                 MARKET_SH, MARKET_SZ, MARKET_BJ)


class TencentSource:
    """腾讯财经数据源 - qt.gtimg.cn"""

    BASE_URL = "https://qt.gtimg.cn/q="
    # 日线 K 线（fqkline，前复权；2026-10-20 实测，单只 ~0.4s）
    DAILY_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"

    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_0) AppleWebKit/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
    ]

    # 行首市场前缀 → 标准市场后缀（腾讯返回 v_sh510300="..." 自带市场，
    # 这是最权威的市场标识，历史 bug 是用 code.startswith('6') 推断，
    # 导致 5 开头的沪市 ETF（510300/518880）被错判为 .SZ）
    _MARKET_PREFIX_MAP = {
        'sh': MARKET_SH,
        'sz': MARKET_SZ,
        'bj': MARKET_BJ,
    }

    @staticmethod
    def _get_headers() -> dict:
        return {
            'User-Agent': random.choice(TencentSource.USER_AGENTS),
            'Referer': 'https://stockapp.finance.qq.com',
        }

    @staticmethod
    def _convert_code(code: str) -> str:
        """转换代码格式 510300.SH -> sh510300（支持 BJ）"""
        converted = to_tencent_code(code)
        return converted if converted else str(code)

    @staticmethod
    def _parse_quote(data: str, market: Optional[str] = None) -> Optional[Dict]:
        """解析腾讯行情数据

        Args:
            data: '=' 后的行情串（~ 分隔）
            market: 行首市场前缀（'sh'/'sz'/'bj'），由调用方从
                    v_sh510300= 提取；缺失时回退到数字前缀规则推断
        """
        try:
            parts = data.split('~')
            if len(parts) < 45:
                return None
            symbol = parts[2]
            if market:
                market_suffix = TencentSource._MARKET_PREFIX_MAP.get(market.lower())
                if not market_suffix:
                    return None
                code = f"{symbol}.{market_suffix}"
            else:
                # 兜底：无行首前缀时按静态规则推断（异常路径）
                from ...utils.code_utils import market_by_symbol
                inferred = market_by_symbol(symbol)
                if not inferred:
                    return None
                code = f"{symbol}.{inferred}"
            return {
                'code': code,
                'name': parts[1],
                'price': float(parts[3]) if parts[3] else 0,
                'change_pct': float(parts[32]) if parts[32] else 0,
                'volume': float(parts[6]) if parts[6] else 0,
            }
        except Exception:
            return None

    @staticmethod
    def get_realtime_quote(codes: List[str]) -> pd.DataFrame:
        """获取指定股票实时行情"""
        try:
            qt_codes = [TencentSource._convert_code(c) for c in codes]
            url = TencentSource.BASE_URL + ','.join(qt_codes)
            response = requests.get(url, headers=TencentSource._get_headers(), timeout=15)
            # 腾讯行情返回 GBK 编码，不显式设置会导致中文名称乱码
            response.encoding = 'gbk'
            if response.status_code == 200:
                content = response.text
                results = []
                for line in content.strip().split('\n'):
                    if '=' not in line:
                        continue
                    var_name, _, payload = line.partition('=')
                    # 从 v_sh510300 / v_bj920819 行首提取市场前缀
                    prefix = var_name.strip().replace('v_', '').strip()
                    market = prefix[:2] if prefix[:2] in TencentSource._MARKET_PREFIX_MAP else None
                    data = payload.strip(';').strip('"')
                    quote = TencentSource._parse_quote(data, market=market)
                    if quote:
                        results.append(quote)
                if results:
                    return pd.DataFrame(results)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Tencent get realtime quote error: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_daily_data(code: str, days: int = 120) -> pd.DataFrame:
        """获取日线数据（fqkline 前复权，2026-10-20 新增）

        修复日线链路断裂：原 _PRIORITY['daily'] 实际只有 akshare
        一条路且未安装，signals 全空（见 PRD 02 §3.8）。
        实测要点：
        - 返回结构 data[{qt_code}]['qfqday']，数组顺序
          [day, open, close, high, low, volume]
        - 北交所 key 为 'day' 而非 'qfqday'，需兼容
        - volume 单位为手，统一 ×100 转股（与新浪日线一致）
        返回列：date/open/high/low/close/volume
        """
        try:
            qt_code = TencentSource._convert_code(code)
            params = {
                'param': f"{qt_code},day,,,{min(int(days), 1024)},qfq",
            }
            response = requests.get(
                TencentSource.DAILY_URL, params=params,
                headers=TencentSource._get_headers(), timeout=15)
            if response.status_code != 200:
                logger.warning(
                    f"Tencent daily {code} HTTP {response.status_code}")
                return pd.DataFrame()
            data = response.json().get('data') or {}
            node = data.get(qt_code) or {}
            # 北交所回退 'day' 键
            bars = node.get('qfqday') or node.get('day') or []
            if not bars:
                return pd.DataFrame()
            records = []
            for bar in bars:
                if len(bar) < 6:
                    continue
                records.append({
                    'date': str(bar[0] or ''),
                    'open': float(bar[1] or 0),
                    'close': float(bar[2] or 0),
                    'high': float(bar[3] or 0),
                    'low': float(bar[4] or 0),
                    # 手 → 股（与新浪日线单位一致）
                    'volume': float(bar[5] or 0) * 100,
                })
            df = pd.DataFrame(records)
            df = df[df['close'] > 0].sort_values('date').reset_index(drop=True)
            return df
        except Exception as e:
            logger.error(f"Tencent get daily data error [{code}]: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_all_stocks() -> pd.DataFrame:
        """获取全市场实时行情（M2' 2026-10-21：动态代码池 + 腾讯分批报价）

        废弃 _get_main_stock_codes 硬编码号段主路（~406 只，缺科创/ETF/
        北交所/深市后段，且 min(len(codes),500) 截断——见 12 §3.14 C1）。
        三层代码池（_resolve_code_pool）：
          主路  新浪 hs_a 全市场列表（stock_list 第一线，实测 5575 只）
          备路  东财 clist 全市场列表（docs/17 P0-1 指定；代理/ECS 风控
                不稳定，故列备路——即 2026-10-20 东财降级的理由）
          兜底  静态号段（emergency-only，warning 留痕）
        报价统一复用 get_realtime_quote（qt.gtimg 分批，BATCH_SIZE=200，
        实测 200 只/0.17s，5575 只约 7s）。
        """
        try:
            codes, tier = TencentSource._resolve_code_pool()
            if not codes:
                logger.error("Tencent get_all_stocks: no code pool available")
                return pd.DataFrame()
            frames = []
            for i in range(0, len(codes), TencentSource.BATCH_SIZE):
                batch = codes[i:i + TencentSource.BATCH_SIZE]
                df = TencentSource.get_realtime_quote(batch)
                if not df.empty:
                    frames.append(df)
            if not frames:
                return pd.DataFrame()
            out = pd.concat(frames, ignore_index=True)
            # 去重（池源理论无重复，防御性保留）
            out = out.drop_duplicates(subset=['code'], keep='first')
            logger.info(
                f"Tencent pool[{tier}] {len(codes)} codes -> {len(out)} quotes")
            return out
        except Exception as e:
            logger.error(f"Tencent get all stocks error: {e}")
            return pd.DataFrame()

    # ---- M2' 代码池常量（2026-10-21）----
    # 动态池最小规模：低于此值视为源损坏（如东财代理截断曾返 100 行），
    # 不采信、继续降级——防止小池结果被上层 execute_with_fallback 当
    # 非空接受而阻断新浪/东财直连全量路径（覆盖数随应答源波动，C1 病灶）
    MIN_POOL_CODES = 1000
    # 运行时可上限（约 30 批，封顶耗时）
    MAX_POOL_CODES = 6000
    # qt.gtimg 单请求代码数（实测 200 只/批 0.17s，50 只/批 0.20s）
    BATCH_SIZE = 200

    @staticmethod
    def _resolve_code_pool() -> Tuple[List[str], str]:
        """三层代码池解析，返回 (codes, tier)"""
        # 主路：新浪 hs_a 全市场列表（延迟导入避免模块级环依赖）
        try:
            from .sina import SinaSource
            df = SinaSource.get_stock_list()
            pool = ([str(c) for c in df['code'].tolist() if c]
                    if df is not None and not df.empty and 'code' in df.columns
                    else [])
        except Exception as e:
            logger.warning(f"Tencent pool[sina_list] failed: {e}")
            pool = []
        if len(pool) >= TencentSource.MIN_POOL_CODES:
            return pool[:TencentSource.MAX_POOL_CODES], 'sina_list'
        # 备路：东财 clist 全市场列表
        try:
            from .eastmoney import EastMoneySource
            df = EastMoneySource.get_all_stocks()
            pool = ([str(c) for c in df['code'].tolist() if c]
                    if df is not None and not df.empty and 'code' in df.columns
                    else [])
        except Exception as e:
            logger.warning(f"Tencent pool[eastmoney_clist] failed: {e}")
            pool = []
        if len(pool) >= TencentSource.MIN_POOL_CODES:
            return pool[:TencentSource.MAX_POOL_CODES], 'eastmoney_clist'
        # 兜底：静态号段（emergency-only）
        logger.warning(
            "Tencent code pool: dynamic sources unavailable "
            f"(sina<{TencentSource.MIN_POOL_CODES}), "
            "falling back to static segments (emergency-only)")
        return TencentSource._get_main_stock_codes(), 'static_emergency'

    @staticmethod
    def _get_main_stock_codes() -> List[str]:
        """静态兜底号段（emergency-only，M2' 起不再作主路）

        仅覆盖沪市主板三段 + 深市主板两段 + 创业板一段；缺科创 688/
        ETF 5xx/北交所 8xx-92xx/深市后段。正常路径由 _resolve_code_pool
        动态池接管；本方法仅在动态源全部不可达时应急。
        """
        codes = []
        # 上海主板
        for i in range(600000, 600200):
            codes.append(f"{i}.SH")
        for i in range(600500, 600700):
            codes.append(f"{i}.SH")
        for i in range(601000, 601200):
            codes.append(f"{i}.SH")
        for i in range(601300, 601500):
            codes.append(f"{i}.SH")
        # 深圳主板
        for i in range(0, 200):
            codes.append(f"{i:06d}.SZ")
        for i in range(1000, 1200):
            codes.append(f"{i:06d}.SZ")
        # 创业板
        for i in range(300000, 300200):
            codes.append(f"{i:06d}.SZ")
        return codes
