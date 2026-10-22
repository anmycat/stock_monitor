"""代码一致性校验服务（CodeGuard）

背景（事故驱动）：
原项目出现过「代码与实际股票不匹配」——同一股票在不同数据源被解析成
不同标准代码（详见 utils/code_utils.py docstring），造成缓存分裂、
ETF 监控匹配失效、持仓 diff 键错乱等问题。

本服务对全部持久化监控目标做「代码-市场-名称-价格」四重校验，
把事故从事后发现变为开盘前自动拦截。两个校验窗口与 worker 时段配合：

1. check_static —— 盘前（PRE_MARKET，9:00-9:15，静态校验）
   - 格式合法性：is_valid_code
   - 市场标记：本地 code 后缀 vs 权威源市场（腾讯行首 v_sh/sz/bj）
   - 存在性：权威源查得到（查无=疑似退市/错码）
   - 名称新鲜度：本地 name vs 权威 name（ST/简称做归一化后比较）

2. check_auction —— 集合竞价（SELF_CHECK，9:15-9:25，顺便校验缓存）
   - 价格连续性：quote:{code} 缓存昨收 vs 竞价匹配价，跳空超过阈值
     视为缓存串号疑云（超过涨跌停板缓冲）
   - 名称再校验（竞价数据更实时）
   - ETF 持仓快照成分 code 与权威市场对账

修复策略（repair）：
- 市场标记错误（🔴）：自动修复——删错误 quote 键，写正确键
- 名称变更（🟡）：更新 code_guard:names 权威名映射，供下游选用；
  不修改 F10 持仓快照（那是历史事实）
- 退市/查无（🔴）：不自动删配置，记入报告并推送告警

推送策略：仅有 🔴/🟡 时推送钉钉（避免每日全绿骚扰）；全绿记日志。
"""
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from loguru import logger

from ..data.repositories import MarketDataRepository
from ..data.sources.tencent import TencentSource
from ..utils.code_utils import (normalize_code, is_valid_code, code_key,
                                split_code, market_by_symbol)
from .monitors.etf_monitor import HOT_ETFS

# 严重度
SEV_ERROR = 'error'      # 🔴 错码/查无/缓存串号——必须人工或自动修复
SEV_WARNING = 'warning'  # 🟡 改名/价格异常——知情即可
SEV_OK = 'ok'            # 🟢 通过


class CodeGuard:
    """代码-股票一致性校验"""

    # Redis 键与 TTL
    KEY_CHECK_DATE_STATIC = "code_guard:check_date_static"
    KEY_CHECK_DATE_AUCTION = "code_guard:check_date_auction"
    KEY_REPORT = "code_guard:report"
    KEY_NAMES = "code_guard:names"            # 权威 code→name 映射
    TTL_REPORT = 7 * 86400
    TTL_CHECK_DATE = 2 * 86400
    TTL_NAMES = 7 * 86400

    # 参数
    BATCH_SIZE = 50                # 腾讯批量查询每批代码数
    REQUEST_INTERVAL = 0.15        # 批次间隔（秒），限流
    PRICE_GAP_THRESHOLD = 0.11     # 竞价价 vs 缓存昨收跳空阈值（涨跌停10%+缓冲）
    NAME_IGNORE_PREFIXES = ('ST', '*ST', 'N', 'C', 'XD', 'XR', 'DR')  # 名称归一化时剥离

    def __init__(self, datacenter: Optional[MarketDataRepository] = None):
        self.datacenter = datacenter
        self.tencent = TencentSource()

    # ------------------------------------------------------------------ 目标收集

    def collect_targets(self) -> Dict[str, Dict]:
        """收集全部持久化监控目标

        Returns:
            {标准代码: {'name': 本地名称, 'source': 来源标签}}
            来源：hot_etf / etf_holdings / stock_list
        """
        targets: Dict[str, Dict] = {}

        # 1. 热门 ETF（配置常量）
        for etf in HOT_ETFS:
            code = normalize_code(etf['code'])
            if code:
                targets[code] = {'name': etf.get('name', ''), 'source': 'hot_etf'}

        # 2. ETF 持仓快照成分股（最新季，etf_holdings:{code}:{quarter}）
        holdings_codes = self._collect_holdings_codes()
        for code, name in holdings_codes.items():
            if code not in targets:
                targets[code] = {'name': name, 'source': 'etf_holdings'}

        # 3. 全市场列表缓存（stock_list）——校验其 code 格式合法性
        if self.datacenter is not None:
            try:
                cached = self.datacenter.cache.get('stock_list')
                if cached:
                    for row in cached:
                        raw = row.get('code', '')
                        code = normalize_code(raw)
                        if code and code not in targets:
                            targets[code] = {
                                'name': row.get('name', ''),
                                'source': 'stock_list',
                            }
            except Exception as e:  # noqa: BLE001 缓存IO
                logger.debug(f"CodeGuard stock_list collect error: {e}")

        return targets

    def _collect_holdings_codes(self) -> Dict[str, str]:
        """从 Redis 聚合全部 ETF 持仓快照的成分股（code -> name）"""
        result: Dict[str, str] = {}
        if self.datacenter is None:
            return result
        try:
            # latest 键给出最新季度，再取快照键拿成分明细
            latest_keys = self.datacenter.cache.keys('etf_holdings:latest:*')
            for lk in latest_keys:
                latest = self.datacenter.cache.get(lk)
                if not latest:
                    continue
                quarter = latest.get('quarter')
                etf_code = latest.get('etf_code') or ''
                symbol = code_key(etf_code)
                if not quarter or not symbol:
                    continue
                snapshot = self.datacenter.cache.get(
                    f'etf_holdings:{symbol}:{quarter}')
                if not snapshot:
                    continue
                for row in snapshot:
                    code = normalize_code(row.get('code', ''))
                    if code and code not in result:
                        result[code] = row.get('name', '')
        except Exception as e:  # noqa: BLE001 缓存IO
            logger.debug(f"CodeGuard holdings collect error: {e}")
        return result

    # ------------------------------------------------------------------ 权威行情

    def fetch_authoritative(self, codes: List[str]) -> Dict[str, Dict]:
        """批量拉取权威行情（腾讯源，行首市场前缀 + GBK 名称）

        查询码用静态规则推断的市场构造（本地后缀可能本身就是错的，
        不能信任）；返回的 code 以腾讯行首市场为权威。

        交易时间门控说明（2026-10-20 用户决策固化）：本方法**直连**
        TencentSource，不经 datacenter.get_all_realtime_quotes 的
        is_trading_time 门控——CodeGuard 是数据校验链路（盘前/竞价
        各一次的低频拉取），不是监控运算；"只在交易时间取数运算"
        的约定适用于 burst/信号/大盘分析，新闻推送与数据校验按其
        自身窗口定义（见 PRD 01 §7.5 / 07 §3.2）。

        Returns:
            {标准代码: {'market': 'SH', 'name': '平安银行', 'price': 11.5}}
        """
        authoritative: Dict[str, Dict] = {}
        query_codes = []
        for code in codes:
            parts = split_code(code)
            if not parts:
                continue
            symbol, local_market = parts
            # 用静态规则推断市场（本地后缀可能错，不信任）
            inferred_market = market_by_symbol(symbol) or local_market
            query_codes.append(f"{symbol}.{inferred_market}")

        for i in range(0, len(query_codes), self.BATCH_SIZE):
            batch = query_codes[i:i + self.BATCH_SIZE]
            try:
                df = self.tencent.get_realtime_quote(batch)
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    code = normalize_code(row.get('code', ''))
                    if not code:
                        continue
                    parts = split_code(code)
                    authoritative[code] = {
                        'market': parts[1],
                        'name': str(row.get('name', '') or ''),
                        'price': float(row.get('price', 0) or 0),
                    }
            except Exception as e:  # noqa: BLE001 外部调用
                logger.warning(f"CodeGuard authoritative batch error: {e}")
            time.sleep(self.REQUEST_INTERVAL)
        return authoritative

    # ------------------------------------------------------------------ 校验

    def _match_authoritative(self, code: str, authoritative: Dict) -> Optional[Tuple]:
        """在权威结果中匹配本地 code

        先精确匹配；本地码后缀错时（如 510300.SZ）按 6 位 key 匹配，
        返回 (权威码, 数据)，由调用方判定 market_mismatch。
        """
        if code in authoritative:
            return code, authoritative[code]
        key = code_key(code)
        if key:
            for acode, aval in authoritative.items():
                if code_key(acode) == key:
                    return acode, aval
        return None

    def check_static(self) -> Dict:
        """盘前静态校验（格式/市场标记/存在性/名称）

        Returns:
            校验报告 dict（见 _finalize_report）
        """
        started = datetime.now()
        targets = self.collect_targets()
        logger.info(f"CodeGuard static check: {len(targets)} targets")
        authoritative = self.fetch_authoritative(list(targets.keys()))
        logger.info(f"CodeGuard authoritative quotes: {len(authoritative)}")

        issues: List[Dict] = []
        checked = 0

        for code, info in targets.items():
            checked += 1
            # 1. 格式合法性（collect_targets 已 normalize，双保险）
            if not is_valid_code(code):
                issues.append(self._issue(SEV_ERROR, 'invalid_format', code,
                                          info['name'], f"代码格式非法: {code}"))
                continue

            auth = self._match_authoritative(code, authoritative)
            if auth is None:
                # 2. 存在性：权威源查无（连按静态规则市场都查不到）
                #   恒定为 warning：腾讯批量可能因限流丢批造成误报，
                #   保守不升级 error；疑似退市由人工看报告确认
                issues.append(self._issue(
                    SEV_WARNING, 'missing', code, info['name'],
                    f"权威行情源未返回（疑似退市/错码/限流丢批）: {code}"))
                continue

            auth_code, auth = auth
            # 3. 市场标记：本地 code vs 权威 code（同 6 位 key 不同后缀=错判）
            if auth_code != code:
                issues.append(self._issue(
                    SEV_ERROR, 'market_mismatch', code, info['name'],
                    f"市场标记错误: {code} 应为 {auth_code}",
                    detail={'correct_code': auth_code}))
                continue

            # 4. 名称新鲜度（hot_etf 为手写简称，跳过名称校验）
            if info.get('source') != 'hot_etf':
                name_issue = self._check_name(code, info['name'], auth['name'])
                if name_issue:
                    issues.append(name_issue)

            # 5. 价格合理性
            if auth['price'] <= 0:
                issues.append(self._issue(
                    SEV_WARNING, 'bad_price', code, auth['name'],
                    f"权威价异常: {auth['price']}"))

        report = self._finalize_report('static', started, checked,
                                       len(authoritative), issues)
        self._repair_static(issues, authoritative)
        self._save_report(report)
        return report

    def check_auction(self) -> Dict:
        """集合竞价校验（价格连续性 + 名称 + 持仓快照对账）

        利用 9:15-9:25 竞价期间的虚拟匹配价，与 quote:{code} 缓存中的
        昨收比较：跳空超过阈值说明缓存可能串号（存错了股票的价格）。
        """
        started = datetime.now()
        targets = self.collect_targets()
        logger.info(f"CodeGuard auction check: {len(targets)} targets")
        authoritative = self.fetch_authoritative(list(targets.keys()))

        issues: List[Dict] = []
        checked = 0

        for code, info in targets.items():
            checked += 1
            matched = self._match_authoritative(code, authoritative)
            if matched is None:
                continue  # 存在性由 static 窗口负责，竞价不重复告警
            auth_code, auth = matched

            # 1. 名称再校验（竞价数据更实时；hot_etf 手写简称跳过）
            if info.get('source') != 'hot_etf':
                name_issue = self._check_name(code, info['name'], auth['name'])
                if name_issue:
                    issues.append(name_issue)

            # 2. 价格连续性：缓存昨收 vs 竞价价（以权威码为准）
            cached = self._get_cached_quote(auth_code)
            if not cached:
                continue
            cached_price = self._to_float(cached.get('price'))
            auction_price = auth['price']
            if cached_price > 0 and auction_price > 0:
                gap = abs(auction_price - cached_price) / cached_price
                if gap > self.PRICE_GAP_THRESHOLD:
                    issues.append(self._issue(
                        SEV_ERROR, 'price_gap', auth_code, auth['name'],
                        f"竞价价 {auction_price} 与缓存价 {cached_price} "
                        f"跳空 {gap:+.1%}，疑似缓存串号",
                        detail={'cached_price': cached_price,
                                'auction_price': auction_price,
                                'gap_pct': round(gap, 4)}))

        report = self._finalize_report('auction', started, checked,
                                       len(authoritative), issues)
        self._repair_auction(issues, authoritative)
        self._save_report(report)
        return report

    # ------------------------------------------------------------------ 修复

    def _repair_static(self, issues: List[Dict], authoritative: Dict):
        """静态校验的自动修复：市场标记错误 → 清理脏缓存键"""
        for issue in issues:
            if issue['type'] != 'market_mismatch':
                continue
            wrong = issue['code']
            correct = (issue.get('detail') or {}).get('correct_code')
            if not correct or self.datacenter is None:
                continue
            # 1) 删除错误键的缓存（若存在）
            if self.datacenter.cache.exists(f'quote:{wrong}'):
                self.datacenter.cache.delete(f'quote:{wrong}')
                logger.warning(f"CodeGuard repair: deleted stale cache "
                               f"quote:{wrong}")
            # 2) 用权威数据写正确键
            auth = authoritative.get(correct)
            if auth:
                self.datacenter.cache.set(f'quote:{correct}', {
                    'code': correct,
                    'name': auth['name'],
                    'price': auth['price'],
                    'repaired_by': 'code_guard',
                    'repaired_at': datetime.now().isoformat(),
                }, ttl=300)
                logger.warning(f"CodeGuard repair: wrote correct cache "
                               f"quote:{correct}")
            issue['repaired'] = True

    def _repair_auction(self, issues: List[Dict], authoritative: Dict):
        """竞价校验的自动修复：价格跳空 → 清缓存等下个周期刷新"""
        for issue in issues:
            if issue['type'] != 'price_gap':
                continue
            code = issue['code']
            if self.datacenter is None:
                continue
            if self.datacenter.cache.exists(f'quote:{code}'):
                self.datacenter.cache.delete(f'quote:{code}')
                logger.warning(f"CodeGuard repair: cleared suspicious cache "
                               f"quote:{code} (price gap)")
            issue['repaired'] = True

    # ------------------------------------------------------------------ 名称校验

    def _check_name(self, code: str, local_name: str,
                    auth_name: str) -> Optional[Dict]:
        """名称一致性：归一化（剥离 ST 等前缀/空格）后比较

        ETF 场景本地多为简称（"沪深300ETF" vs "沪深300ETF华泰柏瑞"），
        互为子串视为一致；只有彻底不同才判定改名/ST 状态变化。
        """
        ln = self._normalize_name(local_name)
        an = self._normalize_name(auth_name)
        if not ln or not an:
            return None
        if ln == an or ln in an or an in ln:
            return None
        return self._issue(
            SEV_WARNING, 'name_mismatch', code, local_name,
            f"名称不一致: 本地[{local_name}] vs 权威[{auth_name}]"
            f"（可能改名/ST，权威名已记录）",
            detail={'auth_name': auth_name})

    @classmethod
    def _normalize_name(cls, name: str) -> str:
        """名称归一化：去空白、剥离 ST/*ST/N/XD 等前后缀"""
        n = str(name or '').strip().replace(' ', '')
        changed = True
        while changed and n:
            changed = False
            for prefix in cls.NAME_IGNORE_PREFIXES:
                if n.upper().startswith(prefix) and len(n) > len(prefix):
                    n = n[len(prefix):]
                    changed = True
        return n

    # ------------------------------------------------------------------ 报告与推送

    def check_and_push(self, notifier, mode: str = 'static') -> Optional[Dict]:
        """校验 + 推送一体化（worker 调用入口）

        Args:
            notifier: NotificationManager（需有 send_custom_message）
            mode: 'static' | 'auction'

        Returns:
            报告 dict；无目标或全绿时返回 None（不推送）
        """
        report = (self.check_static() if mode == 'static'
                  else self.check_auction())
        if not report or report['issue_count'] == 0:
            logger.info(f"CodeGuard {mode}: all {report.get('checked', 0) if report else 0} "
                        f"targets OK")
            return None
        title, msg = self.format_report_message(report)
        try:
            notifier.send_custom_message(title, msg)
            logger.warning(
                f"CodeGuard {mode}: {report['issue_count']} issues "
                f"(error={report['error_count']} warning={report['warning_count']}), pushed")
        except Exception as e:  # noqa: BLE001 外部调用
            logger.error(f"CodeGuard push error: {e}")
        return report

    @staticmethod
    def _issue(severity: str, issue_type: str, code: str, name: str,
               message: str, detail: Optional[Dict] = None) -> Dict:
        return {
            'severity': severity,
            'type': issue_type,
            'code': code,
            'name': name,
            'message': message,
            'detail': detail or {},
        }

    @staticmethod
    def _finalize_report(mode: str, started: datetime, checked: int,
                         quoted: int, issues: List[Dict]) -> Dict:
        error_count = sum(1 for i in issues if i['severity'] == SEV_ERROR)
        warning_count = sum(1 for i in issues if i['severity'] == SEV_WARNING)
        by_type: Dict[str, int] = {}
        for i in issues:
            by_type[i['type']] = by_type.get(i['type'], 0) + 1
        return {
            'mode': mode,
            'checked': checked,
            'quoted': quoted,
            'issues': issues,
            'issue_count': len(issues),
            'error_count': error_count,
            'warning_count': warning_count,
            'by_type': by_type,
            'started_at': started.isoformat(),
            'duration_sec': round((datetime.now() - started).total_seconds(), 2),
        }

    def _save_report(self, report: Dict):
        if self.datacenter is None:
            return
        try:
            self.datacenter.cache.set(self.KEY_REPORT, report, ttl=self.TTL_REPORT)
            # 权威名映射沉淀（供下游展示选用）
            if report.get('issues'):
                names = self.datacenter.cache.get(self.KEY_NAMES) or {}
                for issue in report['issues']:
                    auth_name = (issue.get('detail') or {}).get('auth_name')
                    if auth_name:
                        names[issue['code']] = auth_name
                if names:
                    self.datacenter.cache.set(self.KEY_NAMES, names,
                                              ttl=self.TTL_NAMES)
        except Exception as e:  # noqa: BLE001 缓存IO
            logger.debug(f"CodeGuard save report error: {e}")

    def get_report(self) -> Optional[Dict]:
        """最近一次校验报告（API 用）"""
        if self.datacenter is None:
            return None
        try:
            return self.datacenter.cache.get(self.KEY_REPORT)
        except Exception as e:  # noqa: BLE001 缓存IO
            logger.debug(f"CodeGuard get report error: {e}")
            return None

    # ------------------------------------------------------------------ 文案

    _TYPE_LABELS = {
        'invalid_format': '代码格式非法',
        'missing': '行情源查无此码',
        'market_mismatch': '市场标记错判',
        'name_mismatch': '名称变更',
        'bad_price': '价格异常',
        'price_gap': '缓存价格跳空',
    }

    @classmethod
    def format_report_message(cls, report: Dict) -> Tuple[str, str]:
        """生成钉钉 markdown 推送文案（分级展示）"""
        mode_label = '盘前校验' if report['mode'] == 'static' else '集合竞价校验'
        title = f"🛡️ 代码一致性{mode_label}: {report['issue_count']}个问题"
        lines = [
            f"**{mode_label}报告**",
            f"校验目标 {report['checked']} 只，命中行情 {report['quoted']} 只，"
            f"耗时 {report['duration_sec']}s",
            "",
        ]

        for severity, emoji in ((SEV_ERROR, '🔴'), (SEV_WARNING, '🟡')):
            items = [i for i in report['issues'] if i['severity'] == severity]
            if not items:
                continue
            sev_label = '严重' if severity == SEV_ERROR else '警告'
            lines.append(f"**{emoji} {sev_label}**（{len(items)}）")
            for issue in items[:15]:
                label = cls._TYPE_LABELS.get(issue['type'], issue['type'])
                mark = ' [已自动修复]' if issue.get('repaired') else ''
                lines.append(f"- {label}{mark}: {issue['message']}")
            if len(items) > 15:
                lines.append(f"- ...等共{len(items)}条")
            lines.append("")

        lines.append("修复策略：市场标记错判自动清理脏缓存并写正确键；"
                     "名称变更以权威行情为准；查无代码请人工确认是否退市。")
        return title, "\n".join(lines)

    # ------------------------------------------------------------------ 内部

    def _get_cached_quote(self, code: str) -> Optional[Dict]:
        if self.datacenter is None:
            return None
        try:
            return self.datacenter.cache.get(f'quote:{code}')
        except Exception as e:  # noqa: BLE001 缓存IO
            logger.debug(f"CodeGuard cached quote error [{code}]: {e}")
            return None

    @staticmethod
    def _to_float(v) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0
