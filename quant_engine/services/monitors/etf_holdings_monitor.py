"""ETF持仓成分股监控模块

对热门ETF的季度持仓（股票投资明细）做快照管理，按季度环比diff：
- 数量：持股数（万股）环比变化（相对+绝对）
- 方向：新进 / 退出 / 增持 / 减持 / 持平
- 价格：持仓市值（万元）与权重（占净值比例）变化，可叠加成分股实时价

数据来源：天天基金F10持仓接口（见 data.sources.fund_holdings），季度更新。
ETF总份额申赎会导致全部成分股持股数同比例缩放而权重不变，
因此告警判定以权重变化为主、持股数变化需伴随权重同向变动，剔除被动缩放噪声。
"""
import time
from datetime import datetime
from typing import Dict, List, Optional

from loguru import logger

from ...data.repositories import MarketDataRepository
from ...data.sources.fund_holdings import EastMoneyFundHoldingsSource
from .etf_monitor import HOT_ETFS


class ETFHoldingsMonitor:
    """ETF持仓成分股监控"""

    # 告警阈值（类属性，便于测试覆盖）
    SHARE_CHANGE_THRESHOLD = 0.10    # 持股数相对变化 ≥10%
    WEIGHT_CHANGE_THRESHOLD = 0.30   # 权重变化 ≥0.3个百分点
    MIN_WEIGHT_DELTA_FOR_SHARE = 0.10  # 持股数变化需伴随的最小权重变化(pct)
    MIN_MARKET_VALUE = 1000.0        # 持仓市值门槛（万元），过滤微型持仓噪声
    TOPLINE = 100                    # 每季跟踪前N大持仓

    # Redis键与TTL
    KEY_SNAPSHOT = "etf_holdings:{code}:{quarter}"     # 单季持仓快照（天）
    KEY_LATEST = "etf_holdings:latest:{code}"          # 最新季摘要
    KEY_CHANGES = "etf_holdings:changes:{code}"        # 最近一次环比变化
    KEY_CHECK_DATE = "etf_holdings:check_date"         # 每日检查去重
    TTL_SNAPSHOT = 400 * 86400
    TTL_LATEST = 7 * 86400
    TTL_CHANGES = 120 * 86400
    TTL_CHECK_DATE = 2 * 86400

    def __init__(self, datacenter: Optional[MarketDataRepository] = None):
        self.datacenter = datacenter
        self.source = EastMoneyFundHoldingsSource()
        self.target_etfs = HOT_ETFS

    # ------------------------------------------------------------------ 公共

    def refresh_etf(self, etf_code: str, etf_name: str = "") -> Optional[Dict]:
        """抓取单只ETF持仓，发现新季度则与上一季diff

        Returns:
            新季度的变化字典（含diff结果）；无新季度返回None；抓取失败返回None
        """
        code = etf_code.split('.')[0]
        try:
            df = self.source.get_holdings(code, topline=self.TOPLINE)
        except Exception as e:  # noqa: BLE001 外部调用
            logger.warning(f"ETF holdings fetch failed {code}: {e}")
            return None
        if df.empty:
            logger.info(f"ETF holdings empty {code}")
            return None

        quarters = self._sorted_quarters(df['quarter'].unique())
        latest_q = quarters[-1]
        latest_records = self._records_of(df, latest_q)

        # 已处理过该季度 -> 跳过（每日重复调用幂等）
        if self._latest_quarter(code) == latest_q:
            return None

        # 存储该季快照
        self._set(self.KEY_SNAPSHOT.format(code=code, quarter=latest_q),
                  latest_records, self.TTL_SNAPSHOT)

        prev_q = quarters[-2] if len(quarters) >= 2 else None
        changes = None
        if prev_q:
            prev_records = self._records_of(df, prev_q)
            self._set(self.KEY_SNAPSHOT.format(code=code, quarter=prev_q),
                      prev_records, self.TTL_SNAPSHOT)
            changes = self.diff_quarters(prev_q, latest_q, prev_records,
                                         latest_records)
            changes['etf_code'] = etf_code
            changes['etf_name'] = etf_name or code
            changes['generated_at'] = datetime.now().isoformat()
            changes['alert_count'] = self.filter_alertable(changes)['alert_count']
            self._set(self.KEY_CHANGES.format(code=code), changes, self.TTL_CHANGES)

        self._set(self.KEY_LATEST.format(code=code), {
            'etf_code': etf_code,
            'etf_name': etf_name or code,
            'quarter': latest_q,
            'report_date': str(df[df['quarter'] == latest_q]['report_date'].iloc[0]),
            'updated_at': datetime.now().isoformat(),
            'holdings_count': len(latest_records),
            'top': latest_records[:10],
        }, self.TTL_LATEST)

        if changes:
            logger.info(
                f"ETF holdings new quarter {code} {latest_q}: "
                f"new={len(changes['new_entries'])} exit={len(changes['exits'])} "
                f"up={len(changes['increased'])} down={len(changes['decreased'])}"
            )
        else:
            logger.info(f"ETF holdings snapshot saved {code} {latest_q} (no previous quarter)")
        return changes

    def check_all(self) -> List[Dict]:
        """遍历全部热门ETF，返回需要推送的变化列表（已过阈值过滤）"""
        push_list = []
        for etf in self.target_etfs:
            try:
                changes = self.refresh_etf(etf['code'], etf['name'])
            except Exception as e:  # noqa: BLE001 单项跳过
                logger.warning(f"ETF holdings check failed {etf['code']}: {e}")
                continue
            if not changes:
                continue
            alertable = self.filter_alertable(changes)
            if alertable['alert_count'] > 0:
                push_list.append(alertable)
            time.sleep(0.3)  # ETF间限流
        return push_list

    @staticmethod
    def diff_quarters(prev_q: str, curr_q: str,
                      prev_records: List[Dict],
                      curr_records: List[Dict]) -> Dict:
        """两季持仓环比diff（全量，阈值过滤在filter_alertable）"""
        prev_map = {r['code']: r for r in prev_records}
        curr_map = {r['code']: r for r in curr_records}

        new_entries, exits, increased, decreased = [], [], [], []
        unchanged = 0

        for code, cur in curr_map.items():
            prev = prev_map.get(code)
            if prev is None:
                new_entries.append(ETFHoldingsMonitor._item(code, cur, direction='new'))
                continue
            shares_chg = ETFHoldingsMonitor._rel_change(prev['shares'], cur['shares'])
            weight_delta = round(cur['weight'] - prev['weight'], 4)
            if shares_chg >= ETFHoldingsMonitor.SHARE_CHANGE_THRESHOLD:
                increased.append(ETFHoldingsMonitor._item(code, cur, prev=prev, direction='up',
                                                         shares_chg=shares_chg, weight_delta=weight_delta))
            elif shares_chg <= -ETFHoldingsMonitor.SHARE_CHANGE_THRESHOLD:
                decreased.append(ETFHoldingsMonitor._item(code, cur, prev=prev, direction='down',
                                                         shares_chg=shares_chg, weight_delta=weight_delta))
            else:
                unchanged += 1

        for code, prev in prev_map.items():
            if code not in curr_map:
                exits.append(ETFHoldingsMonitor._item(code, prev, direction='exit'))

        # 各列表按权重降序，重要的排前面
        for lst in (new_entries, exits, increased, decreased):
            lst.sort(key=lambda x: -x['weight'])

        # 基金规模（净值）合理性检查：由 持仓市值/权重 反推总净值。
        # ETF份额申赎会使全部持股数同比例缩放而权重不变；若两季反推净值
        # 跳变超过50%，视为数据源异常（实测2026Q1部分ETF存在此现象），
        # 记录 nav_change_pct 供人工判断，权重类告警仍然有效。
        nav_prev = ETFHoldingsMonitor._implied_nav(prev_records)
        nav_curr = ETFHoldingsMonitor._implied_nav(curr_records)
        nav_change = ((nav_curr - nav_prev) / nav_prev) if nav_prev else None
        if nav_change is not None and abs(nav_change) > 0.5:
            logger.warning(
                f"ETF holdings NAV anomaly {curr_q} vs {prev_q}: "
                f"{nav_change:+.1%} (implied {nav_prev:.0f}万 -> {nav_curr:.0f}万), "
                f"持股数变化可能含源端数据问题"
            )

        return {
            'previous_quarter': prev_q,
            'current_quarter': curr_q,
            'current_report_date': curr_records[0].get('report_date', '') if curr_records else '',
            'previous_report_date': prev_records[0].get('report_date', '') if prev_records else '',
            'new_entries': new_entries,
            'exits': exits,
            'increased': increased,
            'decreased': decreased,
            'unchanged_count': unchanged,
            'nav_change_pct': round(nav_change, 4) if nav_change is not None else None,
        }

    def filter_alertable(self, changes: Dict) -> Dict:
        """按阈值过滤出值得推送的变化，返回带alert_count的副本"""
        def keep(item: Dict) -> bool:
            d = item['direction']
            if d in ('new', 'exit'):
                return item['market_value'] >= self.MIN_MARKET_VALUE
            if abs(item.get('weight_delta') or 0) >= self.WEIGHT_CHANGE_THRESHOLD:
                return True
            # 持股数显著变化且权重同向变动（剔除ETF份额申赎导致的整体缩放）
            return (abs(item.get('shares_change_pct') or 0) >= self.SHARE_CHANGE_THRESHOLD
                    and abs(item.get('weight_delta') or 0) >= self.MIN_WEIGHT_DELTA_FOR_SHARE)

        result = dict(changes)
        for key in ('new_entries', 'exits', 'increased', 'decreased'):
            result[key] = [i for i in changes.get(key, []) if keep(i)]
        result['alert_count'] = sum(len(result[k]) for k in
                                    ('new_entries', 'exits', 'increased', 'decreased'))
        return result

    def format_changes_message(self, changes: Dict) -> str:
        """生成钉钉/企微等markdown推送文案"""
        lines = [
            f"**{changes['previous_quarter']} → {changes['current_quarter']}**"
            f"（截止 {changes.get('current_report_date', '')}）",
            "",
        ]

        nav_chg = changes.get('nav_change_pct')
        if nav_chg is not None and abs(nav_chg) > 0.5:
            lines.append(f"⚠️ 两季反推基金规模变化 {nav_chg:+.1%}，持股数变化或含数据源异常，"
                         f"请结合权重判断")
            lines.append("")

        def fmt(item: Dict) -> str:
            parts = [f"{item['name']}({item['code'].split('.')[0]})"]
            if item['direction'] == 'new':
                parts.append(f"权重{item['weight']:.2f}%")
            elif item['direction'] == 'exit':
                parts.append(f"原权重{item['prev_weight']:.2f}%")
            else:
                sign = '+' if (item.get('shares_change_pct') or 0) >= 0 else ''
                parts.append(f"持股{sign}{(item.get('shares_change_pct') or 0):.1f}%")
                wd = item.get('weight_delta') or 0
                if abs(wd) >= 0.01:
                    parts.append(f"权重{'+' if wd >= 0 else ''}{wd:.2f}pct")
            parts.append(f"市值{item['market_value'] / 10000:.2f}亿")
            return ' '.join(parts)

        sections = [
            ('new_entries', '🆕 新进'),
            ('exits', '🚪 退出'),
            ('increased', '🔴 增持'),
            ('decreased', '🟢 减持'),
        ]
        for key, label in sections:
            items = changes.get(key, [])
            if not items:
                continue
            lines.append(f"**{label}** ({len(items)}只)")
            for item in items[:10]:
                lines.append(f"- {fmt(item)}")
            if len(items) > 10:
                lines.append(f"- ...等共{len(items)}只")
            lines.append("")

        lines.append(f"持平/未达阈值: {changes.get('unchanged_count', 0)}只")
        return "\n".join(lines)

    # ------------------------------------------------------------------ API辅助

    def get_latest(self, etf_code: str) -> Optional[Dict]:
        return self._get(self.KEY_LATEST.format(code=etf_code.split('.')[0]))

    def get_changes(self, etf_code: str) -> Optional[Dict]:
        return self._get(self.KEY_CHANGES.format(code=etf_code.split('.')[0]))

    def get_overview(self) -> List[Dict]:
        """全部监控ETF的最新持仓摘要（供API）"""
        overview = []
        for etf in self.target_etfs:
            latest = self.get_latest(etf['code'])
            overview.append({
                'code': etf['code'],
                'name': etf['name'],
                'category': etf.get('category', ''),
                'quarter': latest.get('quarter') if latest else None,
                'report_date': latest.get('report_date') if latest else None,
                'holdings_count': latest.get('holdings_count', 0) if latest else 0,
                'top': latest.get('top', [])[:5] if latest else [],
                'tracked': latest is not None,
            })
        return overview

    # ------------------------------------------------------------------ 内部

    def _latest_quarter(self, code: str) -> Optional[str]:
        latest = self._get(self.KEY_LATEST.format(code=code))
        return latest.get('quarter') if latest else None

    @staticmethod
    def _sorted_quarters(quarters) -> List[str]:
        def key(q: str):
            year, qn = q.split('Q')
            return (int(year), int(qn))
        return sorted(quarters, key=key)

    @staticmethod
    def _records_of(df, quarter: str) -> List[Dict]:
        sub = df[df['quarter'] == quarter].sort_values('seq')
        return [
            {
                'code': row['code'],
                'name': row['name'],
                'weight': float(row['weight']),
                'shares': float(row['shares']),
                'market_value': float(row['market_value']),
                'report_date': str(row['report_date']),
                'seq': int(row['seq']),
            }
            for _, row in sub.iterrows()
        ]

    @staticmethod
    def _item(code: str, rec: Dict, prev: Optional[Dict] = None,
              direction: Optional[str] = None,
              shares_chg: Optional[float] = None,
              weight_delta: Optional[float] = None) -> Dict:
        return {
            'code': code,
            'name': rec['name'],
            'direction': direction,
            'weight': round(float(rec['weight']), 4),
            'prev_weight': round(float(prev['weight']), 4) if prev else 0.0,
            'shares': round(float(rec['shares']), 2),
            'prev_shares': round(float(prev['shares']), 2) if prev else 0.0,
            'shares_change_pct': round(shares_chg, 4) if shares_chg is not None else None,
            'weight_delta': round(weight_delta, 4) if weight_delta is not None else None,
            'market_value': round(float(rec['market_value']), 2),
        }

    @staticmethod
    def _rel_change(prev: float, curr: float) -> float:
        if not prev:
            return 1.0 if curr else 0.0
        return (curr - prev) / prev

    @staticmethod
    def _implied_nav(records: List[Dict]) -> float:
        """由 持仓市值/权重 反推基金总净值（万元）；权重和为0时返回0"""
        mv_sum = sum(r['market_value'] for r in records)
        w_sum = sum(r['weight'] for r in records)
        if not w_sum:
            return 0.0
        return mv_sum / w_sum * 100

    def _get(self, key: str, default=None):
        if self.datacenter is None:
            return default
        try:
            return self.datacenter.cache.get(key)
        except Exception as e:  # noqa: BLE001 缓存IO
            logger.debug(f"ETF holdings cache get error [{key}]: {e}")
            return default

    def _set(self, key: str, value, ttl: int) -> bool:
        if self.datacenter is None:
            return False
        try:
            return self.datacenter.cache.set(key, value, ttl=ttl)
        except Exception as e:  # noqa: BLE001 缓存IO
            logger.debug(f"ETF holdings cache set error [{key}]: {e}")
            return False
