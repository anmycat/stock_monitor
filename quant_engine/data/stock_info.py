"""股票静态信息表（D49，2026-10-27）——代码/名称/板块/ST 状态本地缓存

## 背景
用户提议：ECS 上保存一份"代码-股票名"对应表，定期校验（ST/脱帽/
新增/退市），减少对行情服务的请求、提升计算效率。

## 实现要点
- 存储：<仓库根>/data/stock_info.json（.gitignore 已排除 *.db，本文件
  单独 gitignore——各环境自行生成，不进版本库）
- 数据源：复用 SinaSource.get_all_stocks()（M2' 主路，实测 5575 只
  全覆盖）；只依赖其静态列 code/name，不引入新外部依赖
- 校验：refresh() 拉取最新全市场列表与本地表 diff，产出
  added/removed/st_on（戴帽）/st_off（脱帽）/renamed 五类变更；
  变更可经通知渠道推送（ST/脱帽/退市对实盘有直接影响）
- 减请求：get_stock_list 缓存未命中时，静态表 fresh（默认 7 天内）
  直接本地构建，不打行情源；同时一天最多触发一次后台 refresh
  检查（频率闸 STOCK_INFO_REFRESH_MIN_INTERVAL）
- 线程安全：模块级单例 + 简单互斥锁（写路径串行）
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pandas as pd
from loguru import logger

# ---- 板块识别（code 前缀 → 板块）----
# 与 market_sanity.py 的涨跌幅段表、burst_monitor 池过滤口径一致
_BOARD_PREFIXES: list[tuple[str, str]] = [
    ('688', 'SH_STAR'),      # 科创板
    ('689', 'SH_STAR_CDR'),  # 科创板 CDR
    ('600', 'SH_MAIN'),
    ('601', 'SH_MAIN'),
    ('603', 'SH_MAIN'),
    ('605', 'SH_MAIN'),
    ('900', 'SH_B'),         # 沪 B 股
    ('000', 'SZ_MAIN'),
    ('001', 'SZ_MAIN'),
    ('002', 'SZ_SME'),       # 中小板（已并入主板，保留号段识别）
    ('003', 'SZ_MAIN'),
    ('300', 'SZ_GEM'),       # 创业板
    ('301', 'SZ_GEM'),
    ('302', 'SZ_GEM'),
    ('200', 'SZ_B'),         # 深 B 股
    ('8', 'BJ'),             # 北交所
    ('4', 'BJ'),
    ('920', 'BJ'),
    ('5', 'FUND'),           # 上交所基金
    ('15', 'FUND'),          # 深交所基金（ETF 等）
    ('16', 'FUND'),
    ('18', 'FUND'),
]


def detect_board(code: str) -> str:
    """由代码推断板块（8/4/920 → BJ；688 → 科创；300/301 → 创业；…）"""
    c = str(code).split('.')[0]
    for prefix, board in _BOARD_PREFIXES:
        if c.startswith(prefix):
            return board
    return 'UNKNOWN'


def is_st_name(name: str) -> bool:
    """名称 ST 判定：*ST / ST / S*ST / S ST 前缀均为风险警示股"""
    n = str(name).strip().upper().replace(' ', '')
    return n.startswith(('*ST', 'S*ST', 'ST', 'SST'))


# ingest 最小行数门槛：低于此值视为兜底小表（source.py 兜底 5 只），拒绝覆盖
MIN_INGEST_ROWS = 1000


class StockInfoTable:
    """股票静态信息表：本地 JSON 单例 + 定期刷新 + 变更 diff"""

    def __init__(self, path: str | None = None):
        raw = path or 'data/stock_info.json'
        p = Path(raw)
        if not p.is_absolute():
            # 相对路径按仓库根解析（quant_engine/data/stock_info.py → parents[2]）
            p = Path(__file__).resolve().parents[2] / raw
        self.path = p
        self._lock = threading.Lock()
        self._stocks: dict[str, dict] = {}   # code → {name, board, is_st}
        self._updated_at = 0.0
        self._last_refresh_attempt = 0.0
        self._load()

    # ---- 持久化 ----
    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            data = json.loads(self.path.read_text(encoding='utf-8'))
            self._stocks = data.get('stocks', {})
            self._updated_at = float(data.get('updated_at', 0) or 0)
        except Exception as e:  # noqa: BLE001 兜底：坏表按空处理，不影响主链
            logger.warning(f"[StockInfo] 读取 {self.path} 失败（按空表处理）: {e}")
            self._stocks = {}
            self._updated_at = 0.0

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(
            {'version': 1, 'updated_at': self._updated_at, 'stocks': self._stocks},
            ensure_ascii=False), encoding='utf-8')
        tmp.replace(self.path)  # 原子替换，避免半文件

    # ---- 刷新与 diff ----
    def ingest(self, df: pd.DataFrame) -> dict | None:
        """用外部已拉取的全市场列表（含 code/name 列）更新静态表。

        与 refresh() 的区别：不再发起网络请求，直接消费调用方已有的
        DataFrame（CompositeSource.get_stock_list 的返回值）——避免
        重复拉取。小表（< MIN_INGEST_ROWS，如 5 只兜底）拒绝入库，
        防止兜底数据覆盖真实静态表。
        """
        if df is None or df.empty or 'code' not in df.columns:
            return None
        if len(df) < MIN_INGEST_ROWS:
            logger.debug(f"[StockInfo] 跳过 ingest：{len(df)} 行 < {MIN_INGEST_ROWS}（疑似兜底小表）")
            return None

        fresh: dict[str, dict] = {}
        for _, row in df.iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', '')).strip()
            if not code or not name:
                continue
            fresh[code] = {
                'name': name,
                'board': detect_board(code),
                'is_st': is_st_name(name),
            }
        if len(fresh) < MIN_INGEST_ROWS:
            return None
        return self._apply(fresh)

    def refresh(self, min_interval: float = 6 * 3600) -> dict:
        """主动拉取最新全市场列表并 diff（盘后定时任务用）。

        频率闸：距上次成功刷新不足 min_interval 秒则跳过（返回空 diff）。
        返回 {'added': [...], 'removed': [...], 'st_on': [...], 'st_off': [...],
              'renamed': [...], 'total': int, 'skipped': bool}
        """
        with self._lock:
            if self._updated_at and time.time() - self._updated_at < min_interval:
                return {'skipped': True, 'total': len(self._stocks)}

        try:
            from .sources.sina import SinaSource
            df = SinaSource.get_all_stocks()
        except Exception as e:  # noqa: BLE001 兜底：拉数失败保留旧表
            logger.warning(f"[StockInfo] 拉取全市场列表失败: {e}")
            return {'skipped': False, 'error': str(e), 'total': len(self._stocks)}

        if df is None or df.empty or 'code' not in df.columns:
            logger.warning("[StockInfo] 拉取全市场列表为空，保留旧表")
            return {'skipped': False, 'empty': True, 'total': len(self._stocks)}

        fresh: dict[str, dict] = {}
        for _, row in df.iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', '')).strip()
            if not code or not name:
                continue
            fresh[code] = {
                'name': name,
                'board': detect_board(code),
                'is_st': is_st_name(name),
            }
        if len(fresh) < MIN_INGEST_ROWS:
            logger.warning(f"[StockInfo] 拉取结果仅 {len(fresh)} 行，拒绝覆盖旧表")
            return {'skipped': False, 'empty': True, 'total': len(self._stocks)}

        diff = self._apply(fresh)
        n_change = sum(len(diff[k]) for k in ('added', 'removed', 'st_on', 'st_off', 'renamed'))
        logger.info(f"[StockInfo] 刷新完成: {diff['total']} 只, 变更 {n_change} 项 "
                    f"(新增{len(diff['added'])}/退市{len(diff['removed'])}/"
                    f"戴帽{len(diff['st_on'])}/脱帽{len(diff['st_off'])}/更名{len(diff['renamed'])})")
        return diff

    def _apply(self, fresh: dict[str, dict]) -> dict:
        """diff + 落盘（调用方持构建好的 fresh 字典）"""
        with self._lock:
            old = self._stocks
            diff = {
                'added': sorted(set(fresh) - set(old)),
                'removed': sorted(set(old) - set(fresh)),
                'st_on': sorted(c for c in set(fresh) & set(old)
                                if fresh[c]['is_st'] and not old[c]['is_st']),
                'st_off': sorted(c for c in set(fresh) & set(old)
                                 if not fresh[c]['is_st'] and old[c]['is_st']),
                'renamed': sorted(c for c in set(fresh) & set(old)
                                  if fresh[c]['name'] != old[c]['name']),
                'total': len(fresh),
                'skipped': False,
            }
            self._stocks = fresh
            self._updated_at = time.time()
            self._save()
        return diff

    # ---- 查询 ----
    def is_fresh(self, max_age_days: float = 7.0) -> bool:
        """静态表是否足够新（get_stock_list 免打源判据）"""
        if not self._stocks:
            return False
        return (time.time() - self._updated_at) < max_age_days * 86400

    def get_name(self, code: str) -> str | None:
        rec = self._stocks.get(code)
        return rec['name'] if rec else None

    def is_st(self, code: str) -> bool:
        rec = self._stocks.get(code)
        return bool(rec and rec['is_st'])

    def get_board(self, code: str) -> str | None:
        rec = self._stocks.get(code)
        return rec['board'] if rec else None

    def __contains__(self, code: object) -> bool:
        return code in self._stocks

    def __len__(self) -> int:
        return len(self._stocks)

    @property
    def updated_at(self) -> float:
        return self._updated_at

    def to_dataframe(self, exclude_st: bool = False,
                     exclude_bj: bool = True) -> pd.DataFrame:
        """输出 code/name/industry 三列（与 get_stock_list 契约一致）。

        exclude_bj 默认 True：扫描池排除北交所（D44：TDX 不支持 BJ），
        与 MarketDataRepository._filter_markets 口径一致。
        """
        rows = []
        for code, rec in self._stocks.items():
            if exclude_st and rec['is_st']:
                continue
            if exclude_bj and rec['board'] in ('BJ',):
                continue
            rows.append({'code': code, 'name': rec['name'], 'industry': ''})
        return pd.DataFrame(rows, columns=['code', 'name', 'industry'])


# ---- 模块级单例 ----
_table: StockInfoTable | None = None
_table_lock = threading.Lock()


def get_stock_info_table() -> StockInfoTable:
    global _table
    if _table is None:
        with _table_lock:
            if _table is None:
                _table = StockInfoTable()
    return _table


def format_stock_info_diff(diff: dict) -> str:
    """diff → 钉钉可读摘要（仅在有变更时调用）"""
    if not diff or diff.get('skipped'):
        return ''
    parts = []
    for key, label in (('st_on', '戴帽ST'), ('st_off', '脱帽'),
                       ('added', '新增'), ('removed', '退市/暂停'),
                       ('renamed', '更名')):
        items = diff.get(key) or []
        if items:
            tbl = get_stock_info_table()
            names = [f"{c}({tbl.get_name(c) or '?'})" for c in items[:5]]
            more = f" 等{len(items)}只" if len(items) > 5 else ''
            parts.append(f"{label}: {'、'.join(names)}{more}")
    return '\n'.join(parts)
