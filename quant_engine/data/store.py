"""SQLite 持久层（2026-10-25，research/docs/25 §5.1 首位建议）

修复「信号只在 Redis 活 15 分钟、历史靠 JSON 散落」的审计缺口：
- signals：BUY/WATCH 信号归档（同日同代码同策略 upsert，盘中多轮只留最新观测）
- eval_runs / eval_results：T+N 后验评估运行快照与评估结果（Redis 30 天
  TTL 之外的耐久层，Redis 未命中时回退）
- summaries：AI 总结归档（Redis 24h TTL 之外的耐久层）

设计约束（docs/25 §4/§5）：
- 同步 sqlite3（写入量 KB/天，无需 aiosqlite）；WAL 模式支持并发读
- 单文件零运维，默认 <仓库根>/data/history.db（.gitignore 已排除 *.db）
- **全部公开方法异常自吞**返回安全默认值——持久层故障绝不影响信号推送
  主流程（Redis 仍是实时层，SQLite 只是审计归档）
- 只依赖标准库 + loguru，不 import models/services（data 叶族依赖宪法）
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger

from ..config import get_settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_date TEXT NOT NULL,           -- 信号生成日 YYYY-MM-DD
    created_at TEXT NOT NULL,            -- ISO8601 生成时刻
    code TEXT NOT NULL,
    name TEXT,
    action TEXT NOT NULL,                -- BUY/WATCH（SKIP 不落库）
    level TEXT,                          -- S/A/B
    score REAL,
    strategy TEXT,
    price REAL,
    position REAL,
    target_price REAL,
    stop_loss REAL,
    capital_flow_5d REAL,                -- 近5日主力净流入（元），NULL=降级未取到
    kelly_position REAL,
    kelly_confidence REAL,
    kelly_source TEXT,
    market_type TEXT,                    -- 生成时市场状态
    reason TEXT,                         -- JSON 数组
    UNIQUE(signal_date, code, strategy)  -- 同日同策略幂等 upsert
);
CREATE INDEX IF NOT EXISTS idx_signals_code_date ON signals(code, signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(signal_date DESC);

CREATE TABLE IF NOT EXISTS eval_runs (
    run_id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    market_type TEXT,
    market_score REAL,
    total_signals INTEGER DEFAULT 0,
    signals_json TEXT NOT NULL           -- 信号快照（T 日全生命周期可追溯）
);

CREATE TABLE IF NOT EXISTS eval_results (
    run_id TEXT PRIMARY KEY,
    evaluated_at TEXT NOT NULL,
    total_signals INTEGER DEFAULT 0,
    win_count INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0,
    avg_pnl REAL DEFAULT 0,
    avg_max_profit REAL DEFAULT 0,
    avg_max_drawdown REAL DEFAULT 0,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    summary_type TEXT NOT NULL,          -- call_auction/morning_review/daily_review
    generated_at TEXT NOT NULL,
    market TEXT,
    model TEXT,
    signals_count INTEGER DEFAULT 0,
    sectors_count INTEGER DEFAULT 0,
    full_text TEXT,
    data_json TEXT                       -- 全量 summary_data
);
CREATE INDEX IF NOT EXISTS idx_summaries_type_time ON summaries(summary_type, generated_at DESC);
"""

_SIGNAL_COLS = ("signal_date, created_at, code, name, action, level, score, strategy, "
                "price, position, target_price, stop_loss, capital_flow_5d, "
                "kelly_position, kelly_confidence, kelly_source, market_type, reason")


class HistoryStore:
    """SQLite 归档存储（线程本地连接 + WAL，进程级单例见 get_history_store）"""

    def __init__(self, db_path: str | None = None):
        settings = get_settings()
        raw = db_path or settings.SQLITE_DB_PATH
        path = Path(raw)
        if not path.is_absolute():
            # 相对路径按仓库根解析（quant_engine/data/store.py → parents[2]）
            path = Path(__file__).resolve().parents[2] / path
        self.db_path = path
        self._local = threading.local()
        self.enabled = False
        self._init_db()

    # ---- 连接管理 ----
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            conn = sqlite3.connect(str(self.db_path), timeout=10)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA synchronous=NORMAL")
            # 建表幂等：库文件被删后重连时自愈重建 schema
            conn.executescript(_SCHEMA)
            self._local.conn = conn
        return conn

    def _drop_conn(self):
        """丢弃当前线程连接（失效自愈：下次操作重新打开）"""
        conn = getattr(self._local, 'conn', None)
        if conn is not None:
            try:
                conn.close()
            except Exception as e:  # noqa: BLE001 关闭失败仅记录，不影响引用清理
                logger.debug(f"HistoryStore close error: {e}")
        self._local.conn = None

    def _init_db(self):
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = self._conn()
            conn.executescript(_SCHEMA)
            conn.commit()
            self.enabled = True
            logger.info(f"SQLite持久层就绪: {self.db_path}")
        except Exception as e:  # noqa: BLE001 初始化失败 → 整体降级关闭
            self.enabled = False
            logger.error(f"SQLite持久层初始化失败，持久化降级关闭: {e}")
            return
        try:
            self._prune()
        except Exception as e:  # noqa: BLE001 清理失败不影响使用
            logger.warning(f"SQLite归档清理失败: {e}")

    def close(self):
        self._drop_conn()

    def _safe(self, fn, default):
        """统一降级包装：未启用/异常 → 安全默认值（绝不向上抛）"""
        if not self.enabled:
            return default
        try:
            return fn()
        except sqlite3.Error as e:
            self._drop_conn()
            logger.warning(f"SQLite操作失败({type(e).__name__})，本次降级: {e}")
            return default
        except Exception as e:  # noqa: BLE001 序列化等非 sqlite 故障同样降级
            logger.warning(f"SQLite操作失败，本次降级: {e}")
            return default

    # ---- 信号 ----
    def save_signals(self, rows: list[dict]) -> int:
        """信号归档 upsert（同日同代码同策略只留最新观测）

        rows: dict 列表，键见 _SIGNAL_COLS（reason 传 list，内部转 JSON）
        返回写入行数；rows 为空或降级返回 0
        """
        if not rows:
            return 0

        def _write() -> int:
            conn = self._conn()
            n = 0
            for r in rows:
                conn.execute(
                    f"""INSERT INTO signals ({_SIGNAL_COLS})
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(signal_date, code, strategy) DO UPDATE SET
                          created_at=excluded.created_at, name=excluded.name,
                          action=excluded.action, level=excluded.level,
                          score=excluded.score, price=excluded.price,
                          position=excluded.position,
                          target_price=excluded.target_price,
                          stop_loss=excluded.stop_loss,
                          capital_flow_5d=excluded.capital_flow_5d,
                          kelly_position=excluded.kelly_position,
                          kelly_confidence=excluded.kelly_confidence,
                          kelly_source=excluded.kelly_source,
                          market_type=excluded.market_type,
                          reason=excluded.reason""",
                    (
                        r.get('signal_date'), r.get('created_at'), r.get('code'),
                        r.get('name'), r.get('action'), r.get('level'), r.get('score'),
                        r.get('strategy'), r.get('price'), r.get('position'),
                        r.get('target_price'), r.get('stop_loss'),
                        r.get('capital_flow_5d'), r.get('kelly_position'),
                        r.get('kelly_confidence'), r.get('kelly_source'),
                        r.get('market_type'),
                        json.dumps(r.get('reason') or [], ensure_ascii=False),
                    ))
                n += 1
            conn.commit()
            return n

        return self._safe(_write, 0)

    def get_signal_history(self, limit: int = 50, code: str | None = None,
                           action: str | None = None, date_from: str | None = None,
                           date_to: str | None = None) -> list[dict]:
        """信号历史查询（signal_date 倒序；date_from/to 为 YYYY-MM-DD 含边界）"""

        def _read() -> list[dict]:
            where, params = ["1=1"], []
            if code:
                where.append("code=?")
                params.append(code)
            if action:
                where.append("action=?")
                params.append(action)
            if date_from:
                where.append("signal_date>=?")
                params.append(date_from)
            if date_to:
                where.append("signal_date<=?")
                params.append(date_to)
            params.append(int(limit))
            conn = self._conn()
            cur = conn.execute(
                f"SELECT {_SIGNAL_COLS} FROM signals"
                f" WHERE {' AND '.join(where)}"
                f" ORDER BY signal_date DESC, id DESC LIMIT ?", params)
            out = []
            for row in cur.fetchall():
                (signal_date, created_at, code_, name, action_, level, score, strategy,
                 price, position, target, stop, flow, kpos, kconf, ksrc, mtype,
                 reason) = row
                out.append({
                    'signal_date': signal_date, 'created_at': created_at,
                    'code': code_, 'name': name or '', 'action': action_,
                    'level': level, 'score': score, 'strategy': strategy or '',
                    'price': price, 'position': position,
                    'target_price': target, 'stop_loss': stop,
                    'capital_flow_5d': flow, 'kelly_position': kpos,
                    'kelly_confidence': kconf, 'kelly_source': ksrc,
                    'market_type': mtype,
                    'reason': self._loads(reason, []),
                })
            return out

        return self._safe(_read, [])

    # ---- 后验评估 ----
    def save_eval_run(self, run_id: str, timestamp: str, market_type: str | None,
                      market_score: float | None, signals: list[dict]) -> bool:
        """评估运行快照归档（Redis 30 天 TTL 之外的耐久层）"""

        def _write() -> bool:
            conn = self._conn()
            conn.execute(
                """INSERT INTO eval_runs (run_id, timestamp, market_type, market_score,
                                        total_signals, signals_json)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(run_id) DO UPDATE SET
                     timestamp=excluded.timestamp,
                     market_type=excluded.market_type,
                     market_score=excluded.market_score,
                     total_signals=excluded.total_signals,
                     signals_json=excluded.signals_json""",
                (run_id, timestamp, market_type, market_score, len(signals),
                 json.dumps(signals, ensure_ascii=False, default=str)))
            conn.commit()
            return True

        return self._safe(_write, False)

    def get_eval_run(self, run_id: str) -> dict | None:
        """读取运行快照（形状与 Redis run:{id} 一致，供 evaluate_run 回退）"""

        def _read():
            conn = self._conn()
            row = conn.execute(
                "SELECT run_id, timestamp, market_type, market_score, signals_json"
                " FROM eval_runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                return None
            return {
                'run_id': row[0],
                'timestamp': row[1],
                'market_state': {'type': row[2], 'score': row[3]},
                'signals': self._loads(row[4], []),
            }

        return self._safe(_read, None)

    def list_eval_runs(self, limit: int = 20) -> list[dict]:
        """运行列表（timestamp 倒序，形状同 PostEvaluation.get_run_list 元素）"""

        def _read() -> list[dict]:
            conn = self._conn()
            cur = conn.execute(
                "SELECT run_id, timestamp, market_type, total_signals FROM eval_runs"
                " ORDER BY timestamp DESC, run_id DESC LIMIT ?", (int(limit),))
            return [{'run_id': r[0], 'run_date': r[1],
                     'market_state': r[2] or 'unknown', 'total_signals': r[3] or 0}
                    for r in cur.fetchall()]

        return self._safe(_read, [])

    def save_eval_result(self, run_id: str, result: dict) -> bool:
        """评估结果归档（latest-wins：同 run 重复评估只留最新一次）"""

        def _write() -> bool:
            conn = self._conn()
            conn.execute(
                """INSERT INTO eval_results (run_id, evaluated_at, total_signals,
                        win_count, win_rate, avg_pnl, avg_max_profit,
                        avg_max_drawdown, details_json)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(run_id) DO UPDATE SET
                     evaluated_at=excluded.evaluated_at,
                     total_signals=excluded.total_signals,
                     win_count=excluded.win_count, win_rate=excluded.win_rate,
                     avg_pnl=excluded.avg_pnl,
                     avg_max_profit=excluded.avg_max_profit,
                     avg_max_drawdown=excluded.avg_max_drawdown,
                     details_json=excluded.details_json""",
                (run_id, datetime.now().isoformat(),
                 result.get('total_signals', 0), result.get('win_count', 0),
                 result.get('win_rate', 0), result.get('avg_pnl', 0),
                 result.get('avg_max_profit', 0), result.get('avg_max_drawdown', 0),
                 json.dumps(result.get('details', []), ensure_ascii=False, default=str)))
            conn.commit()
            return True

        return self._safe(_write, False)

    def get_eval_result(self, run_id: str) -> dict | None:
        """读取最新评估结果（无归档返回 None）"""

        def _read():
            conn = self._conn()
            row = conn.execute(
                "SELECT run_id, evaluated_at, total_signals, win_count, win_rate,"
                " avg_pnl, avg_max_profit, avg_max_drawdown, details_json"
                " FROM eval_results WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                return None
            return {
                'run_id': row[0], 'evaluated_at': row[1],
                'total_signals': row[2], 'win_count': row[3], 'win_rate': row[4],
                'avg_pnl': row[5], 'avg_max_profit': row[6],
                'avg_max_drawdown': row[7], 'details': self._loads(row[8], []),
            }

        return self._safe(_read, None)

    # ---- AI 总结 ----
    def save_summary(self, summary_type: str, summary_data: dict,
                     signals_count: int | None = None,
                     sectors_count: int | None = None) -> bool:
        """总结归档（Redis 24h TTL 之外的耐久层）"""

        def _write() -> bool:
            conn = self._conn()
            conn.execute(
                """INSERT INTO summaries (summary_type, generated_at, market, model,
                        signals_count, sectors_count, full_text, data_json)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (summary_type, datetime.now().isoformat(),
                 (str(summary_data.get('market') or '')[:200] or None),
                 summary_data.get('model'),
                 int(signals_count or 0), int(sectors_count or 0),
                 summary_data.get('full_text') or '',
                 json.dumps(summary_data, ensure_ascii=False, default=str)))
            conn.commit()
            return True

        return self._safe(_write, False)

    def get_summary_history(self, summary_type: str | None = None,
                            limit: int = 20) -> list[dict]:
        """总结历史（id 倒序；summary_type 为空返回全部类型）"""

        def _read() -> list[dict]:
            conn = self._conn()
            if summary_type:
                cur = conn.execute(
                    "SELECT id, summary_type, generated_at, market, model,"
                    " signals_count, sectors_count, full_text, data_json"
                    " FROM summaries WHERE summary_type=?"
                    " ORDER BY id DESC LIMIT ?", (summary_type, int(limit)))
            else:
                cur = conn.execute(
                    "SELECT id, summary_type, generated_at, market, model,"
                    " signals_count, sectors_count, full_text, data_json"
                    " FROM summaries ORDER BY id DESC LIMIT ?", (int(limit),))
            return [{
                'id': r[0], 'summary_type': r[1], 'generated_at': r[2],
                'market': r[3], 'model': r[4], 'signals_count': r[5] or 0,
                'sectors_count': r[6] or 0, 'full_text': r[7] or '',
                'data': self._loads(r[8], {}),
            } for r in cur.fetchall()]

        return self._safe(_read, [])

    # ---- 运维 ----
    def table_counts(self) -> dict:
        """各表行数（健康检查/测试用）"""

        def _read() -> dict:
            conn = self._conn()
            return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    for t in ('signals', 'eval_runs', 'eval_results', 'summaries')}

        return self._safe(_read, {})

    def _prune(self) -> dict:
        """保留期清理（进程启动时一次）：
        信号 SIGNAL_RETENTION_DAYS 天 / 评估 EVAL_RETENTION_DAYS 天 /
        总结按 id 保留最新 SUMMARY_RETENTION_COUNT 条；孤儿评估结果级联清理
        """
        settings = get_settings()
        signal_cutoff = (datetime.now()
                         - timedelta(days=settings.SIGNAL_RETENTION_DAYS)).strftime('%Y-%m-%d')
        eval_cutoff = (datetime.now()
                       - timedelta(days=settings.EVAL_RETENTION_DAYS)).isoformat()
        conn = self._conn()
        n_signals = conn.execute(
            "DELETE FROM signals WHERE signal_date < ?", (signal_cutoff,)).rowcount
        n_runs = conn.execute(
            "DELETE FROM eval_runs WHERE timestamp < ?", (eval_cutoff,)).rowcount
        conn.execute(
            "DELETE FROM eval_results WHERE run_id NOT IN (SELECT run_id FROM eval_runs)")
        conn.execute(
            "DELETE FROM summaries WHERE id NOT IN"
            " (SELECT id FROM summaries ORDER BY id DESC LIMIT ?)",
            (int(settings.SUMMARY_RETENTION_COUNT),))
        conn.commit()
        pruned = {'signals': n_signals, 'eval_runs': n_runs}
        if n_signals or n_runs:
            logger.info(f"SQLite归档清理: {pruned}")
        return pruned

    @staticmethod
    def _loads(raw, default):
        if not raw:
            return default
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return default


_store: HistoryStore | None = None
_store_lock = threading.Lock()


def get_history_store() -> HistoryStore:
    """进程级单例（与 RedisCache 每进程一会话的模式一致）"""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = HistoryStore()
    return _store
