"""TDX 7709 纯 Python 协议核（D82，2026-11-24）——eltdx Rust 协议核的自研移植。

来源：`electkismet/eltdx` v3.3.5 源码逐函数分析（research/docs/45）+ ECS 真机抓包
（`tcpdump` 取 116.205.183.150:7709 实际报文）+ 仓库冻结测试向量逐位校验。

**为何自研而非依赖 wheel**：
- eltdx 许可证 Research-Only/Non-Commercial，明确禁止 production / automated trading；
  本项目 ECS 定时推信号属灰区（用户 2026-11-21 裁定自用非商业可用，但依赖始终存在）。
- wheel 是 PyO3/Rust abi3 二进制，py3.9 本地装不了、py3.10-3.14 各平台要分发；
  自研核纯标准库（struct/zlib/socket），本地 py3.9 与 ECS py3.14 同一份代码。
- 本项目只需 7709 的 K 线子集，不必背 84,499 行全栈。

**移植边界（零信号影响铁律）**：
- 只实现帧编解码 + 握手 + 心跳 + 节点池 + 日/分钟 K 线；输出列严格对齐项目标准
  (`date/open/high/low/close/volume/amount`)，成交量按 D80 `round(lots)*100` 口径。
- 分时/逐笔/资金流/榜单/连板等旁路能力仍走 eltdx wheel（未装则维持空，零降级）；
  不写任何 df.attrs、不改评分/门控/Kelly/风控/回测。
- 默认 `TDX_WIRE_ENABLED=true`，关掉回退到原 eltdx wheel 路径（若安装）。

**实证基线（ECS 2026-11-24 真机）**：600519 11-20 O=1450.89/H=1489.09/L=1423.9/
C=1485.75、000001 C=12.22，与 eltdx wheel 及 docs/45 历史记录逐值一致；0.03s/2 票。

**帧格式（抓包 + 源码冻结向量双重确认）**：
- 请求头 12B：`0x0C` + msg_id<u32 LE> + control<0x01> + len<u16 LE>(=data+2) ×2
  + msg_type<u16 LE> + data。注意 control 是**单字节**（pytdx 与本项目早期误写 2 字节致超时）。
- 响应头 16B：`b1 cb 74 00` + control + msg_id<u32> + reserved + msg_type<u16>
  + zip_length<u16> + length<u16> + payload（zip_length；若 zip!=len 则 zlib 解压）。
- 日/分钟 K 请求 0x052D payload 40B（抓包实证）：market<u16>(沪1深0) + 6s代码 +
  period<u16> + param<u16> + start<u16> + count<u16> + adjust<u16> + anchor<u32> + 18×0。
  GitHub 源码写 20 零（42B），线上 wheel 3.3.5 实发 18 零——以抓包为准（已验证）。
- 复权：adjust 0=none/1=qfq/2=hfq/3=fixed_qfq/4=fixed_hfq。
- 周期：day=4/1、5m=0/1、15m=1/1、30m=2/1、60m=3/1、1m=7/1（raw/param）。
"""
from __future__ import annotations

import math
import random
import socket
import struct
import threading
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

# 内置节点（eltdx v3.3.5 FALLBACK_HOSTS 前若干台； ECS 2026-11-24 实测 116.205.183.150 可用）
_DEFAULT_HOSTS = (
    '116.205.183.150:7709', '116.205.171.132:7709', '111.230.186.52:7709',
    '129.204.230.128:7709', '116.205.163.254:7709', '110.41.2.72:7709',
    '159.75.29.111:7709', '43.139.95.83:7709', '175.178.128.227:7709',
    '110.41.147.114:7709', '124.71.9.153:7709', '81.71.32.47:7709',
)
RESP_PREFIX = b'\xb1\xcb\x74\x00'
REPORT_HEADER_SIZE = 16
_SH_TZ = timezone(timedelta(hours=8), name='Asia/Shanghai')

_TYPE_HEARTBEAT = 0x0004
_TYPE_HANDSHAKE = 0x000D
_TYPE_KLINES = 0x052D

# 项目周期 → (raw, param)；与 eltdx unit.rs KlinePeriod::normalize 对齐
_PERIODS: dict[str, tuple[int, int]] = {
    '5m': (0, 1), '15m': (1, 1), '30m': (2, 1), '60m': (3, 1),
    'day': (4, 1), '1m': (7, 1),
}
_ADJUST: dict[str, int] = {
    'none': 0, 'qfq': 1, 'hfq': 2, 'fixed_qfq': 3, 'fixed_hfq': 4,
}


def normalize_code(code: str) -> tuple[int, str] | None:
    """项目码（600519/600519.SH/000001.SZ）→ (market_id, 6位数字)；非法返回 None。

    market_id 遵循 TDX 标准：沪=1、深=0、北=2（与 eltdx unit.rs MARKET_TO_ID 一致）。
    """
    sym = str(code or '').strip().lower()
    market: str | None = None
    if '.' in sym:
        head, suffix = sym.split('.', 1)
        if suffix not in {'sh', 'sz', 'bj'} or not head.isdigit() or len(head) != 6:
            return None
        market, sym = suffix, head
    if not sym.isdigit() or len(sym) != 6:
        return None
    if market is None:
            if sym.startswith(('5', '6', '9', '110', '113', '118')):
                market = 'sh'
            elif sym.startswith(('92', '4', '8')):
                market = 'bj'
            else:
                market = 'sz'
    return {'sh': 1, 'sz': 0, 'bj': 2}[market], sym


def map_period(period: str) -> tuple[int, int] | None:
    key = str(period or '').strip().lower()
    return _PERIODS.get(key)


def encode_frame(msg_id: int, msg_type: int, data: bytes = b'') -> bytes:
    """请求帧编码（冻结向量校验：0c 7b 000000 01 0800 0800 4e04 ...）。"""
    length = len(data) + 2
    return (struct.pack('<BIBHH', 0x0C, msg_id & 0xFFFFFFFF, 0x01, length, length)
            + struct.pack('<H', msg_type) + data)


@dataclass(frozen=True)
class _Response:
    msg_id: int
    msg_type: int
    control: int
    data: bytes
    compressed: bool


def decode_header(raw: bytes) -> tuple[int, int, int, int]:
    """响应头解析 → (msg_id, msg_type, zip_length, length)；含前缀校验。"""
    if len(raw) < REPORT_HEADER_SIZE or raw[:4] != RESP_PREFIX:
        raise ValueError(f'bad response header: {raw[:8].hex()}')
    return (
        int.from_bytes(raw[5:9], 'little'),
        int.from_bytes(raw[10:12], 'little'),
        int.from_bytes(raw[12:14], 'little'),
        int.from_bytes(raw[14:16], 'little'),
    )


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    buf = bytearray()
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            raise ConnectionError('tdx peer closed')
        buf.extend(chunk)
    return bytes(buf)


def _compact_float(value: int) -> float:
    """TDX 4 字节压缩浮点（成交量/成交额），与 pytdx/eltdx get_volume 同算法。"""
    if value == 0:
        return 0.0
    signed = int.from_bytes(value.to_bytes(4, 'big', signed=False), 'big', signed=True)
    log_point = signed >> 24
    h = (signed >> 16) & 0xFF
    m = (signed >> 8) & 0xFF
    low = signed & 0xFF
    base = math.pow(2.0, float(log_point * 2 - 0x7F))
    high = base * (64.0 + float(h & 0x7F)) / 64.0 if h > 0x80 else base * float(h) / 128.0
    scale = 2.0 if h & 0x80 else 1.0
    return base + high + base * float(m) / 32768.0 * scale + base * float(low) / 8388608.0 * scale


def _varint(payload: bytes, offset: int) -> tuple[int, int]:
    """eltdx consume_varint：首字节低 6 位 + 0x40 符号位，后续 7 位续接。"""
    value = 0
    shift = 0
    start = offset
    while True:
        if offset >= len(payload):
            raise ValueError('unterminated varint')
        byte = payload[offset]
        offset += 1
        if offset - 1 == start:
            value += byte & 0x3F
            shift = 6
        else:
            value += (byte & 0x7F) << shift
            shift += 7
        if not byte & 0x80:
            break
    return (-value if payload[start] & 0x40 else value), offset


def _bar_time(raw: bytes, period_raw: int) -> datetime:
    """K 线时间解码：日线 yyyymmdd<u32>；分钟线 date packed <u16> + minute-of-day <u16>。"""
    if len(raw) != 4:
        raise ValueError('invalid bar time length')
    if period_raw in {0, 1, 2, 3, 7, 8}:
        packed = int.from_bytes(raw[:2], 'little')
        minute = int.from_bytes(raw[2:4], 'little')
        year = (packed >> 11) + 2004
        rest = packed % 2048
        return datetime(year, rest // 100, rest % 100, minute // 60, minute % 60, tzinfo=_SH_TZ)
    if period_raw == 13:
        return datetime(2003, 12, 31, tzinfo=_SH_TZ) + timedelta(
            seconds=int.from_bytes(raw, 'little'))
    ymd = int.from_bytes(raw, 'little')
    if not 19900101 <= ymd <= 20991231:
        raise ValueError(f'invalid bar date: {ymd}')
    return datetime(ymd // 10000, (ymd % 10000) // 100, ymd % 100, 15, 0, tzinfo=_SH_TZ)


def parse_klines(payload: bytes, period_raw: int, *, volume_lots: bool = True) -> list[dict]:
    """解析 0x052D 响应 payload → 标准记录列表。

    `volume_lots=True` 时 volume = 手数（D80 对齐腾讯按手取整：`round(lots)*100` 在适配层做）。
    """
    if len(payload) < 2:
        return []
    count = int.from_bytes(payload[:2], 'little')
    offset = 2
    bars: list[dict] = []
    last_close = 0
    for _ in range(count):
        time_raw = payload[offset:offset + 4]
        if len(time_raw) < 4:
            break
        t = _bar_time(time_raw, period_raw)
        offset += 4
        try:
            open_delta, offset = _varint(payload, offset)
            close_delta, offset = _varint(payload, offset)
            high_delta, offset = _varint(payload, offset)
            low_delta, offset = _varint(payload, offset)
        except ValueError:
            break
        if offset + 8 > len(payload):
            break
        volume_raw = int.from_bytes(payload[offset:offset + 4], 'little')
        amount_raw = int.from_bytes(payload[offset + 4:offset + 8], 'little')
        offset += 8
        open_milli = last_close + open_delta
        close_milli = open_milli + close_delta
        high_milli = open_milli + high_delta
        low_milli = open_milli + low_delta
        last_close = close_milli
        vol = _compact_float(volume_raw)
        # 对齐 eltdx KlineBar.volume_lots 语义：wire 原始值是股数，wheel 除以 100 = 手数。
        # 适配层 D80 `round(lots)*100` 会把手数还原为按手取整的股数；若这里直接给股数
        # 会被再乘 100 放大 100 倍（ECS 真机 600519 11-20 实证：6,722,940 股 vs 67,229.4 手）。
        lots = vol / 100.0 if volume_lots else vol
        bars.append({
            'time': t,
            'open': open_milli / 1000.0,
            'close': close_milli / 1000.0,
            'high': high_milli / 1000.0,
            'low': low_milli / 1000.0,
            'volume_lots': lots,
            'amount': _compact_float(amount_raw),
        })
    return bars


# ── 0x000F 权息事件 + 本地复权（D82 精度攻坚，源码机制自研）────────────────
# 机制来源 eltdx corporate.rs / equity.py（research/docs/46 §6.5），纯自研实现。
# 意义：服务端 qfq 按 round(·,2) 舍入致 0.001 差异；拿 adjust=none 完整毫位价 +
# 权息系数本地复合，可保留 3 位逐位对齐腾讯（ECS 120/120 实证）。
_TYPE_CAPITAL_CHANGES = 0x000F
_CAP_RECORD_SIZE = 29  # 定长记录：market1+code6+resv1+date4+cat1+c1..c4(4×4)


def parse_capital_changes(payload: bytes) -> list[dict]:
    """解析 0x000F 响应 → 权息事件记录列表（category/date/c1..c4）。

    block: u16 block_count + 每块 [market u8 + code 6s + count u16 + count×29B record]。
    record: market1 + code6 + reserved1 + date u32([8:12]) + category u8([12])
            + c1/c2/c3/c4 f32 LE([13:17],[17:21],[21:25],[25:29])。
    category=1（送转配/分红）才参与复权；category 属股本类时 c 值 ×10000（此处原样保留，
    由 _adjustment_factors 按 category 处理，与 eltdx parse_capital_change_record 一致）。
    """
    if len(payload) < 2:
        return []
    offset = 2  # skip block_count
    events: list[dict] = []
    while offset + 9 <= len(payload):
        market = payload[offset]
        number = payload[offset + 1:offset + 7].decode('ascii', errors='ignore')
        count = int.from_bytes(payload[offset + 7:offset + 9], 'little')
        offset += 9
        for _ in range(count):
            if offset + _CAP_RECORD_SIZE > len(payload):
                return events
            rec = payload[offset:offset + _CAP_RECORD_SIZE]
            date_raw = int.from_bytes(rec[8:12], 'little')
            category = rec[12]
            raw_vals = [struct.unpack('<f', rec[13 + 4 * i:17 + 4 * i])[0] for i in range(4)]
            # category 属"股本/送转"类按 ×10000 还原（对齐 eltdx is_share_count_category）
            if category in {2, 3, 4, 5}:
                vals = [v * 10000.0 for v in raw_vals]
            elif category == 6:
                vals = [raw_vals[0], raw_vals[1], raw_vals[2] * 10000.0, raw_vals[3]]
            else:
                vals = raw_vals
            if date_raw:
                y, md = date_raw // 10000, date_raw % 10000
                events.append({
                    'market': market, 'code': number,
                    'date': datetime(y, md // 100, md % 100, tzinfo=_SH_TZ).date(),
                    'category': category,
                    'c1': vals[0], 'c2': vals[1], 'c3': vals[2], 'c4': vals[3],
                })
            offset += _CAP_RECORD_SIZE
    return events


def adjustment_factors(events: list[dict], *, start_date=None, anchor_date=None) -> list[dict]:
    """从权息事件复合前复权 scale/offset（eltdx equity.py 纯数学自研）。

    仅取 category=1 且 date 有效、>= start_date 的事件，按日期升序；
    每个事件日一行，复合该日<=date<=anchor 的全部事件：
        m = (10 + S + R)/10;  c = (D - R*P)/10     (D=c1 P=c2 S=c3 R=c4)
        qfq_scale /= m;  qfq_offset = (qfq_offset - c)/m
    返回按日期升序的 [{date, scale, offset}]，供 local_qfq 使用。
    """
    evs = [e for e in events
           if e.get('category') == 1 and e.get('date') is not None
           and (start_date is None or e['date'] >= start_date)]
    evs.sort(key=lambda e: e['date'])
    dates = sorted({e['date'] for e in evs})
    out = []
    for d in dates:
        scale, offset = 1.0, 0.0
        for e in evs:
            if e['date'] >= d and (anchor_date is None or e['date'] <= anchor_date):
                m = (10.0 + e['c3'] + e['c4']) / 10.0
                c = (e['c1'] - e['c4'] * e['c2']) / 10.0
                if m <= 0:
                    continue
                scale /= m
                offset = (offset - c) / m
        out.append({'date': d, 'scale': scale, 'offset': offset})
    return out


def local_qfq(bars: list[dict], factors: list[dict], *, digits: int = 3) -> list[dict]:
    """对 adjust=none 的不复权 bar 应用本地前复权，返回新 bar 列表（不改入参）。

    前复权系数选择：第一条 bar_date < factor.date 的行；无匹配用 scale=1/offset=0。
    digits 控制舍入位数：3 逐位对齐腾讯，2 复现服务端 qfq（ECS 120/120 实证）。
    """
    result = []
    for b in bars:
        t = b['time']
        bar_date = t.date() if isinstance(t, datetime) else t
        scale, offset = 1.0, 0.0
        for f in factors:
            if bar_date < f['date']:
                scale, offset = f['scale'], f['offset']
                break
        nb = dict(b)
        for key in ('open', 'close', 'high', 'low'):
            if key in nb and nb[key] is not None:
                nb[key] = round(nb[key] * scale + offset, digits)
        result.append(nb)
    return result


class TdxWireError(RuntimeError):
    """协议/连接层错误（上层按失败降级，不抛到信号链）。"""


class TdxWireClient:
    """TDX 7709 纯 Python 只读客户端：单连接 + 锁串行 + 握手 + 失败重连。

    与 eltdx wheel 的 `TdxClient.bars.get()` 接口子集兼容，供 `EltdxClient` 直接复用
    现有 `_bars_to_df` 转换，零改上层契约。仅实现 K 线；其余旁路仍交 wheel。
    """

    def __init__(self, hosts: Iterable[str] | None = None, *, timeout: float = 3.0,
                 heartbeat_interval: float = 30.0, max_retry: int = 2):
        self.hosts = [h.strip() for h in (hosts or _DEFAULT_HOSTS) if h.strip()]
        self.timeout = float(timeout)
        self.heartbeat_interval = float(heartbeat_interval)
        self.max_retry = max(0, int(max_retry))
        self._sock: socket.socket | None = None
        self._host: str | None = None
        self._msg_id = random.randint(1, 0xFFFFFFFF)
        self._last_io = 0.0
        self._lock = threading.RLock()
        self._warned = False
        # 与 eltdx TdxClient.bars 兼容的子集接口（放在 __init__ 避免类体前向引用）
        self.bars = TdxWireClient._BarsApi(self)
        self.corporate = TdxWireClient._CorporateApi(self)

    def _next_id(self) -> int:
        self._msg_id = (self._msg_id + 1) & 0xFFFFFFFF
        return self._msg_id or 1

    def _parse_host(self, host: str) -> tuple[str, int]:
        host = host.strip()
        if ':' in host:
            addr, port = host.rsplit(':', 1)
            return addr, int(port)
        return host, 7709

    def close(self) -> None:
        with self._lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except Exception:  # noqa: S110 - 关闭失败无需处理，上层已记日志
                    pass
                self._sock = None
                self._host = None

    def _connect_locked(self) -> None:
        """连接 + 握手；任一节点失败换下一个，全部失败抛 TdxWireError。"""
        import time
        last_error: Exception | None = None
        for host in self.hosts:
            addr, port = self._parse_host(host)
            try:
                sock = socket.create_connection((addr, port), timeout=self.timeout)
                sock.settimeout(self.timeout)
                msg_id = self._next_id()
                sock.sendall(encode_frame(msg_id, _TYPE_HANDSHAKE, b'\x01'))
                header = _recv_exact(sock, REPORT_HEADER_SIZE)
                rid, rtype, zlen, length = decode_header(header)
                payload = _recv_exact(sock, zlen)
                if zlen != length:
                    payload = zlib.decompress(payload)
                if rid != msg_id or rtype != _TYPE_HANDSHAKE or len(payload) < 189:
                    raise TdxWireError('invalid handshake response')
                self._sock = sock
                self._host = host
                self._last_io = time.monotonic()
                return
            except Exception as e:
                last_error = e
                try:
                    sock.close()  # type: ignore[possibly-undefined]
                except Exception:  # noqa: S110 - 连接失败后的清理，错误已在末端统一抛出
                    pass
        raise TdxWireError(f'all TDX hosts failed: {type(last_error).__name__}: {last_error}')

    def _request_locked(self, msg_type: int, data: bytes) -> bytes:
        import time
        if self._sock is None:
            self._connect_locked()
        assert self._sock is not None
        msg_id = self._next_id()
        try:
            if time.monotonic() - self._last_io >= self.heartbeat_interval:
                self._sock.sendall(encode_frame(self._next_id(), _TYPE_HEARTBEAT))
                _recv_exact(self._sock, REPORT_HEADER_SIZE)  # 心跳响应只消费头
            self._sock.sendall(encode_frame(msg_id, msg_type, data))
            header = _recv_exact(self._sock, REPORT_HEADER_SIZE)
            rid, rtype, zlen, length = decode_header(header)
            payload = _recv_exact(self._sock, zlen)
            if zlen != length:
                payload = zlib.decompress(payload)
            if rid != msg_id or rtype != msg_type:
                raise TdxWireError(f'mismatched response: {rid}/{rtype} vs {msg_id}/{msg_type}')
            self._last_io = time.monotonic()
            return payload
        except Exception:
            self.close()
            raise

    def _request(self, msg_type: int, data: bytes) -> bytes:
        last_error: Exception | None = None
        for attempt in range(self.max_retry + 1):
            with self._lock:
                try:
                    return self._request_locked(msg_type, data)
                except Exception as e:
                    last_error = e
        raise TdxWireError(f'request failed after {self.max_retry + 1} attempts: {last_error}')

    # ── 与 eltdx TdxClient.bars 兼容的接口子集 ──────────────────────────────
    class _BarsApi:
        def __init__(self, owner: TdxWireClient):
            self._owner = owner
            self.last_call: tuple[str, str, int, str | None] | None = None

        def get(self, code: str, *, period: str = 'day', count: int = 800,
                adjust: str | None = None, **_: Any) -> TdxWireClient.BarSeries:
            owner = self._owner
            self.last_call = (code, period, int(count), adjust)
            conv = normalize_code(code)
            per = map_period(period)
            if conv is None or per is None or count <= 0:
                return TdxWireClient.BarSeries([])
            period_raw, param = per
            adjust_key = (adjust or 'none').strip().lower()
            adjust_val = _ADJUST.get(adjust_key)
            if adjust_val is None:
                return TdxWireClient.BarSeries([])
            market, number = conv
            # 抓包实证 payload 40B（18 尾零）；count 上限 800（TDX 单页）
            payload = (struct.pack('<H6sHHHHHI', market, number.encode('ascii'),
                                   period_raw, param, 0, min(int(count), 800), adjust_val, 0)
                       + b'\x00' * 18)
            try:
                body = owner._request(_TYPE_KLINES, payload)
                bars = parse_klines(body, period_raw)
            except Exception as e:
                if not owner._warned:
                    logger.warning(f'[TDX-WIRE] {code} K线失败 {type(e).__name__}: {e}')
                    owner._warned = True
                return TdxWireClient.BarSeries([])
            return TdxWireClient.BarSeries(bars)

    @dataclass
    class BarSeries:
        bars: list[dict]

    class _CorporateApi:
        """0x000F 权息事件查询（对齐 eltdx client.corporate 子集）。"""

        def __init__(self, owner: TdxWireClient):
            self._owner = owner

        def capital_changes(self, code: str) -> list[dict]:
            conv = normalize_code(code)
            if conv is None:
                return []
            market, number = conv
            # 请求帧：count u16 + 每码 [market u8 + 6s 代码]
            data = struct.pack('<H', 1) + bytes([market]) + number.encode('ascii')
            try:
                body = self._owner._request(_TYPE_CAPITAL_CHANGES, data)
                return parse_capital_changes(body)
            except Exception as e:
                if not self._owner._warned:
                    logger.warning(f'[TDX-WIRE] {code} 权息查询失败 {type(e).__name__}: {e}')
                    self._owner._warned = True
                return []

        def adjustment_factors(self, code: str, *, start_date=None,
                               anchor_date=None) -> list[dict]:
            events = self.capital_changes(code)
            return adjustment_factors(events, start_date=start_date, anchor_date=anchor_date)

    def ping(self) -> str:
        with self._lock:
            if self._sock is None:
                self._connect_locked()
            return 'pong' if self._sock is not None else ''

    def probe_hosts(self, max_hosts: int | None = None) -> list[dict]:
        """轻量 TCP 握手探测（只连+握手，不发 K 线请求）；不改节点列表。"""
        import time
        results: list[dict] = []
        for host in (self.hosts[:max_hosts] if max_hosts else self.hosts):
            addr, port = self._parse_host(host)
            started = time.monotonic()
            sock: socket.socket | None = None
            try:
                sock = socket.create_connection((addr, port), timeout=self.timeout)
                sock.settimeout(self.timeout)
                msg_id = self._next_id()
                sock.sendall(encode_frame(msg_id, _TYPE_HANDSHAKE, b'\x01'))
                header = _recv_exact(sock, REPORT_HEADER_SIZE)
                rid, rtype, zlen, _ = decode_header(header)
                _recv_exact(sock, zlen)
                ok = rid == msg_id and rtype == _TYPE_HANDSHAKE
                results.append({'host': host, 'ok': ok,
                                'latency_ms': int((time.monotonic() - started) * 1000), 'error': ''})
            except Exception as e:
                results.append({'host': host, 'ok': False, 'latency_ms': 0,
                                'error': f'{type(e).__name__}: {e}'})
            finally:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:  # noqa: S110 - 探测清理，错误已写入 results
                        pass
        return results


__all__ = [
    'TdxWireClient', 'TdxWireError', 'adjustment_factors', 'decode_header',
    'encode_frame', 'local_qfq', 'map_period', 'normalize_code',
    'parse_capital_changes', 'parse_klines',
]
