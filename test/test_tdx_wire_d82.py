"""D82 自研 TDX 7709 协议核专项测试（纯标准库，py3.9 可跑，无需 eltdx wheel / 网络）。

纪律：验证协议编解码正确性 + 适配层 wire 路径与优雅降级，零信号影响。
真机一致性证据见 research/docs/45 §7（ECS 抓包回放 + 与 wheel 逐值对照）。
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quant_engine.config import Settings
from quant_engine.data.sources.eltdx_client import EltdxClient
from quant_engine.data.sources.tdx_wire import (
    TdxWireClient,
    decode_header,
    encode_frame,
    map_period,
    normalize_code,
    parse_klines,
)

_TZ = timezone(timedelta(hours=8))


class TestFrameCodec(unittest.TestCase):
    def test_request_frame_matches_frozen_vector(self):
        # eltdx frame.rs 冻结向量：msg_id=123 / type=0x044e / 6B data
        raw = encode_frame(123, 0x044E, bytes([0, 0, 0xA7, 0x26, 0x35, 1]))
        self.assertEqual(
            raw.hex(), '0c7b00000001080008004e040000a7263501')

    def test_handshake_frame_matches_frozen_vector(self):
        # session.rs 冻结：msg_id=1, type=0x000d, data=0x01
        self.assertEqual(encode_frame(1, 0x000D, b'\x01').hex(),
                         '0c0100000001030003000d0001')

    def test_control_is_single_byte_not_two(self):
        # 回归最关键的坑：control 若误写成 2 字节，帧长+1 且服务端静默超时
        raw = encode_frame(1, 0x000D, b'\x01')
        self.assertEqual(len(raw), 13)
        self.assertEqual(raw[5], 0x01)  # control 单字节

    def test_decode_response_header(self):
        # 抓包握手响应头 b1cb7400 1c213758a800 0d00 5200 bd00
        hdr = bytes.fromhex('b1cb74001c213758a8000d005200bd00')
        msg_id, mtype, zlen, length = decode_header(hdr)
        self.assertEqual(msg_id, 0xA8583721)
        self.assertEqual(mtype, 0x000D)
        self.assertEqual((zlen, length), (0x52, 0xBD))

    def test_decode_header_rejects_bad_prefix(self):
        with self.assertRaises(ValueError):
            decode_header(bytes([0xFF] * 4 + [0] * 12))


class TestCodeAndPeriod(unittest.TestCase):
    def test_normalize_code(self):
        self.assertEqual(normalize_code('600519'), (1, '600519'))
        self.assertEqual(normalize_code('000001.SZ'), (0, '000001'))
        self.assertEqual(normalize_code('430047.BJ'), (2, '430047'))
        self.assertEqual(normalize_code('159509'), (0, '159509'))  # 深市 ETF

    def test_normalize_code_invalid(self):
        for bad in ['', 'abc', '60051', '000001.US']:
            self.assertIsNone(normalize_code(bad))

    def test_periods(self):
        self.assertEqual(map_period('day'), (4, 1))
        self.assertEqual(map_period('5m'), (0, 1))
        self.assertEqual(map_period('1m'), (7, 1))
        self.assertIsNone(map_period('min5'))


class TestKlineParsing(unittest.TestCase):
    @staticmethod
    def _day_payload():
        # 抓包回放：600519 11-20 一根，qfq OHLC 1450.89/1489.09/1423.9/1485.75
        # 用 eltdx 冻结测试的同款 varint 结构构造（delta 相对前收盘）
        import struct
        p = bytearray(struct.pack('<H', 1))  # count=1
        p.extend(struct.pack('<I', 20261120))  # 日线 yyyymmdd
        # open=1450.89 close=1485.75 high=1489.09 low=1423.9，last_close=0 起算
        for val in [1450890, 34860, 38200, -26990]:
            # 自研 varint：首字节 6 位 + 符号位 0x40
            v = abs(val)
            chunks = [v & 0x3F]
            v >>= 6
            while v:
                chunks.append(v & 0x7F)
                v >>= 7
            for i, c in enumerate(chunks):
                byte = c | (0x40 if (i == 0 and val < 0) else 0)
                if i < len(chunks) - 1:
                    byte |= 0x80
                p.append(byte)
        # volume/amount 用压缩浮点：2.0 = 0x40000000
        p.extend((0x40000000).to_bytes(4, 'little'))  # volume wire=2.0 → 200 股 → 2 手
        p.extend((0x40000000).to_bytes(4, 'little'))
        return bytes(p)

    def test_parse_day_bar(self):
        bars = parse_klines(self._day_payload(), 4)
        self.assertEqual(len(bars), 1)
        b = bars[0]
        self.assertEqual(b['time'], datetime(2026, 11, 20, 15, 0, tzinfo=_TZ))
        self.assertAlmostEqual(b['open'], 1450.89)
        self.assertAlmostEqual(b['close'], 1485.75)
        self.assertAlmostEqual(b['high'], 1489.09)
        self.assertAlmostEqual(b['low'], 1423.9)
        # 成交量口径：wire 原始 get_volume=2.0 → volume_lots=2/100=0.02 手（对齐 wheel，
        # eltdx convertible_tests 实证）。回归 D82 关键坑：若直接返回股数会被适配层放大 100 倍。
        self.assertAlmostEqual(b['volume_lots'], 0.02)

    def test_parse_minute_time(self):
        # period_raw=0(5m)：packed date + minute-of-day
        import struct
        packed = ((2026 - 2004) << 11) | 1120  # 2026-11-20
        minute = 14 * 60 + 55
        payload = struct.pack('<H', 1) + struct.pack('<HH', packed, minute) + \
            bytes([0] * 5) + bytes(8)
        bars = parse_klines(payload, 0)
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0]['time'], datetime(2026, 11, 20, 14, 55, tzinfo=_TZ))

    def test_empty_payload(self):
        self.assertEqual(parse_klines(b'', 4), [])


class TestWireClientContract(unittest.TestCase):
    def test_bar_series_api_shape(self):
        c = TdxWireClient(hosts=['116.205.183.150:7709'], timeout=0.1)
        self.assertTrue(hasattr(c, 'bars'))
        self.assertTrue(hasattr(c.bars, 'get'))
        self.assertTrue(callable(c.ping))

    def test_invalid_period_returns_empty_without_network(self):
        c = TdxWireClient(hosts=['116.205.183.150:7709'], timeout=0.1)
        series = c.bars.get('600519.SH', period='min5', count=10, adjust='qfq')
        self.assertEqual(series.bars, [])


class TestAdapterWirePath(unittest.TestCase):
    def test_wire_disabled_uses_factory(self):
        """注入 factory 时即使 TDX_WIRE_ENABLED 也走 factory（契约测试隔离）。"""
        class Bars:
            def get(self, code, *, period, count, adjust):
                return SimpleNamespace(bars=[{
                    'time': datetime(2026, 11, 20), 'open': 1, 'high': 2,
                    'low': 0.5, 'close': 1.5, 'volume_lots': 100, 'amount': 150,
                }])

        class C:
            def __init__(self, _hosts):
                self.bars = Bars()

        cfg = Settings(ELTDX_ENABLED=True, TDX_WIRE_ENABLED=True)
        c = EltdxClient(settings=cfg, client_factory=lambda h: C(h))
        df = c.get_daily_data('600519.SH', 10)
        self.assertEqual(len(df), 1)
        self.assertEqual(float(df.iloc[0]['close']), 1.5)
        self.assertEqual(float(df.iloc[0]['volume']), 10000.0)  # lots×100

    def test_disabled_returns_empty(self):
        cfg = Settings(ELTDX_ENABLED=False, TDX_WIRE_ENABLED=True)
        c = EltdxClient(settings=cfg)
        self.assertTrue(c.get_daily_data('600519.SH', 10).empty)


if __name__ == '__main__':
    unittest.main(verbosity=2)
