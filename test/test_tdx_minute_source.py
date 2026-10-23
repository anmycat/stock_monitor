"""通达信分钟线数据源测试（2026-10-20 TDX minute 接入落地）

背景：PRD 02 §3.9 调研结论——TDX（pytdx TCP 7709）5/15/30/60 分钟K 完整
可用，正好补齐 gates._check_minute_trigger 的分钟断链（接入前无任何源实现
get_minute_data，akshare 亦未安装，gates 恒"分钟数据不足"放行）。

本测试全部离线：用假 pytdx 模块注入 sys.modules，不发真实网络请求。
覆盖：周期映射、字段与排序、服务器池探活/轮换、会话中重试、全失败降级、
pytdx 缺失降级、代码转换（沪深支持/北交所不支持）、优先级注册。
"""
import os
import sys
import types
import unittest
from typing import ClassVar
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from quant_engine.data.sources.tdx import TDX_SERVERS, TdxSource


def _make_bars(count=20, start_price=10.0):
    """构造 TDX 风格的 bar 列表（旧→新升序，与真实 pytdx 实测一致：
    bars[-1] 即最新根；2026-10-20 真实调用验证过该顺序）"""
    bars = []
    for i in range(count):
        price = start_price + i * 0.01
        minute = 5 + i * 5  # 时间随 i 推进
        bars.append({
            'datetime': f'2026-10-20 {9 + minute // 60:02d}:{minute % 60:02d}',
            'open': price - 0.02, 'close': price,
            'high': price + 0.03, 'low': price - 0.05,
            'vol': 1000.0 * (i + 1),      # TDX 单位实测为股（无需换算）
            'amount': 10000.0 * (i + 1),
        })
    return bars


class FakeTdxHq_API:
    """假 TDX 客户端：模拟协议级抖动（TCP 通但返空/超时）与成功路径"""

    # 类级配置：每次测试前重置
    fail_connect: ClassVar[set] = set()    # {(ip, port), ...} connect 返回 False
    empty_quote: ClassVar[set] = set()     # {(ip, port), ...} 连接成功但快照返空（协议级不可用）
    bar_errors: ClassVar[int] = 0          # get_security_bars 前 N 次抛异常（会话中抖动）
    quote_calls: ClassVar[list] = []       # 记录探活快照调用
    bar_calls: ClassVar[list] = []         # 记录 K 线调用参数

    def __init__(self, heartbeat=False):
        self.connected = False
        self.host = None

    def connect(self, ip, port, time_out=None):
        if (ip, port) in FakeTdxHq_API.fail_connect:
            return False
        self.connected = True
        self.host = (ip, port)
        return True

    def get_security_quotes(self, codes):
        FakeTdxHq_API.quote_calls.append(list(codes))
        if not self.connected:
            return None
        if self.host in FakeTdxHq_API.empty_quote:
            return None  # 协议级不可用：连接成功但无数值
        return [{'code': c, 'price': 9.76, 'name': '浦发银行'} for _, c in codes]

    def get_security_bars(self, category, market, code, start, count):
        FakeTdxHq_API.bar_calls.append((category, market, code, start, count))
        if not self.connected:
            return None
        if FakeTdxHq_API.bar_errors > 0:
            FakeTdxHq_API.bar_errors -= 1
            raise ConnectionError('simulated mid-session drop')
        return _make_bars(count=min(count, 20))

    def disconnect(self):
        self.connected = False
        self.host = None


def _fake_pytdx_modules():
    """构造假 pytdx 模块树（pytdx.hq.TdxHq_API → FakeTdxHq_API）"""
    hq_mod = types.ModuleType('pytdx.hq')
    hq_mod.TdxHq_API = FakeTdxHq_API
    pkg_mod = types.ModuleType('pytdx')
    pkg_mod.hq = hq_mod
    return {'pytdx': pkg_mod, 'pytdx.hq': hq_mod}


class TdxMinuteSourceTest(unittest.TestCase):
    """TdxSource 分钟线取数（离线 mock）"""

    def setUp(self):
        FakeTdxHq_API.fail_connect = set()
        FakeTdxHq_API.empty_quote = set()
        FakeTdxHq_API.bar_errors = 0
        FakeTdxHq_API.quote_calls = []
        FakeTdxHq_API.bar_calls = []
        # 重置 TdxSource 类级共享状态（连接/探活缓存/懒加载标记）
        with TdxSource._lock:
            TdxSource._api = None
            TdxSource._server = None
            TdxSource._probe_time = 0.0
            TdxSource._pytdx_checked = False
            TdxSource._pytdx_available = False

    def tearDown(self):
        with TdxSource._lock:
            TdxSource._api = None
            TdxSource._server = None

    # ---------------------------------------------------------- 基本取数

    def test_minute_5_returns_standard_dataframe(self):
        """5 分钟K：标准列、升序、数值透传（iloc[-1] 必须是最新根——gates 依赖）"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            df = TdxSource.get_minute_data('600000.SH', '5')
        self.assertFalse(df.empty)
        self.assertEqual(list(df.columns),
                         ['date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        # 输出必须严格升序
        self.assertEqual(df['date'].tolist(), sorted(df['date'].tolist()))
        first, last = df.iloc[0], df.iloc[-1]
        self.assertEqual(first['date'], '2026-10-20 09:05')
        self.assertAlmostEqual(first['close'], 10.00)
        self.assertAlmostEqual(first['volume'], 1000.0)   # 股，不换算
        self.assertAlmostEqual(first['amount'], 10000.0)
        # gates 用 iloc[-1] 取最新价：末行必须是最新时间
        self.assertEqual(last['date'], '2026-10-20 10:40')
        self.assertAlmostEqual(last['close'], 10.19)

    def test_period_category_mapping(self):
        """周期映射：5/15/30/60 → category 0/1/2/3"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            for period, expect_cat in [('5', 0), ('15', 1), ('30', 2), ('60', 3)]:
                FakeTdxHq_API.bar_calls = []
                df = TdxSource.get_minute_data('000001.SZ', period)
                self.assertFalse(df.empty, f'{period}分钟应取到数')
                self.assertEqual(FakeTdxHq_API.bar_calls[-1][0], expect_cat)
                # 深市 market id = 0
                self.assertEqual(FakeTdxHq_API.bar_calls[-1][1], 0)

    def test_shanghai_market_id(self):
        """沪市 market id = 1"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            TdxSource.get_minute_data('600000.SH', '5')
        self.assertEqual(FakeTdxHq_API.bar_calls[-1][1], 1)
        self.assertEqual(FakeTdxHq_API.bar_calls[-1][2], '600000')

    def test_unsupported_period_1min(self):
        """1 分钟K TDX 不支持（调研实测每天仅返 1 根）→ 空且不建连接"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            df = TdxSource.get_minute_data('600000.SH', '1')
        self.assertTrue(df.empty)
        self.assertEqual(FakeTdxHq_API.bar_calls, [])
        self.assertEqual(FakeTdxHq_API.quote_calls, [])

    def test_bj_market_unsupported(self):
        """北交所 TDX 无 market（调研实测两市场均空）→ 空"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            df = TdxSource.get_minute_data('920819.BJ', '5')
        self.assertTrue(df.empty)
        self.assertEqual(FakeTdxHq_API.bar_calls, [])

    def test_bad_code_returns_empty(self):
        """无法解析的代码 → 空，不炸"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            df = TdxSource.get_minute_data('NOT_A_CODE', '5')
        self.assertTrue(df.empty)

    def test_output_ascending_regardless_of_input_order(self):
        """无论上游返回升序还是倒序，输出必须升序（不依赖顺序假设）"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            df_asc = TdxSource.get_minute_data('600000.SH', '5')
            # 人为把 fake 输入翻成倒序（模拟上游顺序变化）
            original = FakeTdxHq_API.get_security_bars

            def reversed_bars(self, *a, **kw):
                return list(reversed(original(self, *a, **kw)))

            FakeTdxHq_API.get_security_bars = reversed_bars
            try:
                df_desc = TdxSource.get_minute_data('600000.SH', '5')
            finally:
                FakeTdxHq_API.get_security_bars = original
        self.assertEqual(df_asc['date'].tolist(), df_desc['date'].tolist())
        self.assertEqual(df_desc['date'].tolist(), sorted(df_desc['date'].tolist()))

    # ---------------------------------------------------------- 服务器池

    def test_server_pool_rotation(self):
        """前 3 台 connect 失败、第 4 台协议级返空 → 第 5 台成功"""
        fail = {TDX_SERVERS[0], TDX_SERVERS[1], TDX_SERVERS[2]}
        empty = {TDX_SERVERS[3]}
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            FakeTdxHq_API.fail_connect = fail
            FakeTdxHq_API.empty_quote = empty
            df = TdxSource.get_minute_data('600000.SH', '5')
        self.assertFalse(df.empty)
        self.assertIn(TdxSource._server, [f'{ip}:{port}' for ip, port in TDX_SERVERS[4:6]])

    def test_connection_reuse_no_reprobe(self):
        """连接可用且探活未过期 → 复用，不重复探活"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            TdxSource.get_minute_data('600000.SH', '5')
            calls_after_first = len(FakeTdxHq_API.quote_calls)
            TdxSource.get_minute_data('600000.SH', '5')
        self.assertEqual(len(FakeTdxHq_API.quote_calls), calls_after_first)

    def test_retry_after_mid_session_failure(self):
        """会话中掉线：首次 bars 抛异常 → 自动重探服务器 → 第二次成功"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            FakeTdxHq_API.bar_errors = 1
            df = TdxSource.get_minute_data('600000.SH', '5')
        self.assertFalse(df.empty)
        # 至少两次 bars 调用（首次失败 + 重试成功）
        self.assertGreaterEqual(len(FakeTdxHq_API.bar_calls), 2)

    def test_all_servers_down_returns_empty(self):
        """全部服务器不可用 → 空 DataFrame（不抛异常，交降级链）"""
        with patch.dict(sys.modules, _fake_pytdx_modules()):
            FakeTdxHq_API.fail_connect = set(TDX_SERVERS)
            df = TdxSource.get_minute_data('600000.SH', '5')
        self.assertTrue(df.empty)

    def test_pytdx_not_installed_degrades(self):
        """pytdx 未安装 → 空 DataFrame + 不炸（ECS 未装时自动降级）"""
        modules = {'pytdx': None, 'pytdx.hq': None}  # import 时抛 ImportError
        with patch.dict(sys.modules, modules):
            df = TdxSource.get_minute_data('600000.SH', '5')
        self.assertTrue(df.empty)
        self.assertEqual(FakeTdxHq_API.bar_calls, [])


class TdxRegistrationTest(unittest.TestCase):
    """TDX 在数据源链路的注册与优先级"""

    def test_priority_and_registration(self):
        from quant_engine.data.source import _PRIORITY, CompositeSource
        # minute 第一线且唯一线为 tdx（akshare 遗留链已于 P0 切除，见 prd/15）
        self.assertEqual(_PRIORITY['minute'], ['tdx'])
        cs = CompositeSource()
        self.assertIn('tdx', cs.sources)
        self.assertTrue(hasattr(cs.sources['tdx'], 'get_minute_data'))
        # akshare 死依赖零残留
        self.assertNotIn('akshare', cs.sources)
        for cap in _PRIORITY:
            self.assertNotIn('akshare', _PRIORITY[cap])
        # 其他主路不含 tdx（实时/列表/日线维持腾讯/新浪）
        for cap in ('realtime', 'stock_list', 'daily'):
            self.assertNotIn('tdx', _PRIORITY[cap])


if __name__ == '__main__':
    unittest.main()
