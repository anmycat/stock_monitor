"""D68 东财反封禁「三招」跨进程加固专项测试

用户截图要求：「第一，限速，两次请求至少隔一秒。第二，加点随机抖动，
别太像机器人。第三，所有请求共用一个连接，别每次都新建。」

本测试覆盖：
  - Redis 原子预约：多进程共享同一限速器时，全局间隔仍 ≥1s + 抖动
  - Redis 故障 / fork 子进程 / 开关关闭 → 优雅降级到进程内锁，不抛异常
  - 所有东财域请求（行情/资金流/涨停池/新闻/基金档案/搜索）收敛到 em_get
  - 连接复用：不再自建 Session
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import quant_engine.data.sources.eastmoney as em
from quant_engine.data.sources import fund_holdings, news_sources
from quant_engine.data.sources.eastmoney import (
    EastMoneySource,
    _Throttle,
    attach_global_throttle,
    em_get,
)

SESSION = '2026-11-07'


class FakeRedis:
    """模拟 _RESERVE_LUA 的原子预约语义（按 script/keys/args 记录调用）。"""

    def __init__(self):
        self.next_allowed = 0.0
        self.calls: list[tuple] = []
        self.fail = False

    def eval(self, script, numkeys, key, now, gap, ttl):
        self.calls.append((script, numkeys, key, now, gap, ttl))
        if self.fail:
            raise ConnectionError('redis down')
        now, gap = float(now), float(gap)
        allowed = max(now, self.next_allowed + gap)
        self.next_allowed = allowed
        return allowed - now


class FakeResponse:
    def __init__(self, status_code=200, text='{}', content=b'{}'):
        self.status_code = status_code
        self.text = text
        self.content = content


class TestGlobalReservation(unittest.TestCase):
    """Redis 预约式跨进程限速"""

    def test_second_reservation_waits_at_least_one_second(self):
        """核心：连续两次预约，第二次必须等待 ≥1s（截图「至少隔一秒」）"""
        r = FakeRedis()
        t = _Throttle()
        t.attach_redis(r)
        with mock.patch.object(em.time, 'sleep') as sleep:
            t.wait()          # 首次无「上一次请求」→ 直接放行，不 sleep
            t.wait()          # 第二次必须等待到全局允许时刻
        waits = [c.args[0] for c in sleep.call_args_list if c.args]
        self.assertEqual(len(r.calls), 2)
        self.assertEqual(len(waits), 1)      # 只有第二次需要等待
        self.assertGreaterEqual(waits[0], em._Throttle.MIN_INTERVAL)
        # gap = MIN_INTERVAL + jitter，抖动始终叠加在限速之上
        for call in r.calls:
            self.assertGreaterEqual(call[4], em._Throttle.MIN_INTERVAL
                                    + em._Throttle.JITTER_RANGE[0] - 1e-9)
            self.assertLessEqual(call[4], em._Throttle.MIN_INTERVAL
                                 + em._Throttle.JITTER_RANGE[1] + 1e-9)

    def test_reservation_is_monotonic_across_three_callers(self):
        """模拟三服务进程并发：允许时刻严格递增，不出现同时放行"""
        r = FakeRedis()
        allowed = []
        with mock.patch.object(em.time, 'sleep'):
            for _ in range(3):
                t = _Throttle()      # 每个「进程」独立实例，共享同一个 FakeRedis
                t.attach_redis(r)
                t.wait()
                allowed.append(r.next_allowed)
        self.assertEqual(len(set(allowed)), 3)
        self.assertEqual(allowed, sorted(allowed))

    def test_jitter_is_random_not_fixed_cadence(self):
        r = FakeRedis()
        t = _Throttle()
        t.attach_redis(r)
        with mock.patch.object(em.time, 'sleep'):
            for _ in range(8):
                t.wait()
        gaps = [c[4] for c in r.calls]
        self.assertGreater(len({round(g, 6) for g in gaps}), 1)

    def test_redis_failure_falls_back_to_local_lock(self):
        r = FakeRedis()
        r.fail = True
        t = _Throttle()
        t.attach_redis(r)
        with mock.patch.object(em.time, 'sleep') as sleep:
            t.wait()   # 不得抛异常
        self.assertTrue(sleep.called)
        # 降级后走进程内锁；首次请求只需抖动（无「上一次」可等）
        self.assertGreaterEqual(sleep.call_args.args[0],
                                em._Throttle.JITTER_RANGE[0] - 1e-9)

    def test_forked_process_ignores_inherited_redis_client(self):
        """guarded_fetch 用 fork：子进程必须丢弃父进程连接池，降级本地锁"""
        t = _Throttle()
        t.attach_redis(FakeRedis())
        with mock.patch.object(em.os, 'getpid', return_value=t._pid + 1), \
                mock.patch.object(em.time, 'sleep'):
            self.assertIsNone(t._reserve_global())
        self.assertIsNone(t._redis)

    def test_switch_off_never_touches_redis(self):
        r = FakeRedis()
        t = _Throttle()
        t.attach_redis(r)
        with mock.patch.object(em, 'get_settings',
                               return_value=SimpleNamespace(EM_GLOBAL_THROTTLE=False)), \
                mock.patch.object(em.time, 'sleep'):
            t.wait()
        self.assertEqual(r.calls, [])

    def test_default_switch_is_on(self):
        from quant_engine.config import Settings
        self.assertTrue(Settings().EM_GLOBAL_THROTTLE)


class TestAttach(unittest.TestCase):
    def test_disconnected_cache_is_not_attached(self):
        th = _Throttle()
        with mock.patch.object(em, '_THROTTLE', th):
            attach_global_throttle(SimpleNamespace(
                is_connected=False, client=FakeRedis()))
            self.assertIsNone(th._redis)

    def test_connected_cache_is_attached(self):
        th = _Throttle()
        r = FakeRedis()
        with mock.patch.object(em, '_THROTTLE', th):
            attach_global_throttle(SimpleNamespace(is_connected=True, client=r))
            self.assertIs(th._redis, r)

    def test_none_and_broken_cache_are_safe(self):
        with mock.patch.object(em, '_THROTTLE', _Throttle()):
            attach_global_throttle(None)
            attach_global_throttle(object())


class TestSingleEntryPoint(unittest.TestCase):
    """所有东财请求共用一个连接 + 都经过限速"""

    def test_em_get_waits_and_uses_shared_session(self):
        with mock.patch.object(em, '_THROTTLE') as th, \
                mock.patch.object(em._SESSION, 'get',
                                  return_value=FakeResponse()) as get:
            em_get('https://push2.eastmoney.com/x', params={'a': '1'},
                   headers={'Referer': 'https://data.eastmoney.com/'})
        th.wait.assert_called_once()
        self.assertEqual(get.call_args.kwargs['params'], {'a': '1'})
        self.assertEqual(get.call_args.kwargs['headers']['Referer'],
                         'https://data.eastmoney.com/')
        # 其它默认头仍在（合并而非替换）
        self.assertIn('User-Agent', get.call_args.kwargs['headers'])

    def test_legacy_get_delegates_to_em_get(self):
        with mock.patch.object(em, 'em_get', return_value=FakeResponse()) as g:
            EastMoneySource._get('https://push2.eastmoney.com/x', {'p': '1'})
        self.assertTrue(g.called)

    def test_em_pool_uses_the_same_chain(self):
        from quant_engine.data.sources.em_pool import EastMoneyPoolSource
        with mock.patch.object(EastMoneySource, '_get_json',
                               return_value={'data': {'pool': []}}):
            out = EastMoneyPoolSource.get_limit_up_pool('20261107')
        self.assertTrue(out.empty)

    def test_news_source_no_longer_uses_urllib_directly(self):
        """东财快讯走 em_get；raw_fetcher 注入点保留（测试/离线用）"""
        with mock.patch.object(em, 'em_get',
                               return_value=FakeResponse(content=b'{}')) as g:
            self.assertEqual(news_sources.EastMoneyNewsSource._em_get_with_retry(
                'https://np-weblist.eastmoney.com/comm/web/getFastNewsList'), '{}')
        self.assertEqual(g.call_count, 1)
        # 注入点仍在且优先级更高
        with mock.patch.object(em, 'em_get') as g2:
            items = news_sources.EastMoneyNewsSource(columns=['101']).fetch(
                raw_fetcher=lambda url, headers: '{"data":{"fastNewsList":[]}}')
            self.assertEqual(items, [])
            g2.assert_not_called()

    def test_fund_holdings_no_independent_session(self):
        src = fund_holdings.EastMoneyFundHoldingsSource()
        self.assertFalse(hasattr(src, 'session'),
                         '不得再自建 requests.Session（连接复用要求）')

    def test_fund_holdings_requests_go_through_em_get(self):
        src = fund_holdings.EastMoneyFundHoldingsSource()
        with mock.patch.object(fund_holdings, 'em_get',
                               return_value=FakeResponse(text='no apidata')) as g:
            self.assertIsNone(src._request('510300', 2026, 100))
            self.assertEqual(src.get_available_years('510300'), [])
        self.assertGreaterEqual(g.call_count, 2)

    def test_news_search_goes_through_em_get(self):
        from quant_engine.services.monitors.news_service import NewsService
        svc = NewsService()
        body = 'jQuery({"result":{"cmsArticleWebOld":[]}})'
        with mock.patch.object(em, 'em_get',
                               return_value=FakeResponse(text=body)) as g:
            out = svc._search_eastmoney_keyword('600000')
        self.assertEqual(out, [])
        self.assertEqual(g.call_count, 1)

    def test_no_direct_http_call_to_eastmoney_in_source_tree(self):
        """静态护栏：sources 目录下不得出现绕过 em_get 的东财域直连"""
        root = Path(__file__).resolve().parent.parent / 'quant_engine'
        offenders = []
        for p in root.rglob('*.py'):
            if p.name == 'eastmoney.py':
                continue
            for i, line in enumerate(p.read_text(encoding='utf-8').splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith('#') or 'noqa' in stripped:
                    continue
                if 'eastmoney.com' in stripped and any(
                        k in stripped for k in ('requests.get', 'urlopen',
                                                'httpx.Client', 'client.get')):
                    offenders.append(f'{p.name}:{i}: {stripped[:80]}')
        self.assertEqual(offenders, [])


class TestLocalFallbackSemantics(unittest.TestCase):
    """无 Redis（本地/CI）时保持 D53 原有行为，零行为变化"""

    def test_local_lock_enforces_min_interval(self):
        t = _Throttle()
        with mock.patch.object(em.time, 'sleep') as sleep:
            t.wait()
            t.wait()
        waits = [c.args[0] for c in sleep.call_args_list if c.args]
        # 首次只加抖动；第二次（紧接调用）必须补足 ≥1s 间隔
        self.assertGreaterEqual(waits[0], em._Throttle.JITTER_RANGE[0] - 1e-9)
        self.assertGreaterEqual(waits[1], em._Throttle.MIN_INTERVAL
                                + em._Throttle.JITTER_RANGE[0] - 1e-9)

    def test_no_redis_attached_uses_local(self):
        t = _Throttle()
        with mock.patch.object(em.time, 'sleep') as sleep, \
                mock.patch.object(em, 'get_settings',
                                  return_value=SimpleNamespace(EM_GLOBAL_THROTTLE=True)):
            t.wait()
        self.assertTrue(sleep.called)


if __name__ == '__main__':
    unittest.main(verbosity=2)
