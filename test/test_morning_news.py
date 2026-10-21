"""早间财经新闻监控单元测试（离线，fixture=真实接口响应）

覆盖：三源解析、时间解析、标题规范化、去重（精确/近似/跨次/源优先级）、
板块分类与上限、分组排序、摘要构建与限量、文案格式、预览模式无副作用。
运行：cd test && ../coding/.venv/bin/python test_morning_news.py
"""
import json
import re
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from quant_engine.data.sources.news_sources import (  # noqa: E402
    NewsItem, EastMoneyNewsSource, TongHuaShunNewsSource, SinaLiveNewsSource,
    RssNewsSource, strip_html, parse_datetime,
)
from quant_engine.services.monitors.news_monitor import (  # noqa: E402
    MorningNewsMonitor, normalize_title, title_hash, SECTOR_KEYWORDS,
    UNMATCHED_SECTOR,
)

FIXTURES = ROOT / 'test' / 'fixtures'


def _load(name):
    with open(FIXTURES / name, encoding='utf-8') as f:
        return f.read()


def _stub_fetcher(url, headers=None, payload=None):
    """按URL路由到fixture的桩"""
    if 'np-weblist' in url:
        col = re.search(r'fastColumn=(\d+)', url).group(1)
        return _load(f'news_em_{col}.json')
    if '10jqka' in url:
        return _load('news_ths.json')
    if 'zhibo.sina' in url:
        return _load('news_sina.json')
    return None


class StubCache:
    """内存缓存桩（模拟 RedisCache get/set 语义）"""

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ttl=3600):
        self.store[key] = value
        return True


class StubDatacenter:
    def __init__(self):
        self.cache = StubCache()


def _make_monitor(**kwargs):
    m = MorningNewsMonitor(StubDatacenter())
    for k, v in kwargs.items():
        setattr(m, k, v)
    return m


class TestNewsSourceParsing(unittest.TestCase):
    """三源解析测试（fixture=2026-10-18真实响应）"""

    def test_eastmoney_parsing(self):
        items = EastMoneyNewsSource().fetch(raw_fetcher=_stub_fetcher)
        self.assertEqual(len(items), 90)  # 3栏目×30
        first = items[0]
        self.assertEqual(first.source, '东财快讯')
        self.assertTrue(first.item_id)
        self.assertIsInstance(first.published_at, datetime)
        self.assertTrue(first.title)
        self.assertTrue(first.url.startswith('https://finance.eastmoney.com/a/'))
        # 101栏目首条时间 19:00（固定fixture内容）
        self.assertEqual(first.published_at.strftime('%Y-%m-%d %H:%M:%S'),
                         '2026-10-18 19:00:14')

    def test_eastmoney_rejects_missing_req_trace(self):
        # 不带req_trace时接口返回错误体，解析结果应为空而非崩溃
        def bad_fetcher(url, headers=None):
            return json.dumps({'code': 0, 'message': 'req_trace missing', 'data': None})
        items = EastMoneyNewsSource().fetch(raw_fetcher=bad_fetcher)
        self.assertEqual(items, [])

    def test_ths_parsing(self):
        items = TongHuaShunNewsSource().fetch(raw_fetcher=_stub_fetcher)
        self.assertEqual(len(items), 30)
        first = items[0]
        self.assertEqual(first.source, '同花顺')
        self.assertEqual(first.title, '全国秋粮收获进度过七成')
        self.assertIsInstance(first.published_at, datetime)
        self.assertTrue(first.url.startswith('https://news.10jqka.com.cn/'))
        self.assertIn('玉米', first.tags)  # tagInfo提取
        self.assertTrue(first.content)

    def test_sina_parsing(self):
        items = SinaLiveNewsSource().fetch(raw_fetcher=_stub_fetcher)
        self.assertEqual(len(items), 30)
        first = items[0]
        self.assertEqual(first.source, '新浪财经')
        # rich_text 的【标题】前缀被正确拆出
        self.assertEqual(first.title, '全国秋粮收获进度过七成')
        self.assertFalse(first.title.startswith('【'))
        self.assertTrue(first.url.startswith('https://finance.sina.cn/'))
        self.assertIsInstance(first.published_at, datetime)
        # 正文不含【】前缀
        self.assertFalse(first.content.startswith('【'))

    def test_sina_title_without_brackets(self):
        # 无【】前缀时退化为前40字
        item = SinaLiveNewsSource._split_title('这是一条没有括号前缀的快讯内容' * 3)
        self.assertEqual(item[0], '这是一条没有括号前缀的快讯内容这是一条没有括号前缀的快讯内容这是一条没有括号前缀的快讯内容'[:40])
        self.assertTrue(item[1])

    def test_strip_html(self):
        self.assertEqual(strip_html('<b>粗体</b>&nbsp;文本'), '粗体 文本')
        self.assertEqual(strip_html(' 多个   空白 '), '多个 空白')

    def test_parse_datetime_formats(self):
        self.assertEqual(parse_datetime('2026-10-18 22:16:12'),
                         datetime(2026, 10, 18, 22, 16, 12))
        self.assertEqual(parse_datetime('2026-10-18 22:16'),
                         datetime(2026, 10, 18, 22, 16))
        self.assertEqual(parse_datetime('1792333164'),
                         datetime.fromtimestamp(1792333164))
        self.assertEqual(parse_datetime(1792333164),
                         datetime.fromtimestamp(1792333164))
        self.assertEqual(parse_datetime('2026-10-18'),
                         datetime(2026, 10, 18))
        self.assertIsNone(parse_datetime(''))
        self.assertIsNone(parse_datetime('不是时间'))

    def test_rss_source_parsing(self):
        rss = """<?xml version="1.0"?>
        <rss><channel><item>
          <title>测试新闻标题</title>
          <link>https://example.com/a</link>
          <description>&lt;p&gt;摘要内容&lt;/p&gt;</description>
          <pubDate>Sun, 18 Oct 2026 09:15:00 +0800</pubDate>
        </item></channel></rss>"""
        items = RssNewsSource('测试源', 'https://example.com/rss').fetch(
            raw_fetcher=lambda url, headers=None: rss)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].title, '测试新闻标题')
        self.assertEqual(items[0].url, 'https://example.com/a')
        self.assertEqual(items[0].content, '摘要内容')
        self.assertIsInstance(items[0].published_at, datetime)


class TestTitleNormalization(unittest.TestCase):

    def test_normalize_removes_noise(self):
        self.assertEqual(normalize_title('【新浪】A股大涨：沪指涨2.5%！'),
                         normalize_title('a股大涨沪指涨25'))
        self.assertEqual(normalize_title('ABC科技'), normalize_title('abc科技'))
        self.assertEqual(normalize_title('   spaced   out  '), 'spacedout')

    def test_hash_stable_and_distinct(self):
        self.assertEqual(title_hash('测试标题'), title_hash('【前缀】测试标题'))
        self.assertNotEqual(title_hash('完全不同的标题'), title_hash('另一个标题'))


class TestDedup(unittest.TestCase):

    def _item(self, title, source='东财快讯', rank=1, minutes_ago=10):
        return NewsItem(source=source, source_rank=rank, item_id=title,
                        title=title, published_at=datetime.now() - timedelta(minutes=minutes_ago))

    def test_exact_dup_keeps_priority_source(self):
        m = _make_monitor()
        items = [
            self._item('央行宣布降准0.5个百分点', source='新浪财经', rank=3),
            self._item('央行宣布降准0.5个百分点', source='东财快讯', rank=1),
            self._item('央行宣布降准0.5个百分点', source='同花顺', rank=2),
        ]
        kept, dropped = m.dedup(items)
        self.assertEqual(len(kept), 1)
        self.assertEqual(dropped, 2)
        self.assertEqual(kept[0].source, '东财快讯')  # 保留最高优先级源

    def test_near_dup_detected(self):
        m = _make_monitor()
        items = [
            self._item('英伟达发布新一代AI芯片Rubin平台'),
            self._item('英伟达发布新一代AI芯片Rubin平台，性能提升3倍'),  # 加尾注
            self._item('贵州茅台发布三季报净利润同比增长15%'),  # 真不同
        ]
        kept, dropped = m.dedup(items)
        self.assertEqual(len(kept), 2)
        self.assertEqual(dropped, 1)

    def test_cross_run_dedup_via_state(self):
        m = _make_monitor()
        items = [self._item('跨次推送去重测试新闻甲')]
        kept, _ = m.dedup(items)
        self.assertEqual(len(kept), 1)
        # 第二次推送同样内容 -> 命中跨次指纹
        kept2, dropped2 = m.dedup([self._item('跨次推送去重测试新闻甲')])
        self.assertEqual(len(kept2), 0)
        self.assertEqual(dropped2, 1)
        # 指纹已写入缓存桩
        self.assertTrue(m._cache.store.get('news:seen'))

    def test_preview_mode_no_state_write(self):
        m = _make_monitor()
        before = dict(m._cache.store)
        m.dedup([self._item('预览模式测试新闻')], persist=False)
        self.assertEqual(m._cache.store, before)

    def test_empty_title_dropped(self):
        m = _make_monitor()
        kept, dropped = m.dedup([self._item('')])
        self.assertEqual(len(kept), 0)
        self.assertEqual(dropped, 1)


class TestClassification(unittest.TestCase):

    def _monitor(self):
        return _make_monitor()

    def test_single_sector_match(self):
        m = self._monitor()
        item = NewsItem(source='东财快讯', source_rank=1, item_id='1',
                        title='某公司光刻机研发取得突破',
                        content='半导体设备国产化加速')
        self.assertEqual(m.classify(item), ['半导体'])

    def test_multi_sector_capped_at_3(self):
        m = self._monitor()
        # 同时命中新能源车/有色金属/宏观政策/全球市场（4个）
        item = NewsItem(source='东财快讯', source_rank=1, item_id='1',
                        title='碳酸锂价格上涨 新能源车成本承压 政策预计出台',
                        content='美联储关注中国锂电产业链')
        sectors = m.classify(item)
        self.assertLessEqual(len(sectors), 3)
        self.assertIn('新能源车', sectors)

    def test_unmatched_goes_to_general(self):
        m = self._monitor()
        item = NewsItem(source='东财快讯', source_rank=1, item_id='1',
                        title='希腊一架小型飞机坠毁 造成2人死亡')
        self.assertEqual(m.classify(item), [])

    def test_no_false_positive_on_generic_words(self):
        """全球/国际/海外等泛词已从全球市场剔除"""
        m = self._monitor()
        item = NewsItem(source='同花顺', source_rank=2, item_id='1',
                        title='全国秋粮收获进度过七成',
                        content='在全球是大面积高产的领先水平',
                        tags=['伊犁哈萨克自治州', '玉米'])
        sectors = m.classify(item)
        self.assertNotIn('全球市场', sectors)
        self.assertIn('农业养殖', sectors)

    def test_group_ordering_and_general_last(self):
        m = self._monitor()
        now = datetime.now()
        items = [
            NewsItem('东财快讯', 1, 'a', '美联储加息预期升温', published_at=now),
            NewsItem('东财快讯', 1, 'b', '半导体设备订单增长', published_at=now),
            NewsItem('东财快讯', 1, 'c', '芯片研发进展顺利', published_at=now),
            NewsItem('东财快讯', 1, 'd', '希腊小型飞机坠毁', published_at=now),
        ]
        groups = m.group_by_sector(items)
        names = list(groups.keys())
        self.assertEqual(names[0], '半导体')       # 2条最多排最前
        self.assertEqual(names[-1], UNMATCHED_SECTOR)  # 综合要闻恒最后


class TestDigestAndMessage(unittest.TestCase):

    def _items(self, n):
        return [NewsItem('东财快讯', 1, str(i), f'半导体行业新闻第{i}条',
                         url=f'https://finance.eastmoney.com/a/20261018{i:06d}.html',
                         published_at=datetime.now() - timedelta(minutes=i))
                for i in range(n)]

    def test_digest_caps_sectors_and_items(self):
        m = _make_monitor(MAX_SECTORS=2, MAX_PER_SECTOR=3)
        items = [NewsItem('东财快讯', 1, str(i), f'半导体新闻{i}',
                          published_at=datetime.now()) for i in range(10)]
        items += [NewsItem('东财快讯', 1, f'g{i}', '美联储议息会议结果公布',
                           published_at=datetime.now()) for i in range(7)]
        items += [NewsItem('东财快讯', 1, f'u{i}', '希腊发生地震',
                           published_at=datetime.now()) for i in range(5)]
        digest = m.build_digest(items, 0, datetime(2026, 10, 17, 15, 5),
                                datetime.now(), True)
        self.assertLessEqual(len(digest['sectors']), 2)
        for entry in digest['sectors']:
            self.assertLessEqual(entry['shown'], 3)
            self.assertTrue(entry['truncated'])
        self.assertEqual(digest['sector_count'], 3)
        self.assertGreater(digest['hidden_sectors'], 0)
        self.assertGreater(digest['hidden_items'], 0)

    def test_digest_source_breakdown(self):
        m = _make_monitor()
        items = [
            NewsItem('东财快讯', 1, 'a', '半导体新闻甲', published_at=datetime.now()),
            NewsItem('同花顺', 2, 'b', '半导体新闻乙', published_at=datetime.now()),
            NewsItem('同花顺', 2, 'c', '半导体新闻丙', published_at=datetime.now()),
        ]
        digest = m.build_digest(items, 0, datetime(2026, 10, 17, 15, 5),
                                datetime.now(), True)
        self.assertEqual(digest['sources'], {'东财快讯': 1, '同花顺': 2})

    def test_format_message_structure(self):
        m = _make_monitor(MAX_SECTORS=8, MAX_PER_SECTOR=6)
        items = self._items(8)
        digest = m.build_digest(items, 3, datetime(2026, 10, 17, 15, 5),
                                datetime(2026, 10, 18, 9, 15), True)
        msg = m.format_message(digest)
        self.assertIn('时间窗', msg)
        self.assertIn('10-17 15:05', msg)
        self.assertIn('新增 8 条', msg)
        self.assertIn('去重剔除 3 条', msg)
        self.assertIn('### 半导体 · 8', msg)
        self.assertIn('[东财快讯]', msg)
        self.assertIn('详情](https://finance.eastmoney.com/a/', msg)

    def test_format_message_length_cap(self):
        m = _make_monitor(MAX_SECTORS=8, MAX_PER_SECTOR=6)
        # 8个不同板块×6条全部展示，构造超长标题+URL确保超过15000字符上限
        sector_kws = ['半导体', '新能源汽车', '光伏', '医药', '白酒', '军工',
                      '银行', '煤炭']
        items = []
        for si, kw in enumerate(sector_kws):
            for i in range(6):
                items.append(NewsItem(
                    '东财快讯', 1, f'{si}{i}', f'{kw}设备' + '长长长' * 25 + f'新闻{i}',
                    url='https://example.com/' + 'x' * 250,
                    published_at=datetime.now()))
        digest = m.build_digest(items, 0, datetime(2026, 10, 17, 15, 5),
                                datetime.now(), True)
        self.assertEqual(len(digest['sectors']), 8)  # 8板块全展示
        msg = m.format_message(digest)
        self.assertLessEqual(len(msg), m.MAX_MESSAGE_CHARS)
        self.assertIn('截断', msg)

    def test_collect_digest_preview_with_fixtures(self):
        """预览模式：fixture三源 → 有摘要且不写状态"""
        m = _make_monitor()
        m.source_manager = _make_source_manager_stub()
        # 冻结时间窗起点：fixture 新闻发布于 2026-10-18 14:45-22:18，而
        # 回看窗默认 now-20h，随墙钟推移会把 fixture 全部滤光（时间炸弹，
        # 2026-10-20 实测复现）。窗口语义是 published_at>=起点，起点须早于
        # fixture 最早一条，固定之使用例与创建时间解耦。
        m._window_start = lambda: (datetime(2026, 10, 18, 14, 0), False)
        digest = m.collect_digest()
        self.assertIsNotNone(digest)
        self.assertGreater(digest['total'], 0)
        self.assertGreater(digest['dedup_dropped'], 0)  # 跨源重复被剔除
        # 状态未写
        self.assertNotIn('news:seen', m._cache.store)
        self.assertNotIn('news:latest', m._cache.store)
        self.assertNotIn('news:last_push_at', m._cache.store)


class _StubSourceManager:
    """用 fixture 桩替换真实抓取"""

    def fetch_all(self):
        items = []
        items += EastMoneyNewsSource().fetch(raw_fetcher=_stub_fetcher)
        items += TongHuaShunNewsSource().fetch(raw_fetcher=_stub_fetcher)
        items += SinaLiveNewsSource().fetch(raw_fetcher=_stub_fetcher)
        return items


def _make_source_manager_stub():
    return _StubSourceManager()


class TestCheckAndPush(unittest.TestCase):

    def test_no_push_when_no_new_items(self):
        m = _make_monitor()
        m.source_manager = _StubSourceManager()
        # 上次推送时间设为“现在” → 时间窗内无新增
        m._cache.store['news:last_push_at'] = datetime.now().isoformat()
        digest = m.check_and_push(notifier=None)
        self.assertIsNone(digest)
        # 窗口推进但仍未写 latest
        self.assertNotIn('news:latest', m._cache.store)

    def test_push_with_stub_notifier(self):
        m = _make_monitor()
        m.source_manager = _StubSourceManager()
        # 宽窗口（回看7天）确保fixture内容都在窗口内
        # 2026-10-20 修：原48h窗口随fixture（10-18）老化而失效（windowed=0→digest None，
        # 与D26"测试时间炸弹"同类）；fixture 是静态桩，放宽到7天不影响断言语义
        m._cache.store['news:last_push_at'] = \
            (datetime.now() - timedelta(days=7)).isoformat()

        sent = []

        class StubNotifier:
            def send_custom_message(self, title, content):
                sent.append((title, content))

        digest = m.check_and_push(notifier=StubNotifier())
        self.assertIsNotNone(digest)
        self.assertGreater(digest['total'], 0)
        self.assertEqual(len(sent), 1)
        title, msg = sent[0]
        self.assertIn('早间财经要闻', title)
        self.assertIn('###', msg)
        # 状态已写
        self.assertIn('news:latest', m._cache.store)
        self.assertIn('news:sectors', m._cache.store)
        self.assertIn('news:last_push_at', m._cache.store)
        # 二次调用全部命中跨次指纹 → 无新增
        digest2 = m.check_and_push(notifier=StubNotifier())
        self.assertIsNone(digest2)
        self.assertEqual(len(sent), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
