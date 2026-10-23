"""财经新闻多源数据层

已实测可用的免费源（2026-10-18 本地实测，ECS 同网段应一致）：
- 东方财富快讯（getFastNewsList，fastColumn 101重要/102全球/103公司）
- 同花顺快讯（tapp/news/push/stock）
- 新浪财经7x24直播（zhibo feed）

配置门控源（需凭据，默认关闭，见 config.py NEWS_* 配置）：
- X(Twitter) API v2 recent search（需 X_BEARER_TOKEN）
- 通达信 MCP wenda_news_query（需 NEWS_TDX_MCP_ENDPOINT + NEWS_TDX_MCP_API_KEY）
- 通用 RSS/Atom（NEWS_RSS_FEEDS 配置，如 ["华尔街见闻|https://..."]）

实测不可用（2026-10-18，勿再试）：
- 财联社电报 nodeapi/updateTelegraphList（404，新版需签名）
- 金十数据 flash-api（502）
- 东财旧接口 newsapi.eastmoney.com/kuaixun（404）
- 36氪/和讯/网易 RSS（返回 HTML 非 XML）
"""
from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from xml.etree import ElementTree

import pandas as pd
from loguru import logger

DEFAULT_TIMEOUT = 15
DEFAULT_RETRY = 2
DEFAULT_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")


@dataclass
class NewsItem:
    """统一新闻结构（published_at 为服务器本地时间口径）"""
    source: str                 # 东财快讯/同花顺/新浪财经/RSS/X/通达信
    source_rank: int            # 去重保留优先级，越小越高
    item_id: str                # 源内唯一ID
    title: str
    content: str = ""           # 摘要/正文
    url: str = ""
    published_at: datetime | None = None
    tags: list[str] = field(default_factory=list)      # 源标签（关键词/板块词）
    stocks: list[str] = field(default_factory=list)    # 关联股票名称

    def to_dict(self) -> dict:
        return {
            'source': self.source,
            'item_id': self.item_id,
            'title': self.title,
            'content': self.content,
            'url': self.url,
            'published_at': self.published_at.isoformat() if self.published_at else None,
            'tags': self.tags,
            'stocks': self.stocks,
        }


def _http_get(url: str, headers: dict | None = None, timeout: int = DEFAULT_TIMEOUT,
              retry: int = DEFAULT_RETRY) -> str | None:
    """带重试的GET，失败返回None（由调用方降级）"""
    hdr = {'User-Agent': DEFAULT_UA}
    if headers:
        hdr.update(headers)
    last_err = None
    for attempt in range(1, retry + 1):
        try:
            req = urllib.request.Request(url, headers=hdr)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode('utf-8', 'replace')
        except Exception as e:
            last_err = e
            logger.debug(f"HTTP GET failed ({attempt}/{retry}) {url[:80]}: {e}")
            if attempt < retry:
                time.sleep(1.0 * attempt)
    logger.warning(f"HTTP GET failed after {retry} tries: {url[:80]} ({last_err})")
    return None


def _http_post_json(url: str, payload: dict, headers: dict | None = None,
                    timeout: int = DEFAULT_TIMEOUT, retry: int = DEFAULT_RETRY) -> dict | None:
    hdr = {'User-Agent': DEFAULT_UA, 'Content-Type': 'application/json'}
    if headers:
        hdr.update(headers)
    last_err = None
    for attempt in range(1, retry + 1):
        try:
            req = urllib.request.Request(
                url, data=json.dumps(payload).encode('utf-8'), headers=hdr)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8', 'replace'))
        except Exception as e:
            last_err = e
            logger.debug(f"HTTP POST failed ({attempt}/{retry}) {url[:80]}: {e}")
            if attempt < retry:
                time.sleep(1.0 * attempt)
    logger.warning(f"HTTP POST failed after {retry} tries: {url[:80]} ({last_err})")
    return None


def strip_html(text: str) -> str:
    """去HTML标签与实体，压缩空白"""
    if not text:
        return ''
    text = re.sub(r'<[^>]+>', '', text)
    text = (text.replace('&nbsp;', ' ').replace('&amp;', '&')
                .replace('&lt;', '<').replace('&gt;', '>')
                .replace('&quot;', '"').replace('&#39;', "'"))
    return re.sub(r'\s+', ' ', text).strip()


def parse_datetime(value) -> datetime | None:
    """多格式时间解析：datetime / unix时间戳(秒) / 常见字符串"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(int(value))
        except (ValueError, OSError, OverflowError):
            return None
    text = str(value).strip()
    # unix 时间戳字符串
    if re.fullmatch(r'\d{9,11}', text):
        try:
            return datetime.fromtimestamp(int(text))
        except (ValueError, OSError, OverflowError):
            return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d', '%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d %H:%M', '%Y/%m/%d'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    # pandas 兜底
    try:
        return pd.to_datetime(text).to_pydatetime()
    except Exception:
        return None


class EastMoneyNewsSource:
    """东方财富快讯（np-weblist getFastNewsList）

    注意：接口必须带 req_trace 参数（否则报 Required String parameter），
    行数据键名是 fastNewsList（非 fastNews）。
    栏目：101=重要/大盘策略 102=全球财经 103=公司新闻
    """

    NAME = '东财快讯'
    RANK = 1
    BASE = 'https://np-weblist.eastmoney.com/comm/web/getFastNewsList'

    def __init__(self, columns: list[str] | None = None, page_size: int = 50):
        self.columns = columns or ['101', '102', '103']
        self.page_size = page_size

    def fetch(self, raw_fetcher=None) -> list[NewsItem]:
        """raw_fetcher: 测试注入 (url, headers) -> str"""
        items: list[NewsItem] = []
        for col in self.columns:
            url = (f"{self.BASE}?client=web&biz=web_724&fastColumn={col}"
                   f"&sortEnd=&pageSize={self.page_size}&req_trace=nx{int(time.time()*1000)}")
            raw = raw_fetcher(url, {}) if raw_fetcher else _http_get(url)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning(f"{self.NAME} column {col}: invalid JSON")
                continue
            rows = ((data.get('data') or {}).get('fastNewsList')) or []
            for row in rows:
                code = str(row.get('code') or '').strip()
                title = (row.get('title') or '').strip()
                if not code or not title:
                    continue
                items.append(NewsItem(
                    source=self.NAME,
                    source_rank=self.RANK,
                    item_id=code,
                    title=title,
                    content=strip_html(row.get('summary') or ''),
                    url=f"https://finance.eastmoney.com/a/{code}.html",
                    published_at=parse_datetime(row.get('showTime')),
                ))
        logger.info(f"{self.NAME}: {len(items)} items")
        return items


class TongHuaShunNewsSource:
    """同花顺快讯（tapp/news/push/stock）"""

    NAME = '同花顺'
    RANK = 2
    URL = 'https://news.10jqka.com.cn/tapp/news/push/stock/?page=1&tag=&track=web&pagesize={size}'

    def __init__(self, page_size: int = 50):
        self.page_size = page_size

    def fetch(self, raw_fetcher=None) -> list[NewsItem]:
        url = self.URL.format(size=self.page_size)
        raw = raw_fetcher(url, {'Referer': 'https://news.10jqka.com.cn/'}) \
            if raw_fetcher else _http_get(url, {'Referer': 'https://news.10jqka.com.cn/'})
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"{self.NAME}: invalid JSON")
            return []
        items: list[NewsItem] = []
        for row in ((data.get('data') or {}).get('list')) or []:
            title = (row.get('title') or '').strip()
            item_id = str(row.get('id') or '')
            if not title or not item_id:
                continue
            tags = [t.get('name') for t in (row.get('tagInfo') or [])
                    if isinstance(t, dict) and t.get('name')]
            stocks = [s.get('name') for s in (row.get('stock') or [])
                      if isinstance(s, dict) and s.get('name')]
            items.append(NewsItem(
                source=self.NAME,
                source_rank=self.RANK,
                item_id=item_id,
                title=title,
                content=strip_html(row.get('digest') or ''),
                url=row.get('url') or '',
                published_at=parse_datetime(row.get('ctime')),
                tags=tags,
                stocks=stocks,
            ))
        logger.info(f"{self.NAME}: {len(items)} items")
        return items


class SinaLiveNewsSource:
    """新浪财经7x24直播（zhibo feed，zhibo_id=152 全球财经）"""

    NAME = '新浪财经'
    RANK = 3
    URL = ('https://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size={size}'
           '&zhibo_id=152&tag_id=0&dire=f&dpc=1&pagesize=20')

    def __init__(self, page_size: int = 50):
        self.page_size = page_size

    @staticmethod
    def _split_title(rich_text: str) -> tuple:
        """新浪富文本以【标题】开头，拆出标题与正文"""
        text = strip_html(rich_text)
        m = re.match(r'^【([^】]{4,60})】\s*(.*)$', text, re.DOTALL)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return text[:40].strip(), text

    def fetch(self, raw_fetcher=None) -> list[NewsItem]:
        url = self.URL.format(size=self.page_size)
        raw = raw_fetcher(url, {}) if raw_fetcher else _http_get(url)
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"{self.NAME}: invalid JSON")
            return []
        feed = (((data.get('result') or {}).get('data') or {})
                .get('feed') or {})
        items: list[NewsItem] = []
        for row in feed.get('list') or []:
            rich = row.get('rich_text') or ''
            title, content = self._split_title(rich)
            if not title:
                continue
            tags: list[str] = []
            tag_raw = row.get('tag')
            if isinstance(tag_raw, str) and tag_raw.startswith('['):
                try:
                    tags = [t.get('name') for t in json.loads(tag_raw)
                            if isinstance(t, dict) and t.get('name')]
                except (json.JSONDecodeError, TypeError):
                    tags = []
            elif isinstance(tag_raw, list):
                tags = [t.get('name') for t in tag_raw
                        if isinstance(t, dict) and t.get('name')]
            items.append(NewsItem(
                source=self.NAME,
                source_rank=self.RANK,
                item_id=str(row.get('id') or row.get('source_content_id') or ''),
                title=title,
                content=content,
                url=row.get('docurl') or '',
                published_at=parse_datetime(row.get('create_time')),
                tags=tags,
            ))
        logger.info(f"{self.NAME}: {len(items)} items")
        return items


class RssNewsSource:
    """通用 RSS/Atom 源（NEWS_RSS_FEEDS 配置注入）

    配置格式：NEWS_RSS_FEEDS = "名称1|https://feed1.xml,名称2|https://feed2.xml"
    """

    RANK = 4

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url

    @staticmethod
    def _local(tag: str) -> str:
        return tag.split('}')[-1] if '}' in tag else tag

    def fetch(self, raw_fetcher=None) -> list[NewsItem]:
        raw = raw_fetcher(self.url, {}) if raw_fetcher else _http_get(self.url)
        if not raw:
            return []
        items: list[NewsItem] = []
        try:
            root = ElementTree.fromstring(raw)
        except ElementTree.ParseError as e:
            logger.warning(f"RSS {self.name}: parse error {e}")
            return []
        # RSS 2.0: channel/item；Atom: entry
        entries = [el for el in root.iter()
                   if self._local(el.tag) in ('item', 'entry')]
        for el in entries:
            getter = {}
            for child in el:
                getter.setdefault(self._local(child.tag), child)
            title = strip_html(getter.get('title').text or '') \
                if getter.get('title') is not None and getter.get('title').text else ''
            if not title:
                continue
            link = ''
            link_el = getter.get('link')
            if link_el is not None:
                link = link_el.get('href') or (link_el.text or '').strip()
            desc_el = None
            for key in ('description', 'summary', 'content'):
                if getter.get(key) is not None:
                    desc_el = getter[key]
                    break
            desc = strip_html(desc_el.text or '') if desc_el is not None else ''
            pub = None
            for key in ('pubDate', 'published', 'updated', 'date'):
                if key in getter and getter[key].text:
                    pub = parse_datetime(getter[key].text)
                    if pub:
                        break
            guid = getter.get('guid') or getter.get('id')
            item_id = (guid.text or '').strip() if guid is not None and guid.text else link
            items.append(NewsItem(
                source=f"RSS:{self.name}",
                source_rank=self.RANK,
                item_id=item_id or title,
                title=title,
                content=desc[:500],
                url=link,
                published_at=pub,
            ))
        logger.info(f"RSS:{self.name}: {len(items)} items")
        return items


class XTwitterNewsSource:
    """X(Twitter) 财经快讯（API v2 recent search）

    需要 X_BEARER_TOKEN（X Developer 账号申请，免费档 1500条/月只读）。
    查询词示例：$SPX OR #stocks OR #finance -is:retweet lang:en
    默认关闭；启用前先在 X 控制台确认账号有 recent search 权限。
    """

    NAME = 'X'
    RANK = 5
    URL = 'https://api.x.com/2/tweets/search/recent'

    def __init__(self, bearer_token: str, query: str | None = None, max_results: int = 50):
        self.bearer_token = bearer_token
        self.query = query or '$SPX OR $DJIA OR $NDX OR #stocks -is:retweet'
        self.max_results = min(max_results, 100)

    def fetch(self, raw_fetcher=None) -> list[NewsItem]:
        if not self.bearer_token:
            return []
        params = {
            'query': self.query,
            'max_results': self.max_results,
            'tweet.fields': 'created_at,text',
        }
        url = f"{self.URL}?{urllib.parse.urlencode(params)}"
        headers = {'Authorization': f'Bearer {self.bearer_token}'}
        raw = raw_fetcher(url, headers) if raw_fetcher else _http_get(url, headers)
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"{self.NAME}: invalid JSON")
            return []
        items: list[NewsItem] = []
        for row in data.get('data') or []:
            text = (row.get('text') or '').strip()
            if not text:
                continue
            tid = str(row.get('id') or '')
            items.append(NewsItem(
                source=self.NAME,
                source_rank=self.RANK,
                item_id=tid,
                title=text[:50].replace('\n', ' '),
                content=text,
                url=f"https://x.com/i/status/{tid}" if tid else '',
                published_at=parse_datetime(row.get('created_at')),
            ))
        logger.info(f"{self.NAME}: {len(items)} items")
        return items


class TongDaXinNewsSource:
    """通达信 MCP 新闻（wenda_news_query）

    需要 NEWS_TDX_MCP_ENDPOINT（MCP HTTP 地址，如 https://xxx/mcp）
    与 NEWS_TDX_MCP_API_KEY。未配置时自动关闭。
    注意：此适配器为配置后启用，首次启用需人工核对返回结构
    （不同 MCP 部署的返回包装可能不同）。
    """

    NAME = '通达信'
    RANK = 6

    def __init__(self, endpoint: str, api_key: str, query: str | None = None):
        self.endpoint = endpoint
        self.api_key = api_key
        self.query = query or '今日财经要闻 早间快讯'

    def fetch(self, raw_fetcher=None) -> list[NewsItem]:
        if not self.endpoint or not self.api_key:
            return []
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'tools/call',
            'params': {
                'name': 'wenda_news_query',
                'arguments': {'query': self.query, 'limit': 30},
            },
        }
        headers = {'Authorization': f'Bearer {self.api_key}'}
        data = raw_fetcher(self.endpoint, headers, payload) \
            if raw_fetcher else _http_post_json(self.endpoint, payload, headers)
        if not data:
            return []
        # MCP 返回 result.content[0].text（可能是 JSON 或纯文本）
        try:
            text = (data.get('result') or {}).get('content', [{}])[0].get('text', '')
        except (AttributeError, IndexError, TypeError):
            text = ''
        if not text:
            logger.warning(f"{self.NAME}: empty MCP response")
            return []
        rows = None
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                rows = parsed
            elif isinstance(parsed, dict):
                rows = parsed.get('data') or parsed.get('list') or parsed.get('news') or []
        except json.JSONDecodeError:
            rows = None
        items: list[NewsItem] = []
        if rows:
            for i, row in enumerate(rows):
                title = str(row.get('title') or row.get('标题') or '').strip()
                if not title:
                    continue
                items.append(NewsItem(
                    source=self.NAME,
                    source_rank=self.RANK,
                    item_id=str(row.get('id') or row.get('url') or i),
                    title=title,
                    content=str(row.get('content') or row.get('summary') or row.get('摘要') or ''),
                    url=str(row.get('url') or row.get('link') or ''),
                    published_at=parse_datetime(
                        row.get('time') or row.get('publish_time') or row.get('发布时间')),
                ))
        else:
            # 纯文本兜底：按行拆分，"时间 标题" 或 "标题"
            for line in text.splitlines():
                line = line.strip().lstrip('-*• ')
                if len(line) < 8:
                    continue
                m = re.match(r'^(\d{1,2}[:：]\d{2})\s+(.+)$', line)
                title = m.group(2) if m else line
                items.append(NewsItem(
                    source=self.NAME,
                    source_rank=self.RANK,
                    item_id=line[:64],
                    title=title[:80],
                    content=line,
                ))
        logger.info(f"{self.NAME}: {len(items)} items")
        return items


class NewsSourceManager:
    """新闻源管理器：聚合所有启用源，单源失败自动降级"""

    def __init__(self, settings=None):
        self.sources = []
        try:
            if settings is None:
                from ..config import get_settings
                settings = get_settings()
        except Exception as e:
            logger.warning(f"News settings load failed: {e}")
            settings = None

        self.sources.append(EastMoneyNewsSource())
        self.sources.append(TongHuaShunNewsSource())
        self.sources.append(SinaLiveNewsSource())

        if settings is not None:
            # RSS 源（配置注入）
            for spec in getattr(settings, 'NEWS_RSS_FEEDS_LIST', []) or []:
                if '|' in spec:
                    name, url = spec.split('|', 1)
                    self.sources.append(RssNewsSource(name.strip(), url.strip()))
            # X(Twitter)（需 Bearer Token）
            x_token = getattr(settings, 'NEWS_X_BEARER_TOKEN', '')
            if x_token:
                self.sources.append(XTwitterNewsSource(
                    x_token, getattr(settings, 'NEWS_X_QUERY', None)))
            # 通达信 MCP（需 endpoint + key）
            tdx_ep = getattr(settings, 'NEWS_TDX_MCP_ENDPOINT', '')
            tdx_key = getattr(settings, 'NEWS_TDX_MCP_API_KEY', '')
            if tdx_ep and tdx_key:
                self.sources.append(TongDaXinNewsSource(tdx_ep, tdx_key))

        logger.info(f"NewsSourceManager: {len(self.sources)} sources "
                    f"({[s.NAME if hasattr(s, 'NAME') else s.name for s in self.sources]})")

    def fetch_all(self) -> list[NewsItem]:
        """抓取所有源，单源异常不影响其他源"""
        items: list[NewsItem] = []
        for src in self.sources:
            try:
                items.extend(src.fetch())
            except Exception as e:
                logger.error(f"News source {getattr(src, 'NAME', src)} failed: {e}")
        # 按时间倒序（无时间的排最后）
        items.sort(key=lambda x: x.published_at or datetime.min, reverse=True)
        return items
