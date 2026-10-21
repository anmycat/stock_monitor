"""早间财经新闻监控（每日9:15按板块推送）

链路：多源抓取 → 时间窗过滤（自上次推送起）→ 去重（精确+近似+源优先级）
→ 板块关键词分类 → 分组限量 → 钉钉等渠道推送。

去重设计（用户要求2）：
1. 规范化标题（去空白/标点/【】前缀/大小写）SHA1 精确去重，跨推送保留48h
2. 标题近似去重（difflib 相似度 ≥ 0.85），跨源改写在所难免（同花顺/新浪
   经常报道同一事件）
3. 同一事件多源报道时保留优先级最高的源（东财>同花顺>新浪>RSS>X>通达信）

板块分类（用户要求4）：
关键词字典 + 源标签加权；一条新闻可归多个板块（上限3个），未匹配入"综合要闻"。
分类是关键词匹配不是NLP，存在错分可能——按板块浏览，重要条目前置。
"""
import hashlib
import re
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

from loguru import logger

from ...data.sources.news_sources import NewsItem, NewsSourceManager

# 板块关键词字典：板块 -> 关键词列表
# 匹配文本 = 标题 + 摘要 + 源标签 + 关联股票名
SECTOR_KEYWORDS: Dict[str, List[str]] = {
    '人工智能算力': [
        '人工智能', '算力', '大模型', 'GPT', 'ChatGPT', '智能体', 'AIGC', 'Sora',
        '英伟达', 'NVIDIA', '光模块', 'CPO', '液冷', '数据中心', 'GPU', 'HBM',
        'AI', '训练', '推理', '生成式', '多模态', 'DeepSeek', 'OpenAI', 'Copilot',
    ],
    '半导体': [
        '半导体', '芯片', '晶圆', '光刻', '刻蚀', '封测', '存储', 'DRAM', 'NAND',
        'EDA', '台积电', '中芯', '北方华创', '中微公司', '韦尔', '兆易', '澜起',
        '寒武纪', '海光', '长电', '通富', '士兰微', '国产替代', '集成电路',
    ],
    '新能源车': [
        '新能源车', '电动车', '锂电', '锂电池', '碳酸锂', '六氟', '电解液', '隔膜',
        '负极', '正极', '宁德', '比亚迪', '理想', '蔚来', '小鹏', '小米汽车',
        '华为汽车', '问界', '智能驾驶', '自动驾驶', '激光雷达', '一体化压铸',
        '充电桩', '换电', '汽车', '整车',
    ],
    '光伏储能': [
        '光伏', '组件', '硅料', '硅片', '电池片', '逆变器', '储能', '风电', '海风',
        '绿电', 'HJT', 'TOPCon', '钙钛矿', '隆基', '通威', '晶科', '阳光电源',
        'TCL中环', '风电设备',
    ],
    '医药生物': [
        '医药', '创新药', 'CXO', '生物', '疫苗', '中药', '医疗器械', '集采', '医保',
        '减肥药', 'GLP-1', '司美', 'PD-1', 'ADC', '基因', '细胞治疗', '迈瑞',
        '药明', '恒瑞', '百济', '信达', '智飞', '眼科', '牙科', '医疗',
    ],
    '白酒食品': [
        '白酒', '茅台', '五粮液', '泸州老窖', '汾酒', '洋河', '啤酒', '乳业', '伊利',
        '蒙牛', '调味品', '海天', '食品', '饮料', '预制菜', '零食', '东鹏',
        '农夫山泉', '餐饮供应链',
    ],
    '军工国防': [
        '军工', '国防', '导弹', '无人机', '航空', '航天', '卫星', '北斗', '雷达',
        '船舶', '兵装', '中航', '航发', '军贸', '阅兵', '低空', '军费', '军品',
    ],
    '金融地产': [
        '银行', '券商', '证券', '保险', '信托', '公募', '基金', '降准', '降息',
        'LPR', 'MLF', '央行', '证监会', '交易所', '注册制', '地产', '房地产',
        '楼市', '房贷', '保交楼', '万科', '融创', '招行', '中信证券', '国泰君安',
        '金融', '信贷', '利率', '汇率',
    ],
    '有色金属': [
        '有色', '黄金', '白银', '锂', '铜', '铝', '稀土', '镍', '钴', '钨', '钼',
        '金价', '铜价', '紫金', '洛阳钼业', '北方稀土', '山东黄金', '贵金属',
        '大宗商品', '期货',
    ],
    '煤炭石油': [
        '煤炭', '煤价', '动力煤', '焦煤', '石油', '原油', '油价', '成品油',
        '天然气', '油气', '炼化', '石化', '中石油', '中石化', '中海油', '页岩',
    ],
    '农业养殖': [
        '农业', '种业', '生猪', '猪肉', '养殖', '饲料', '粮价', '粮食', '玉米',
        '大豆', '小麦', '棉花', '白糖', '乡村振兴', '牧原', '温氏', '秋粮',
        '猪瘟', '种植',
    ],
    '通信电子': [
        '通信', '5G', '6G', '运营商', '光通信', '光纤', '中兴', '基站', '手机',
        '苹果', '折叠屏', '消费电子', '面板', 'LED', '京东方', '立讯', '歌尔',
        '电子', '华为',
    ],
    '计算机软件': [
        '软件', '信创', '国产操作系统', '数据库', '中间件', 'ERP', 'SaaS',
        '云计算', '云服务', '鸿蒙', '欧拉', '金山', '用友', '金蝶', '网络安全',
        '数据安全', '勒索', '等保', '数字化', '互联网',
    ],
    '传媒游戏': [
        '传媒', '游戏', '影视', '电影', '票房', '院线', '剧集', '综艺', '广告',
        '营销', '出版', '短剧', '微短剧', '网易', '腾讯', '字节', '快手', '哔哩',
        '芒果', '分众', 'AI应用', '视频', '直播',
    ],
    '机械制造': [
        '机械', '机床', '工业母机', '刀具', '工程机械', '三一', '徐工', '中联重科',
        '叉车', '电梯', '轴承', '减速器', '丝杠', '轨交', '中车', '机器人',
        '人形机器人', 'Optimus', '埃斯顿', '汇川', '自动化',
    ],
    '化工': [
        '化工', '烯烃', '塑料', '橡胶', '轮胎', '化纤', '氨纶', '粘胶', '纯碱',
        '氯碱', '氟化工', '制冷剂', '钛白粉', '农药', '化肥', '磷化工', '万华',
        '华鲁', '荣盛', '化学',
    ],
    '钢铁建材': [
        '钢铁', '钢价', '螺纹钢', '铁矿', '建材', '水泥', '玻璃', '玻纤', '防水',
        '管材', '涂料', '宝钢', '鞍钢', '海螺', '东方雨虹', '北新建材',
    ],
    '电力公用': [
        '电力', '电价', '电网', '输配电', '特高压', '核电', '核能', '火电', '水电',
        '垃圾发电', '燃气', '水务', '环保', '污水', '碳交易', '碳中和', '碳达峰',
        '绿证', '三峡', '长江电力', '国电南瑞', '公用事业',
    ],
    '交通运输': [
        '航空', '机场', '航运', '港口', '铁路', '公路', '物流', '快递', '顺丰',
        '集运', 'BDI', '运价', '中远海控', '国航', '南航', '东航', '春秋',
        '交运', '出行',
    ],
    '商贸零售': [
        '零售', '商贸', '百货', '超市', '电商', '跨境电商', '免税', '黄金珠宝',
        '苏宁', '美团', '京东', '阿里', '拼多多', '抖音', '直播带货', '消费',
    ],
    '纺织服装': [
        '纺织', '服装', '家纺', '棉纺', '运动服饰', '安踏', '李宁', '海澜', '申洲',
    ],
    '教育': [
        '教育', '高校', '职业教育', '培训', 'K12', '双减', '留学', '考研', '公考',
    ],
    '旅游酒店': [
        '旅游', '酒店', '景区', 'OTA', '携程', '同程', '飞猪', '海南', '免税',
        '酒店房价', 'RevPAR', '出行链',
    ],
    '宏观政策': [
        '政治局', '国常会', '中央经济', '两会', '人大', '政协', 'GDP', 'CPI',
        'PPI', 'PMI', '社融', 'M2', '进出口', '海关', '财政部', '税务总局',
        '发改委', '国务院', '政策', '规划', '纲要', '部署', '会议强调',
        '金融街', '论坛', '监管', '商务部', '人民银行', '逆回购', '就业',
        '人口', '养老金', '生育',
    ],
    '全球市场': [
        '美联储', '鲍威尔', '非农', '美股', '纳斯达克', '标普', '道指', '华尔街',
        '欧股', '日经', '日元', '美元指数', '美债', 'OPEC', '关税', '贸易',
        '制裁', '出口管制', '地缘', '冲突', '战争', '停火', '和谈', '选举',
        '大选', '特朗普', '普京', '泽连斯基', '欧盟', '通胀', '衰退',
        '降息预期', '加息', '隔夜', '美股收盘', '欧股收盘',
    ],
}

UNMATCHED_SECTOR = '综合要闻'

# 纯ASCII关键词用词边界匹配，避免 AI->said 类误命中
_ASCII_KEYWORDS = {kw for kws in SECTOR_KEYWORDS.values() for kw in kws
                   if kw.isascii()}
# 板块 -> 编译后的匹配器
_SECTOR_MATCHERS: Dict[str, List[Tuple[str, 're.Pattern']]] = {}
for _sector, _kws in SECTOR_KEYWORDS.items():
    _compiled = []
    for _kw in _kws:
        if _kw.isascii():
            _compiled.append((_kw, re.compile(r'\b' + re.escape(_kw) + r'\b', re.I)))
        else:
            _compiled.append((_kw, None))
    _SECTOR_MATCHERS[_sector] = _compiled

# 展示顺序（count 相同时的字典序兜底）
_SECTOR_ORDER = {name: i for i, name in enumerate(SECTOR_KEYWORDS.keys())}


def normalize_title(title: str) -> str:
    """标题规范化：去空白/标点/【】前缀/大小写，用于精确去重"""
    if not title:
        return ''
    text = re.sub(r'^【[^】]{1,40}】', '', title.strip())  # 新浪【标题】前缀
    text = text.lower()
    text = re.sub(r'[\s\.,;:!?，。；：！？、（）()\[\]【】{}<>《》"\'\'""''·—\-—…~`@#￥$%&*+/\\|_]+', '', text)
    return text


def title_hash(title: str) -> str:
    return hashlib.sha1(normalize_title(title).encode('utf-8')).hexdigest()[:16]


class MorningNewsMonitor:
    """早间财经新闻监控：抓取→去重→板块分类→推送"""

    # 阈值（与 config NEWS_* 对应，settings 缺省时用类常量）
    WINDOW_HOURS = 20
    MAX_PER_SECTOR = 6
    MAX_SECTORS = 8
    MAX_TOTAL = 120
    DEDUP_THRESHOLD = 0.85
    SEEN_TTL_HOURS = 48
    SEEN_KEEP = 600  # 指纹列表上限（防无限增长）
    MAX_MESSAGE_CHARS = 15000  # 钉钉单条上限留余量

    # Redis 键
    KEY_CHECK_DATE = 'news:check_date'
    KEY_LAST_PUSH = 'news:last_push_at'
    KEY_SEEN = 'news:seen'
    KEY_LATEST = 'news:latest'
    KEY_SECTORS = 'news:sectors'

    def __init__(self, datacenter=None, settings=None):
        self.datacenter = datacenter
        self._cache = getattr(datacenter, 'cache', None)
        try:
            if settings is None:
                from ...config import get_settings
                settings = get_settings()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"News monitor settings load failed: {e}")
            settings = None
        self.settings = settings
        if settings is not None:
            self.WINDOW_HOURS = getattr(settings, 'NEWS_WINDOW_HOURS', self.WINDOW_HOURS)
            self.MAX_PER_SECTOR = getattr(settings, 'NEWS_MAX_PER_SECTOR', self.MAX_PER_SECTOR)
            self.MAX_SECTORS = getattr(settings, 'NEWS_MAX_SECTORS', self.MAX_SECTORS)
            self.MAX_TOTAL = getattr(settings, 'NEWS_MAX_TOTAL', self.MAX_TOTAL)
            self.DEDUP_THRESHOLD = getattr(settings, 'NEWS_DEDUP_THRESHOLD', self.DEDUP_THRESHOLD)
            self.SEEN_TTL_HOURS = getattr(settings, 'NEWS_SEEN_TTL_HOURS', self.SEEN_TTL_HOURS)
        self.source_manager = NewsSourceManager(settings)
        # 无 Redis 时的进程内降级状态
        self._mem_seen: List[Dict] = []
        self._mem_last_push: Optional[str] = None

    # ---------- Redis  helpers（无Redis优雅降级） ----------

    def _get(self, key: str, default=None):
        if self._cache is not None:
            try:
                val = self._cache.get(key)
                if val is not None:
                    return val
            except Exception as e:  # noqa: BLE001
                logger.debug(f"cache get {key} failed: {e}")
        mem_map = {'news:seen': self._mem_seen,
                   'news:last_push_at': self._mem_last_push}
        return mem_map.get(key, default)

    def _set(self, key: str, value, ttl: int) -> None:
        if self._cache is not None:
            try:
                self._cache.set(key, value, ttl=ttl)
                return
            except Exception as e:  # noqa: BLE001
                logger.debug(f"cache set {key} failed: {e}")
        if key == 'news:seen':
            self._mem_seen = value
        elif key == 'news:last_push_at':
            self._mem_last_push = value

    # ---------- 时间窗 ----------

    def _window_start(self) -> Tuple[datetime, bool]:
        """返回(窗口起点, 是否来自上次推送记录)"""
        last = self._get(self.KEY_LAST_PUSH)
        if last:
            try:
                return datetime.fromisoformat(last), True
            except (ValueError, TypeError):
                pass
        return datetime.now() - timedelta(hours=self.WINDOW_HOURS), False

    # ---------- 去重 ----------

    def dedup(self, items: List[NewsItem],
              persist: bool = True) -> Tuple[List[NewsItem], int]:
        """去重，返回(保留列表, 剔除数)

        保留策略：同事件多源报道保留 source_rank 最小（优先级最高）的源。
        persist=False 时不回写指纹列表（API 预览用，不影响9:15推送去重）。
        """
        seen_records: List[Dict] = list(self._get(self.KEY_SEEN, []) or [])
        seen_hashes = {r.get('h') for r in seen_records if isinstance(r, dict)}
        seen_titles = [r.get('t', '') for r in seen_records if isinstance(r, dict)]

        kept: List[NewsItem] = []
        dropped = 0
        # 批内按源优先级+时间排序，保证同事件高优先级源先入
        ordered = sorted(items, key=lambda x: (x.source_rank,
                                               -(x.published_at.timestamp()
                                                 if x.published_at else 0)))
        accepted_titles: List[str] = []

        for item in ordered:
            norm = normalize_title(item.title)
            if not norm:
                dropped += 1
                continue
            h = title_hash(item.title)
            # 1) 跨推送精确去重
            if h in seen_hashes:
                dropped += 1
                continue
            # 2) 批内精确去重
            if h in {title_hash(k.title) for k in kept}:
                dropped += 1
                continue
            # 3) 近似去重（跨推送 + 批内）
            is_dup = False
            candidates = seen_titles + accepted_titles
            for cand in candidates:
                if not cand:
                    continue
                if abs(len(cand) - len(norm)) > max(8, 0.35 * max(len(cand), len(norm))):
                    continue  # 长度差过大不可能是同一事件
                if SequenceMatcher(None, norm, cand).ratio() >= self.DEDUP_THRESHOLD:
                    is_dup = True
                    break
            if is_dup:
                dropped += 1
                continue
            kept.append(item)
            accepted_titles.append(norm)
            seen_records.append({'h': h, 't': norm, 's': item.source})

        # 指纹列表裁剪 + 回写（供下次推送去重）
        if persist:
            if len(seen_records) > self.SEEN_KEEP:
                seen_records = seen_records[-self.SEEN_KEEP:]
            self._set(self.KEY_SEEN, seen_records, ttl=self.SEEN_TTL_HOURS * 3600)
        return kept, dropped

    # ---------- 板块分类 ----------

    @staticmethod
    def classify(item: NewsItem, max_sectors: int = 3) -> List[str]:
        """关键词分类，返回板块列表（可能多板块，未匹配返回[]）"""
        text = ' '.join([item.title, item.content,
                         ' '.join(item.tags or []),
                         ' '.join(item.stocks or [])])
        if not text.strip():
            return []
        matched: List[Tuple[int, int, str]] = []
        for idx, (sector, matchers) in enumerate(_SECTOR_MATCHERS.items()):
            hits = 0
            for kw, pattern in matchers:
                if pattern is not None:
                    if pattern.search(text):
                        hits += 1
                elif kw in text:
                    hits += 1
            if hits > 0:
                matched.append((hits, idx, sector))
        if not matched:
            return []
        matched.sort(key=lambda x: (-x[0], x[1]))
        return [s for _, _, s in matched[:max_sectors]]

    def group_by_sector(self, items: List[NewsItem]) -> Dict[str, List[NewsItem]]:
        """按板块分组（未匹配入综合要闻），板块间按条数降序"""
        groups: Dict[str, List[NewsItem]] = {}
        for item in items:
            sectors = self.classify(item) or [UNMATCHED_SECTOR]
            for sector in sectors:
                groups.setdefault(sector, []).append(item)
        # 板块内按时间倒序
        for lst in groups.values():
            lst.sort(key=lambda x: x.published_at or datetime.min, reverse=True)
        # 板块排序：条数降序，并列按字典序（综合要闻恒最后）
        ordered = dict(sorted(
            groups.items(),
            key=lambda kv: (-len(kv[1]),
                            1 if kv[0] == UNMATCHED_SECTOR else 0,
                            _SECTOR_ORDER.get(kv[0], 999))))
        return ordered

    # ---------- 摘要与文案 ----------

    def build_digest(self, kept: List[NewsItem], dropped: int,
                     window_start: datetime, window_end: datetime,
                     from_last_push: bool) -> Dict:
        groups = self.group_by_sector(kept)
        sector_entries = []
        displayed = 0
        for sector, lst in groups.items():
            if displayed >= self.MAX_SECTORS:
                break
            shown = lst[:self.MAX_PER_SECTOR]
            sector_entries.append({
                'sector': sector,
                'count': len(lst),
                'shown': len(shown),
                'truncated': len(lst) > len(shown),
                'items': [self._item_brief(it) for it in shown],
            })
            displayed += 1
        hidden_sectors = max(0, len(groups) - len(sector_entries))
        hidden_items = sum(len(lst) for lst in
                           list(groups.values())[len(sector_entries):])
        source_counts: Dict[str, int] = {}
        for it in kept:
            source_counts[it.source] = source_counts.get(it.source, 0) + 1
        return {
            'generated_at': window_end.isoformat(),
            'window_start': window_start.isoformat(),
            'window_end': window_end.isoformat(),
            'from_last_push': from_last_push,
            'total': len(kept),
            'dedup_dropped': dropped,
            'sector_count': len(groups),
            'hidden_sectors': hidden_sectors,
            'hidden_items': hidden_items,
            'sources': source_counts,
            'sectors': sector_entries,
        }

    @staticmethod
    def _item_brief(item: NewsItem) -> Dict:
        return {
            'time': item.published_at.strftime('%H:%M') if item.published_at else '',
            'source': item.source,
            'title': item.title,
            'url': item.url,
        }

    def format_message(self, digest: Dict) -> str:
        """钉钉 markdown 文案"""
        start = datetime.fromisoformat(digest['window_start'])
        end = datetime.fromisoformat(digest['window_end'])
        window = f"{start.strftime('%m-%d %H:%M')} → {end.strftime('%m-%d %H:%M')}"
        src_desc = '·'.join(f"{k}" for k in digest['sources'].keys()) or '无'
        lines = [
            f"**时间窗**: {window} ｜ **新增 {digest['total']} 条**"
            f"（去重剔除 {digest['dedup_dropped']} 条）",
            f"**来源**: {src_desc} ｜ 覆盖 {digest['sector_count']} 个板块",
            "",
        ]
        for entry in digest['sectors']:
            mark = f"（+{entry['count'] - entry['shown']}）" if entry['truncated'] else ''
            lines.append(f"### {entry['sector']} · {entry['count']}{mark}")
            for brief in entry['items']:
                title = brief['title']
                if brief['url']:
                    lines.append(f"- {brief['time']} [{brief['source']}] "
                                 f"{title} [详情]({brief['url']})")
                else:
                    lines.append(f"- {brief['time']} [{brief['source']}] {title}")
            lines.append("")
        if digest['hidden_sectors'] > 0:
            lines.append(f"> 其余 {digest['hidden_sectors']} 个板块共 "
                         f"{digest['hidden_items']} 条未展示，"
                         f"详见后台 /api/news/sectors")
        msg = '\n'.join(lines).strip()
        if len(msg) > self.MAX_MESSAGE_CHARS:
            msg = msg[:self.MAX_MESSAGE_CHARS - 60] + "\n\n> （文案超长已截断，完整内容见后台）"
        return msg

    # ---------- 主流程 ----------

    def _collect_windowed(self) -> Tuple[List[NewsItem], int, datetime, bool, datetime]:
        """抓取+时间窗过滤，返回(窗口内条目, 抓取总数, 窗口起点, 是否上次推送, 窗口终点)"""
        window_start, from_last = self._window_start()
        window_end = datetime.now()
        items = self.source_manager.fetch_all()
        windowed = [it for it in items
                    if it.published_at and
                    it.published_at >= window_start - timedelta(minutes=5)]
        windowed.sort(key=lambda x: x.published_at or datetime.min, reverse=True)
        if len(windowed) > self.MAX_TOTAL:
            logger.info(f"News windowed {len(windowed)} > cap {self.MAX_TOTAL}, truncating")
            windowed = windowed[:self.MAX_TOTAL]
        return windowed, len(items), window_start, from_last, window_end

    def collect_digest(self) -> Optional[Dict]:
        """API预览：实时抓取+去重+分类，不写任何推送状态"""
        try:
            windowed, fetched, window_start, from_last, window_end = \
                self._collect_windowed()
            kept, dropped = self.dedup(windowed, persist=False)
            logger.info(f"News digest preview: fetched={fetched} "
                        f"windowed={len(windowed)} kept={len(kept)} dropped={dropped}")
            if not kept:
                return None
            return self.build_digest(kept, dropped, window_start,
                                     window_end, from_last)
        except Exception as e:  # noqa: BLE001
            logger.error(f"News digest preview failed: {e}")
            return None

    def check_and_push(self, notifier=None) -> Optional[Dict]:
        """抓取→去重→分类→推送；无新增返回None"""
        window_start, from_last = self._window_start()
        window_end = datetime.now()

        items = self.source_manager.fetch_all()
        # 时间窗过滤（允许5分钟时钟误差）
        windowed = [it for it in items
                    if it.published_at and
                    it.published_at >= window_start - timedelta(minutes=5)]
        windowed.sort(key=lambda x: x.published_at or datetime.min, reverse=True)
        if len(windowed) > self.MAX_TOTAL:
            logger.info(f"News windowed {len(windowed)} > cap {self.MAX_TOTAL}, truncating")
            windowed = windowed[:self.MAX_TOTAL]

        kept, dropped = self.dedup(windowed)
        logger.info(f"Morning news: fetched={len(items)} windowed={len(windowed)} "
                    f"kept={len(kept)} dropped={dropped} "
                    f"window={'last_push' if from_last else f'{self.WINDOW_HOURS}h'}")

        digest = self.build_digest(kept, dropped, window_start, window_end, from_last)

        if not kept:
            # 无新增：窗口仍推进（下次从此刻算起），但不推送
            self._set(self.KEY_LAST_PUSH, window_end.isoformat(), ttl=7 * 86400)
            logger.info("Morning news: no new items, skip push")
            return None

        # 存储供 API/前端
        self._set(self.KEY_LATEST, digest, ttl=7 * 86400)
        self._set(self.KEY_SECTORS, {
            'generated_at': digest['generated_at'],
            'sectors': [{'sector': e['sector'], 'count': e['count']}
                        for e in digest['sectors']],
            'hidden_sectors': digest['hidden_sectors'],
            'hidden_items': digest['hidden_items'],
        }, ttl=7 * 86400)
        self._set(self.KEY_LAST_PUSH, window_end.isoformat(), ttl=7 * 86400)

        if notifier is not None:
            title = "📰 早间财经要闻（按板块）"
            msg = self.format_message(digest)
            notifier.send_custom_message(title, msg)
            logger.info(f"Morning news pushed: {len(kept)} items, "
                        f"{digest['sector_count']} sectors, "
                        f"{len(msg)} chars")
        return digest
