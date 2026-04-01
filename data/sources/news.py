from __future__ import annotations

import logging

import akshare as ak

from data.sources.sina_news_crawler import fetch_sina_news_by_date

logger = logging.getLogger("trading.data.news")


# A股短线题材相关关键词（用于从海量新闻中筛选有价值的条目）
_A_STOCK_KEYWORDS = (
    # A股行情
    "涨停", "跌停", "板块", "概念股", "A股", "沪指", "深成", "创业板",
    "盘中", "收盘", "开盘", "涨近", "跌近", "涨超", "跌超", "拉升",
    "封板", "连板", "打板", "龙头", "妖股", "强势股", "两市", "沪深",
    "北向资金", "融资", "融券", "主力", "游资", "机构",
    # 行业/题材
    "芯片", "半导体", "AI", "人工智能", "算力", "大模型", "机器人",
    "新能源", "光伏", "锂电", "储能", "风电", "氢能",
    "医药", "生物", "创新药", "中药",
    "军工", "国防", "航天", "卫星",
    "消费", "白酒", "食品", "旅游", "免税",
    "汽车", "智能驾驶", "无人驾驶", "电动车",
    "地产", "房地产", "基建", "水泥",
    "银行", "券商", "保险", "金融",
    "稀土", "有色", "钢铁", "煤炭", "化工",
    "传媒", "游戏", "影视", "短剧",
    "数据要素", "数字经济", "信创", "国产替代",
    "低空经济", "飞行汽车", "eVTOL",
    # 宏观政策
    "央行", "降准", "降息", "LPR", "国务院", "发改委", "证监会",
    "财政部", "工信部", "科技部",
    "注册制", "IPO", "退市", "减持新规", "印花税",
    # 市场情绪
    "牛市", "熊市", "反弹", "回调", "放量", "缩量", "突破",
)


# 排除海外/港股/无关新闻的关键词
_EXCLUDE_KEYWORDS = (
    # 海外市场
    "美股", "纳斯达克", "标普500", "道指", "美联储",
    "英国", "欧洲", "法国", "德国", "日本", "韩国", "印度",
    "以色列", "伊朗", "乌克兰", "俄罗斯", "中东",
    "特朗普", "拜登", "白宫", "五角大楼",
    "挪威", "印尼", "SpaceX", "IPO申请",
    # 海外公司
    "谷歌", "苹果公司", "微软", "亚马逊", "Meta", "OpenAI",
    "英伟达", "AMD", "英特尔", "三星", "Arm",
    "迪士尼", "优步", "Uber", "特斯拉", "马斯克",
    "CrowdStrike", "埃森哲", "贝莱德",
    # 港股（除非同时有 A 股信号）
    "港元", "港股", "恒指", "科指", "港币",
    "-B", "-W", "-S",
    # 加密货币
    "加密货币", "比特币", "以太坊",
    # 低价值
    "光大期货", "中天期货", "外盘头条",
)


def _is_a_stock_relevant(title: str) -> bool:
    """判断新闻标题是否与 A 股短线题材相关。

    同时命中 A 股关键词且不命中海外排除词才保留。
    央视新闻（政策向）不经过此过滤。
    """
    if any(ex in title for ex in _EXCLUDE_KEYWORDS):
        return False
    return any(kw in title for kw in _A_STOCK_KEYWORDS)


def fetch_financial_news(date: str, backtest: bool = False,
                         cache_db=None) -> list[str]:
    """获取财经新闻标题列表（已筛选，只保留 A 股题材相关）。

    优先查库（如有 cache_db），库中无数据时实时抓取。
    回测模式下不做实时抓取（避免前瞻偏差），仅用库中数据 + 央视（支持历史）。

    Args:
        date: 日期 YYYYMMDD
        backtest: 是否回测模式
        cache_db: TradingDB 实例（可选）

    Returns:
        新闻标题列表（去重、筛选后，约 30-50 条）
    """
    date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}" if len(date) == 8 else date
    headlines = []

    # 1. 优先从数据库取（取多一些，筛选后再截断）
    if cache_db:
        cached = cache_db.get_news_by_date(date_fmt, limit=500)
        if cached:
            raw = [item["title"] for item in cached if item.get("title")]
            relevant = [t for t in raw if _is_a_stock_relevant(t)]
            headlines.extend(relevant)
            logger.info("从数据库获取 %s 新闻: %d 条原始, %d 条A股相关",
                        date_fmt, len(raw), len(relevant))

    # 2. 库中数据不足时补充
    if len(headlines) < 10:
        # 央视新闻联播（支持历史日期，回测安全）
        cctv = _fetch_cctv_news(date)
        headlines.extend(cctv)

        # 非回测模式：实时抓取更多源
        if not backtest:
            em = _fetch_em_news()
            headlines.extend(t for t in em if _is_a_stock_relevant(t))
            cls = _fetch_cls_news()
            headlines.extend(t for t in cls if _is_a_stock_relevant(t))

    # 去重
    seen = set()
    unique = []
    for h in headlines:
        if h not in seen:
            seen.add(h)
            unique.append(h)

    return unique[:50]


def collect_news_to_db(db, date: str) -> int:
    """从多个源采集新闻并入库。

    Args:
        db: TradingDB 实例
        date: 日期 YYYYMMDD

    Returns:
        新增条数
    """
    date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}" if len(date) == 8 else date
    all_items = []

    # 1. 新浪财经（主力，量大）
    try:
        sina_items = fetch_sina_news_by_date(date_fmt)
        all_items.extend(sina_items)
        logger.info("新浪财经: %d 条", len(sina_items))
    except Exception as e:
        logger.warning("新浪财经采集失败: %s", e)

    # 2. 央视新闻联播（支持历史）
    cctv_titles = _fetch_cctv_news(date)
    for title in cctv_titles:
        all_items.append({
            "news_date": date_fmt,
            "news_time": "19:00:00",
            "source": "cctv",
            "title": title,
            "content": None,
            "url": None,
            "category": "policy",
        })

    # 3. 东方财富全球财经（仅实时，历史补采无效）
    em_titles = _fetch_em_news()
    for title in em_titles:
        all_items.append({
            "news_date": date_fmt,
            "news_time": "",
            "source": "em",
            "title": title,
            "content": None,
            "url": None,
            "category": "finance",
        })

    # 4. 财联社电报（仅实时）
    cls_titles = _fetch_cls_news()
    for title in cls_titles:
        all_items.append({
            "news_date": date_fmt,
            "news_time": "",
            "source": "cls",
            "title": title,
            "content": None,
            "url": None,
            "category": "finance",
        })

    if all_items:
        added = db.save_news_items(all_items)
        logger.info("新闻入库完成: 总 %d 条, 新增 %d 条", len(all_items), added)
        return added

    return 0


def _fetch_cctv_news(date: str) -> list[str]:
    """央视新闻联播（支持历史日期）。"""
    try:
        df = ak.news_cctv(date=date)
        if df is not None and not df.empty:
            titles = df["title"].tolist()[:15]
            logger.info("获取央视新闻成功，%d 条", len(titles))
            return titles
    except Exception as e:
        logger.warning("获取央视新闻失败: %s", e)
    return []


def _fetch_em_news() -> list[str]:
    """东方财富全球财经（仅实时）。"""
    try:
        df = ak.stock_info_global_em()
        if df is not None and not df.empty:
            col = "标题" if "标题" in df.columns else df.columns[0]
            titles = df[col].tolist()[:30]
            logger.info("获取东方财富新闻成功，%d 条", len(titles))
            return titles
    except Exception as e:
        logger.debug("获取东方财富新闻失败: %s", e)
    return []


def _fetch_cls_news() -> list[str]:
    """财联社电报（仅实时）。"""
    try:
        df = ak.stock_info_global_cls(symbol="全部")
        if df is not None and not df.empty:
            col = "标题" if "标题" in df.columns else df.columns[0]
            titles = df[col].tolist()[:20]
            logger.info("获取财联社新闻成功，%d 条", len(titles))
            return titles
    except Exception as e:
        logger.debug("获取财联社新闻失败: %s", e)
    return []
