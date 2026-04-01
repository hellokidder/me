"""Tushare Pro 新闻接口（新浪爬虫的补充/兜底）。

需要环境变量 TUSHARE_TOKEN。
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger("trading.data.tushare_news")


def fetch_tushare_news(start_date: str, end_date: str,
                       src: str = "sina",
                       limit: int = 200) -> list[dict]:
    """通过 Tushare Pro 获取历史新闻。

    Args:
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        src: 新闻来源 (sina/wallstreetcn/10jqka/eastmoney/yuncaijing)
        limit: 单次最多返回条数

    Returns:
        news_items 格式的列表
    """
    token = os.environ.get("TUSHARE_TOKEN", "")
    if not token:
        logger.info("TUSHARE_TOKEN 未配置，跳过 Tushare 新闻采集")
        return []

    try:
        import tushare as ts
    except ImportError:
        logger.warning("tushare 未安装，跳过 Tushare 新闻采集")
        return []

    try:
        pro = ts.pro_api(token)
        df = pro.news(
            src=src,
            start_date=f"{start_date} 00:00:00",
            end_date=f"{end_date} 23:59:59",
            limit=limit,
        )
    except Exception as e:
        logger.warning("Tushare 新闻获取失败: %s", e)
        return []

    if df is None or df.empty:
        return []

    items = []
    for _, row in df.iterrows():
        dt_str = str(row.get("datetime", ""))
        news_date = dt_str[:10] if len(dt_str) >= 10 else ""
        news_time = dt_str[11:19] if len(dt_str) >= 19 else ""

        title = str(row.get("title", "")).strip()
        if not title:
            continue

        items.append({
            "news_date": news_date,
            "news_time": news_time,
            "source": f"tushare_{src}",
            "title": title,
            "content": str(row.get("content", "")).strip() or None,
            "url": None,
            "category": "finance",
        })

    logger.info("Tushare 新闻 %s~%s: 获取 %d 条", start_date, end_date, len(items))
    return items
