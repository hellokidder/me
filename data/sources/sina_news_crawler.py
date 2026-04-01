"""新浪财经滚动新闻爬虫。

通过 feed.mix.sina.com.cn JSON API 抓取财经/股市新闻。
支持按日期范围批量补采历史数据。
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime

import requests

logger = logging.getLogger("trading.data.sina_crawler")

# 新浪滚动新闻 API
_BASE_URL = "https://feed.mix.sina.com.cn/api/roll/get"

# 分类 lid
_CATEGORIES = {
    "finance": 2516,  # 财经
    "stock": 2517,    # 股市
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/roll/",
}


def fetch_sina_news_by_date(
    target_date: str,
    max_pages: int = 20,
    categories: list[str] | None = None,
) -> list[dict]:
    """抓取指定日期的新浪财经新闻。

    Args:
        target_date: 目标日期，格式 YYYY-MM-DD
        max_pages: 每个分类最多翻页数
        categories: 要抓取的分类，默认 ["finance", "stock"]

    Returns:
        新闻列表，每条包含 news_date, news_time, source, title, content, url, category
    """
    if categories is None:
        categories = ["finance", "stock"]

    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    all_items = []
    seen_titles = set()

    for cat_name in categories:
        lid = _CATEGORIES.get(cat_name)
        if not lid:
            continue

        items = _fetch_category(lid, cat_name, target_dt, max_pages, seen_titles)
        all_items.extend(items)

    logger.info("新浪新闻 %s: 共获取 %d 条", target_date, len(all_items))
    return all_items


def fetch_sina_news_range(
    start_date: str,
    end_date: str,
    max_pages: int = 50,
    categories: list[str] | None = None,
) -> list[dict]:
    """抓取日期范围内的新浪财经新闻（通过翻页向前追溯）。

    注意：新浪 API 按时间倒序返回，翻页越深越老。
    如果目标日期太久远，可能需要翻很多页。

    Args:
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        max_pages: 每个分类最多翻页数
        categories: 要抓取的分类

    Returns:
        新闻列表
    """
    if categories is None:
        categories = ["finance", "stock"]

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    all_items = []
    seen_titles = set()

    for cat_name in categories:
        lid = _CATEGORIES.get(cat_name)
        if not lid:
            continue

        items = _fetch_category_range(lid, cat_name, start_dt, end_dt, max_pages, seen_titles)
        all_items.extend(items)

    logger.info("新浪新闻 %s~%s: 共获取 %d 条", start_date, end_date, len(all_items))
    return all_items


def _fetch_category(
    lid: int,
    cat_name: str,
    target_date,
    max_pages: int,
    seen_titles: set,
) -> list[dict]:
    """抓取单个分类、单日的新闻。"""
    items = []

    for page in range(1, max_pages + 1):
        data_list, reached_before = _fetch_page(lid, page, target_date, target_date)
        if data_list is None:
            break

        for item in data_list:
            title = item.get("title", "").strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            items.append(_parse_item(item, cat_name))

        if reached_before:
            break

        time.sleep(0.3 + random.random() * 0.3)

    return items


def _fetch_category_range(
    lid: int,
    cat_name: str,
    start_date,
    end_date,
    max_pages: int,
    seen_titles: set,
) -> list[dict]:
    """抓取单个分类、日期范围内的新闻。"""
    items = []

    for page in range(1, max_pages + 1):
        data_list, reached_before = _fetch_page(lid, page, start_date, end_date)
        if data_list is None:
            break

        for item in data_list:
            title = item.get("title", "").strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            items.append(_parse_item(item, cat_name))

        if reached_before:
            break

        time.sleep(0.3 + random.random() * 0.3)

    return items


def _fetch_page(lid: int, page: int, start_date, end_date):
    """抓取一页数据。返回 (items_in_range, reached_before_start)。"""
    try:
        resp = requests.get(
            _BASE_URL,
            params={
                "pageid": 153,
                "lid": lid,
                "k": "",
                "num": 50,
                "page": page,
                "r": random.random(),
            },
            headers=_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()
    except Exception as e:
        logger.warning("新浪新闻 API 请求失败 (page=%d): %s", page, e)
        return None, True

    data = result.get("result", {}).get("data")
    if not data:
        return None, True

    filtered = []
    reached_before = False

    for item in data:
        ctime = item.get("ctime")
        if not ctime:
            continue

        try:
            item_dt = datetime.fromtimestamp(int(ctime)).date()
        except (ValueError, TypeError, OSError):
            continue

        if item_dt < start_date:
            reached_before = True
            continue
        if item_dt > end_date:
            continue

        filtered.append(item)

    return filtered, reached_before


def _parse_item(item: dict, category: str) -> dict:
    """将 API 返回的 item 转换为 news_items 表格式。"""
    ctime = int(item.get("ctime", 0))
    dt = datetime.fromtimestamp(ctime) if ctime else None

    return {
        "news_date": dt.strftime("%Y-%m-%d") if dt else "",
        "news_time": dt.strftime("%H:%M:%S") if dt else "",
        "source": "sina",
        "title": item.get("title", "").strip(),
        "content": item.get("intro", "").strip() or None,
        "url": item.get("url", "") or None,
        "category": category,
    }
