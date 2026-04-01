#!/usr/bin/env python3
"""新闻采集脚本。

用法:
    # 采集今日新闻（cron 每日执行）
    python scripts/collect_news.py --today

    # 补采历史新闻（新浪爬虫 + Tushare 兜底）
    python scripts/collect_news.py --backfill --start 20250901 --end 20250930

    # 补采单日
    python scripts/collect_news.py --backfill --start 20250915 --end 20250915
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

# 添加项目根目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_loader import load_config
from utils.logger import setup_logger
from storage.db import TradingDB
from data.sources.news import collect_news_to_db
from data.sources.sina_news_crawler import fetch_sina_news_range
from data.sources.tushare_news import fetch_tushare_news


def today_mode(db: TradingDB):
    """采集今日新闻。"""
    today = datetime.now().strftime("%Y%m%d")
    print(f"采集今日新闻: {today}")
    added = collect_news_to_db(db, today)
    print(f"完成: 新增 {added} 条")


def backfill_mode(db: TradingDB, start: str, end: str):
    """批量补采历史新闻。"""
    start_fmt = f"{start[:4]}-{start[4:6]}-{start[6:]}"
    end_fmt = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    print(f"补采历史新闻: {start_fmt} ~ {end_fmt}")

    # 1. 新浪财经爬虫（主力）
    print("\n[1/3] 新浪财经爬虫...")
    try:
        sina_items = fetch_sina_news_range(start_fmt, end_fmt, max_pages=100)
        if sina_items:
            added = db.save_news_items(sina_items)
            print(f"  新浪财经: 获取 {len(sina_items)} 条, 新增 {added} 条")
        else:
            print("  新浪财经: 无数据")
    except Exception as e:
        print(f"  新浪财经失败: {e}")

    # 2. Tushare 补充
    print("\n[2/3] Tushare Pro...")
    try:
        tushare_items = fetch_tushare_news(start_fmt, end_fmt)
        if tushare_items:
            added = db.save_news_items(tushare_items)
            print(f"  Tushare: 获取 {len(tushare_items)} 条, 新增 {added} 条")
        else:
            print("  Tushare: 无数据（未配置 TUSHARE_TOKEN 或无返回）")
    except Exception as e:
        print(f"  Tushare 失败: {e}")

    # 3. 央视新闻联播（逐日补采，支持历史）
    print("\n[3/3] 央视新闻联播...")
    import akshare as ak
    start_dt = datetime.strptime(start_fmt, "%Y-%m-%d")
    end_dt = datetime.strptime(end_fmt, "%Y-%m-%d")
    total_cctv = 0
    current = start_dt

    while current <= end_dt:
        date_str = current.strftime("%Y%m%d")
        date_fmt = current.strftime("%Y-%m-%d")

        # 跳过已有数据
        existing = db.get_news_count_by_date(date_fmt)
        if existing > 0:
            current += timedelta(days=1)
            continue

        try:
            df = ak.news_cctv(date=date_str)
            if df is not None and not df.empty:
                items = []
                for _, row in df.iterrows():
                    items.append({
                        "news_date": date_fmt,
                        "news_time": "19:00:00",
                        "source": "cctv",
                        "title": str(row.get("title", "")),
                        "content": None,
                        "url": None,
                        "category": "policy",
                    })
                db.save_news_items(items)
                total_cctv += len(items)
        except Exception:
            pass

        current += timedelta(days=1)
        time.sleep(0.3)

    print(f"  央视新闻: 共 {total_cctv} 条")

    # 统计
    print("\n=== 补采完成 ===")
    start_dt = datetime.strptime(start_fmt, "%Y-%m-%d")
    end_dt = datetime.strptime(end_fmt, "%Y-%m-%d")
    current = start_dt
    total = 0
    while current <= end_dt:
        count = db.get_news_count_by_date(current.strftime("%Y-%m-%d"))
        if count > 0:
            total += count
        current += timedelta(days=1)
    print(f"数据库中 {start_fmt}~{end_fmt} 共 {total} 条新闻")


def main():
    parser = argparse.ArgumentParser(description="新闻采集脚本")
    parser.add_argument("--today", action="store_true", help="采集今日新闻")
    parser.add_argument("--backfill", action="store_true", help="补采历史新闻")
    parser.add_argument("--start", type=str, help="开始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, help="结束日期 YYYYMMDD")
    args = parser.parse_args()

    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    config = load_config()
    paths = config.get("paths", {})
    setup_logger(paths.get("log_dir", "logs"))

    db = TradingDB(paths.get("db_path", "storage/trading.db"))

    try:
        if args.today:
            today_mode(db)
        elif args.backfill:
            if not args.start or not args.end:
                print("--backfill 需要 --start 和 --end 参数")
                sys.exit(1)
            backfill_mode(db, args.start, args.end)
        else:
            parser.print_help()
    finally:
        db.close()


if __name__ == "__main__":
    main()
