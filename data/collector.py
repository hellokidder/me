from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

from data.sources.market_index import fetch_market_indices
from data.sources.limit_up import (
    fetch_limit_up_pool,
    fetch_failed_limit_up,
    fetch_limit_down_pool,
    fetch_strong_pool,
)
from data.sources.dragon_tiger import fetch_dragon_tiger_list, fetch_institutional_trading
from data.sources.northbound import fetch_northbound_flow
from data.sources.news import fetch_financial_news
from data.sources.overnight import fetch_overnight_markets
from data.sources.margin import fetch_margin_data
from data.sources.sentiment import calc_market_sentiment

logger = logging.getLogger("trading.data.collector")


class MarketDataCollector:
    def __init__(self, config: dict, mode: str = "live", cache_db=None):
        self.config = config
        self.mode = mode  # "live" or "backtest"
        self.cache_db = cache_db  # TradingDB 实例，用于日线缓存
        self._em_fail_count = 0  # 东方财富连续失败计数（熔断用）

    def collect_all(self, date: str) -> dict:
        logger.info("========== 开始收集 %s 市场数据 [%s] ==========", date, self.mode)
        is_backtest = self.mode == "backtest"

        # 定义所有独立数据源
        # 注：news_headlines 使用 cache_db（SQLite），不能放入线程池
        tasks = {
            "indices": lambda: fetch_market_indices(date if is_backtest else None),
            "limit_up_pool": lambda: fetch_limit_up_pool(date),
            "failed_limit_pool": lambda: fetch_failed_limit_up(date),
            "limit_down_pool": lambda: fetch_limit_down_pool(date),
            "strong_pool": lambda: fetch_strong_pool(date),
            "dragon_tiger": lambda: fetch_dragon_tiger_list(date),
            "institutional": lambda: fetch_institutional_trading(date),
            "northbound": lambda: fetch_northbound_flow(date),
            "overnight": lambda: fetch_overnight_markets(date if is_backtest else None),
            "margin": lambda: fetch_margin_data(date),
        }

        # DataFrame 类型的 key（失败时返回空 DataFrame）
        df_keys = {
            "limit_up_pool", "failed_limit_pool", "limit_down_pool",
            "strong_pool", "dragon_tiger", "institutional",
        }

        results = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fn): key for key, fn in tasks.items()}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    results[key] = future.result()
                except Exception as e:
                    logger.warning("%s 采集失败: %s", key, e)
                    results[key] = pd.DataFrame() if key in df_keys else {}

        # news_headlines 在主线程中执行（cache_db 是 SQLite，不支持跨线程）
        try:
            results["news_headlines"] = fetch_financial_news(
                date, backtest=is_backtest, cache_db=self.cache_db
            )
        except Exception as e:
            logger.warning("news_headlines 采集失败: %s", e)
            results["news_headlines"] = []

        # sentiment 依赖池数据，在并行完成后计算
        results["sentiment"] = calc_market_sentiment(
            results.get("limit_up_pool", pd.DataFrame()),
            results.get("failed_limit_pool", pd.DataFrame()),
            results.get("limit_down_pool", pd.DataFrame()),
        )
        results["date"] = date

        logger.info("========== %s 数据收集完成 ==========", date)
        return results

    def get_stock_history(self, code: str, days: int = 10,
                          reference_date: str | None = None) -> pd.DataFrame:
        """获取个股日线（cache-first）。reference_date 格式 YYYY-MM-DD。
        返回 (df, from_cache) 元组供 batch 方法判断是否需要 sleep。
        """
        ref = reference_date or "9999-12-31"

        # 1. 查缓存
        if self.cache_db:
            cached = self.cache_db.get_stock_cache(code, ref, days)
            if len(cached) >= days:
                df = pd.DataFrame(cached)
                df.rename(columns={
                    "trade_date": "日期", "open": "开盘", "high": "最高",
                    "low": "最低", "close": "收盘", "volume": "成交量",
                    "amount": "成交额",
                }, inplace=True)
                return df, True

        # 2. 调 AkShare
        df = self._fetch_stock_daily(code)
        if df is not None and not df.empty:
            # 写缓存
            if self.cache_db:
                self._write_cache(code, df)
            if reference_date:
                date_col = "日期" if "日期" in df.columns else "date"
                df = df[df[date_col].astype(str) <= reference_date]
            return df.tail(days), False

        return pd.DataFrame(), False

    def _write_cache(self, code: str, df: pd.DataFrame) -> None:
        """将 AkShare 返回的日线 DataFrame 写入缓存。"""
        date_col = "日期" if "日期" in df.columns else "date"
        open_col = "开盘" if "开盘" in df.columns else "open"
        high_col = "最高" if "最高" in df.columns else "high"
        low_col = "最低" if "最低" in df.columns else "low"
        close_col = "收盘" if "收盘" in df.columns else "close"
        vol_col = "成交量" if "成交量" in df.columns else "volume"
        amt_col = "成交额" if "成交额" in df.columns else "amount"

        rows = []
        for _, r in df.iterrows():
            rows.append({
                "trade_date": str(r[date_col])[:10],
                "open": float(r[open_col]) if pd.notna(r.get(open_col)) else None,
                "high": float(r[high_col]) if pd.notna(r.get(high_col)) else None,
                "low": float(r[low_col]) if pd.notna(r.get(low_col)) else None,
                "close": float(r[close_col]) if pd.notna(r.get(close_col)) else None,
                "volume": float(r[vol_col]) if pd.notna(r.get(vol_col)) else None,
                "amount": float(r.get(amt_col)) if amt_col in df.columns and pd.notna(r.get(amt_col)) else None,
            })
        try:
            self.cache_db.save_stock_cache(code, rows)
        except Exception as e:
            logger.debug("写入日线缓存失败 %s: %s", code, e)

    def _fetch_stock_daily(self, code: str) -> pd.DataFrame:
        """获取个股日线，东方财富失败时回退到新浪。带熔断：连续3次失败后跳过东方财富。"""
        # 主：东方财富（熔断后跳过）
        if self._em_fail_count < 3:
            try:
                df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
                if df is not None and not df.empty:
                    self._em_fail_count = 0
                    return df
            except Exception as e:
                self._em_fail_count += 1
                if self._em_fail_count == 3:
                    logger.info("东方财富日线连续失败3次，切换到新浪源")
                else:
                    logger.debug("东方财富日线失败 %s: %s", code, e)

        # 备：新浪（stock_zh_a_daily）
        try:
            prefix = "sh" if code.startswith("6") else "sz"
            df = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", adjust="qfq")
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning("获取 %s 日线失败（双源）: %s", code, e)

        return pd.DataFrame()

    def get_batch_stock_history(self, codes: list[str], days: int = 10,
                                reference_date: str | None = None) -> dict[str, pd.DataFrame]:
        result = {}
        api_count = 0
        cache_count = 0
        for i, code in enumerate(codes):
            df, from_cache = self.get_stock_history(code, days, reference_date=reference_date)
            if not df.empty:
                result[code] = df
            if from_cache:
                cache_count += 1
            else:
                api_count += 1
                if i < len(codes) - 1:
                    time.sleep(0.5)  # 仅对 API 调用限流
        logger.info(
            "批量获取日线完成, %d/%d 成功 (缓存命中 %d, API调用 %d)",
            len(result), len(codes), cache_count, api_count,
        )
        return result
