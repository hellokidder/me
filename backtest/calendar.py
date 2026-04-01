"""A股交易日历，基于新浪历史交易日数据。"""

from __future__ import annotations

import logging
from functools import lru_cache

import akshare as ak

logger = logging.getLogger("trading.backtest.calendar")


class TradingCalendar:
    def __init__(self):
        self._dates: list[str] = []
        self._load()

    def _load(self):
        try:
            df = ak.tool_trade_date_hist_sina()
            self._dates = sorted(
                d.replace("-", "") for d in df["trade_date"].astype(str).tolist()
            )
            logger.info("加载交易日历成功，共 %d 个交易日", len(self._dates))
        except Exception as e:
            logger.error("加载交易日历失败: %s", e)
            self._dates = []

    def is_trading_day(self, date: str) -> bool:
        return date in self._date_set

    def get_trading_days(self, start: str, end: str) -> list[str]:
        return [d for d in self._dates if start <= d <= end]

    def next_trading_day(self, date: str) -> str | None:
        for d in self._dates:
            if d > date:
                return d
        return None

    def prev_trading_day(self, date: str) -> str | None:
        prev = None
        for d in self._dates:
            if d >= date:
                return prev
            prev = d
        return prev

    @property
    def _date_set(self) -> set:
        return set(self._dates)
