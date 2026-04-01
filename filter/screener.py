from __future__ import annotations

import logging

import pandas as pd

from data.collector import MarketDataCollector

logger = logging.getLogger("trading.filter.screener")


class StockScreener:
    def __init__(self, config: dict, collector: MarketDataCollector):
        self.cfg = config.get("screening", {})
        self.collector = collector

    def screen(self, market_data: dict) -> list[dict]:
        # 1. 构建股票池
        universe = self._build_universe(market_data)
        logger.info("股票池共 %d 只", len(universe))

        if not universe:
            logger.warning("股票池为空，无法筛选")
            return []

        # 2. 预筛选：用池自带数据快速排除，减少日线请求量
        pre_filtered = []
        for stock in universe:
            if self.cfg.get("exclude_st", True) and "ST" in stock.get("name", ""):
                continue
            tr = stock.get("turnover_rate", 0)
            if tr and not self._turnover_filter(tr):
                continue
            pre_filtered.append(stock)

        logger.info("预筛选: %d → %d 只", len(universe), len(pre_filtered))

        # 3. 获取日线数据（仅预筛选通过的）
        codes = [s["code"] for s in pre_filtered]
        ref_date = None
        if self.collector.mode == "backtest":
            d = market_data.get("date", "")
            ref_date = f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d) == 8 else None
        histories = self.collector.get_batch_stock_history(
            codes, days=10, reference_date=ref_date
        )

        # 4. 逐只过滤
        has_history = len(histories) > 0
        if not has_history:
            logger.warning("日线数据全部获取失败，使用涨停池/强势股池自带数据进行筛选")

        candidates = []
        for stock in pre_filtered:
            code = stock["code"]
            hist = histories.get(code)

            if hist is not None and not hist.empty:
                # 有日线数据：正常量能过滤
                if not self._volume_filter(hist):
                    continue

                turnover = stock.get("turnover_rate", 0)
                if not self._turnover_filter(turnover):
                    continue

                tech_note = self._technical_pattern(hist)
                volume_vs_5d = self._calc_volume_ratio(hist)
                close = float(hist.iloc[-1].get("收盘", hist.iloc[-1].get("close", 0)))
            else:
                # 无日线数据：用池自带的量比/换手率过滤
                pool_volume_ratio = stock.get("volume_ratio", 0)
                turnover = stock.get("turnover_rate", 0)

                if has_history:
                    # 部分日线可用，跳过无数据的
                    continue

                # 日线全部不可用时，用池自带数据做基本过滤
                vol_threshold = self.cfg.get("volume_ratio_min", 2.0)
                if pool_volume_ratio and pool_volume_ratio < vol_threshold:
                    continue
                if not self._turnover_filter(turnover):
                    continue

                tech_note = ""
                volume_vs_5d = pool_volume_ratio
                close = 0.0

            candidates.append({
                "code": code,
                "name": stock.get("name", ""),
                "close": close,
                "change_pct": stock.get("change_pct"),
                "volume_ratio": stock.get("volume_ratio", volume_vs_5d),
                "turnover_rate": turnover,
                "volume_vs_5d_avg": volume_vs_5d,
                "is_limit_up": stock.get("is_limit_up", False),
                "consecutive_boards": stock.get("consecutive_boards", 0),
                "on_dragon_tiger": stock.get("on_dragon_tiger", False),
                "industry": stock.get("industry", ""),
                "technical_note": tech_note or "",
                "sonnet_score": None,
                "sonnet_theme": None,
            })

        # 4. 过滤高连板（5+连板回撤风险极大）
        max_boards = self.cfg.get("max_consecutive_boards", 4)
        candidates = [
            c for c in candidates
            if c.get("consecutive_boards", 0) <= max_boards
        ]

        # 5. 排序: 连板数×3 + 量比 + 换手率×0.1（v1 公式）
        def _score(x):
            boards = x.get("consecutive_boards", 0)
            vol = x.get("volume_vs_5d_avg") or 0
            tr = x.get("turnover_rate") or 0
            return boards * 3 + vol + tr * 0.1

        candidates.sort(key=_score, reverse=True)

        max_count = self.cfg.get("max_candidates", 15)
        candidates = candidates[:max_count]

        # 5. 候选不足时放宽条件
        if len(candidates) < self.cfg.get("min_candidates", 5):
            logger.warning("候选股仅 %d 只，低于最低阈值", len(candidates))

        logger.info("初筛完成，输出 %d 只候选股", len(candidates))
        return candidates

    @staticmethod
    def _is_tradable(code: str) -> bool:
        """排除北交所（8xxxxx, 920xxx）和科创板（688xxx, 689xxx）。"""
        return not code.startswith(('688', '689', '8', '920'))

    def _build_universe(self, market_data: dict) -> list[dict]:
        universe = {}

        # 涨停池
        limit_up = market_data.get("limit_up_pool", pd.DataFrame())
        if not limit_up.empty:
            for _, row in limit_up.iterrows():
                code = str(row.get("代码", row.get("code", ""))).zfill(6)
                if not self._is_tradable(code):
                    continue
                if code not in universe:
                    universe[code] = {
                        "code": code,
                        "name": str(row.get("名称", row.get("name", ""))),
                        "change_pct": self._safe_float(row.get("涨跌幅", row.get("change_pct"))),
                        "turnover_rate": self._safe_float(row.get("换手率", row.get("turnover_rate"))),
                        "volume_ratio": self._safe_float(row.get("量比", row.get("volume_ratio"))),
                        "consecutive_boards": int(row.get("连板数", row.get("consecutive_boards", 1))),
                        "is_limit_up": True,
                        "on_dragon_tiger": False,
                        "industry": str(row.get("所属行业", row.get("industry", ""))),
                    }

        # 强势股池
        strong = market_data.get("strong_pool", pd.DataFrame())
        if not strong.empty:
            for _, row in strong.iterrows():
                code = str(row.get("代码", row.get("code", ""))).zfill(6)
                if not self._is_tradable(code):
                    continue
                if code not in universe:
                    universe[code] = {
                        "code": code,
                        "name": str(row.get("名称", row.get("name", ""))),
                        "change_pct": self._safe_float(row.get("涨跌幅", row.get("change_pct"))),
                        "turnover_rate": self._safe_float(row.get("换手率", row.get("turnover_rate"))),
                        "volume_ratio": self._safe_float(row.get("量比", row.get("volume_ratio"))),
                        "consecutive_boards": 0,
                        "is_limit_up": False,
                        "on_dragon_tiger": False,
                        "industry": str(row.get("所属行业", row.get("industry", ""))),
                    }

        # 龙虎榜
        dragon_tiger = market_data.get("dragon_tiger", pd.DataFrame())
        if not dragon_tiger.empty:
            for _, row in dragon_tiger.iterrows():
                code = str(row.get("代码", row.get("code", ""))).zfill(6)
                if not self._is_tradable(code):
                    continue
                if code in universe:
                    universe[code]["on_dragon_tiger"] = True
                else:
                    universe[code] = {
                        "code": code,
                        "name": str(row.get("名称", row.get("name", ""))),
                        "change_pct": self._safe_float(row.get("涨跌幅", row.get("change_pct"))),
                        "turnover_rate": 0,
                        "volume_ratio": 0,
                        "consecutive_boards": 0,
                        "is_limit_up": False,
                        "on_dragon_tiger": True,
                        "industry": "",
                    }

        return list(universe.values())

    def _volume_filter(self, hist: pd.DataFrame) -> bool:
        vol_col = "成交量" if "成交量" in hist.columns else "volume"
        if vol_col not in hist.columns or len(hist) < 2:
            return True  # 数据不足，不过滤

        volumes = hist[vol_col].astype(float)
        yesterday_vol = volumes.iloc[-1]
        avg_5d = volumes.iloc[-6:-1].mean() if len(volumes) >= 6 else volumes.iloc[:-1].mean()

        threshold = self.cfg.get("volume_vs_5d_avg_min", 2.0)
        return avg_5d > 0 and yesterday_vol / avg_5d >= threshold

    def _turnover_filter(self, turnover_rate: float) -> bool:
        if not turnover_rate:
            return True  # 数据缺失不过滤
        min_rate = self.cfg.get("turnover_rate_min", 3.0)
        max_rate = self.cfg.get("turnover_rate_max", 15.0)
        return min_rate <= turnover_rate <= max_rate

    def _technical_pattern(self, hist: pd.DataFrame) -> "str | None":
        close_col = "收盘" if "收盘" in hist.columns else "close"
        if close_col not in hist.columns or len(hist) < 5:
            return None

        close = hist[close_col].astype(float)
        patterns = []

        # MA5 金叉 MA20 (纯 pandas 实现)
        if len(close) >= 5:
            ma5 = close.rolling(window=5).mean()
            ma_long_len = min(20, len(close))
            ma20 = close.rolling(window=ma_long_len).mean()
            if ma5.iloc[-1] > ma20.iloc[-1] and ma5.iloc[-2] <= ma20.iloc[-2]:
                patterns.append("MA5金叉MA20")

        # MACD 金叉 (纯 pandas: EMA12 - EMA26, Signal = EMA9 of MACD)
        if len(close) >= 8:
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal = macd_line.ewm(span=9, adjust=False).mean()
            macd_hist = macd_line - signal
            if len(macd_hist) >= 2 and macd_hist.iloc[-1] > 0 and macd_hist.iloc[-2] <= 0:
                patterns.append("MACD金叉")

        # 放量突破 (昨日量超5日均量2倍 + 收阳线)
        vol_col = "成交量" if "成交量" in hist.columns else "volume"
        open_col = "开盘" if "开盘" in hist.columns else "open"
        if vol_col in hist.columns and open_col in hist.columns:
            if close.iloc[-1] > float(hist[open_col].iloc[-1]):
                vols = hist[vol_col].astype(float)
                if len(vols) >= 6:
                    avg5 = vols.iloc[-6:-1].mean()
                    if avg5 > 0 and vols.iloc[-1] / avg5 >= 2.0:
                        patterns.append("放量突破")

        return "、".join(patterns) if patterns else None

    def _calc_volume_ratio(self, hist: pd.DataFrame) -> float:
        vol_col = "成交量" if "成交量" in hist.columns else "volume"
        if vol_col not in hist.columns or len(hist) < 2:
            return 0.0
        volumes = hist[vol_col].astype(float)
        avg = volumes.iloc[:-1].mean()
        if avg > 0:
            return round(float(volumes.iloc[-1] / avg), 2)
        return 0.0

    @staticmethod
    def _safe_float(val) -> float:
        try:
            return float(val) if val is not None else 0.0
        except (ValueError, TypeError):
            return 0.0
