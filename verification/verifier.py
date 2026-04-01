# -*- coding: utf-8 -*-
"""T+1 超短线验证器。

操作逻辑:
  盘前: 基于 T-1 收盘数据生成推荐 (观察名单)
  T 日: 9:30 开盘后观察，开盘涨跌幅在 [entry_gap_min, entry_gap_max] 区间内才买入
  T+1:  卖出。报告最好情况(盘中最高)、最坏情况(盘中最低)、收盘价。

注意: T 日买入后无法当日卖出(T+1 制度)，所以不设 T 日止损。
"""

from __future__ import annotations

import logging
import time

import akshare as ak

from backtest.calendar import TradingCalendar
from storage.db import TradingDB

logger = logging.getLogger("trading.verification")


class Verifier:
    def __init__(self, db: TradingDB, config: dict):
        self.db = db
        self.calendar = TradingCalendar()
        vcfg = config.get("verification", {})
        self.win_threshold = vcfg.get("win_threshold_pct", 0.0)
        self.entry_gap_max = vcfg.get("entry_gap_max_pct", 3.0)
        self.entry_gap_min = vcfg.get("entry_gap_min_pct", -2.0)
        self._em_fail_count = 0

    def verify_date(self, rec_date: str, source: str = "live") -> list[dict]:
        """验证 rec_date 的推荐。

        时间线: rec_date(T-1分析) -> buy_date(T买入) -> sell_date(T+1卖出)
        """
        buy_date = self.calendar.next_trading_day(rec_date)
        if not buy_date:
            logger.warning("找不到 %s 的下一个交易日(买入日)", rec_date)
            return []

        sell_date = self.calendar.next_trading_day(buy_date)
        if not sell_date:
            logger.warning("找不到 %s 的下一个交易日(卖出日)", buy_date)
            return []

        recs = self.db.get_recommendations(rec_date, source)
        if not recs:
            logger.info("%s [%s] 无推荐记录", rec_date, source)
            return []

        results = []
        for rec in recs:
            code = rec["code"]
            try:
                # T-1 收盘价
                rec_close = self._get_rec_close(rec, code, rec_date, source)
                if not rec_close or rec_close <= 0:
                    logger.warning("%s 无法获取 T-1 收盘价", code)
                    continue

                # T 日 OHLC (买入日)
                buy_bar = self._fetch_day_bar(code, buy_date)
                if not buy_bar:
                    logger.warning("%s T日(%s) 无行情", code, buy_date)
                    continue

                # T+1 日 OHLC (卖出日)
                sell_bar = self._fetch_day_bar(code, sell_date)
                if not sell_bar:
                    logger.warning("%s T+1(%s) 无行情", code, sell_date)
                    continue

                buy_open, buy_high, buy_low, buy_close = buy_bar
                t1_open, t1_high, t1_low, t1_close = sell_bar

                # 入场缺口: T日开盘 vs T-1收盘
                entry_gap = (buy_open - rec_close) / rec_close * 100

                # 入场判定: 开盘涨跌幅在 [min, max] 区间内
                entry_feasible = self.entry_gap_min <= entry_gap < self.entry_gap_max

                # 买入价 = T日开盘价
                buy_price = buy_open

                # T 日当天表现 (买入后到收盘)
                buy_day_ret = (buy_close - buy_price) / buy_price * 100

                # T+1 各种情况 vs 买入价
                best_ret = (t1_high - buy_price) / buy_price * 100
                worst_ret = (t1_low - buy_price) / buy_price * 100
                close_ret = (t1_close - buy_price) / buy_price * 100
                open_ret_t1 = (t1_open - buy_price) / buy_price * 100

                # 胜负: T+1 收盘 vs 买入价
                win = close_ret >= self.win_threshold

                # 策略收益 = T+1 收盘卖出 (无止损，超短线持有到T+1收盘)
                if not entry_feasible:
                    strategy_ret = 0.0  # 不入场
                else:
                    strategy_ret = close_ret

                results.append({
                    "rec_date": rec_date,
                    "buy_date": buy_date,
                    "verify_date": sell_date,
                    "code": code,
                    "name": rec["name"],
                    "rec_close": round(rec_close, 2),
                    "buy_price": round(buy_price, 2),
                    "buy_open": buy_open,
                    "buy_high": buy_high,
                    "buy_low": buy_low,
                    "buy_close": buy_close,
                    "t1_open": t1_open,
                    "t1_high": t1_high,
                    "t1_low": t1_low,
                    "t1_close": t1_close,
                    "entry_gap_pct": round(entry_gap, 2),
                    "best_return_pct": round(best_ret, 2),
                    "worst_return_pct": round(worst_ret, 2),
                    "close_return_pct": round(close_ret, 2),
                    "buy_day_return_pct": round(buy_day_ret, 2),
                    "open_return_pct": round(open_ret_t1, 2),
                    "max_return_pct": round(best_ret, 2),
                    "min_return_pct": round(worst_ret, 2),
                    "win": 1 if win else 0,
                    "entry_feasible": 1 if entry_feasible else 0,
                    "strategy_return_pct": round(strategy_ret, 2),
                    "opus_score": rec.get("opus_score"),
                    "rank": rec.get("rank"),
                    "entry_strategy": rec.get("entry_strategy", ""),
                })
            except Exception as e:
                logger.warning("验证 %s 失败: %s", code, e)

            time.sleep(0.3)

        if results:
            self.db.save_verification_results(results, source)
            feasible = [r for r in results if r["entry_feasible"]]
            wins = sum(r["win"] for r in feasible)
            logger.info(
                "%s 验证: %d 推荐, %d 可入场, 胜 %d 负 %d (%.1f%%)",
                rec_date, len(results), len(feasible), wins,
                len(feasible) - wins,
                wins / len(feasible) * 100 if feasible else 0,
            )

        return results

    def verify_batch(self, start_date: str, end_date: str,
                     source: str = "live") -> dict:
        trading_days = self.calendar.get_trading_days(start_date, end_date)
        all_results = []
        for day in trading_days:
            results = self.verify_date(day, source)
            all_results.extend(results)

        stats = self._compute_statistics(all_results, start_date, end_date, source)
        if stats.get("total_recs", 0) > 0:
            self.db.save_verification_summary(stats)
        return stats

    # ------------------------------------------------------------------
    # 数据获取
    # ------------------------------------------------------------------

    def _fetch_stock_df(self, code: str):
        """获取个股日线，东方财富 → 新浪 双源。"""
        if self._em_fail_count < 3:
            try:
                df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
                if df is not None and not df.empty:
                    self._em_fail_count = 0
                    return df
            except Exception:
                self._em_fail_count += 1
                if self._em_fail_count == 3:
                    logger.info("东方财富连续失败3次，切换新浪源")
        try:
            prefix = "sh" if code.startswith("6") else "sz"
            df = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", adjust="qfq")
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning("获取 %s 日线失败(双源): %s", code, e)
        return None

    def _fetch_day_bar(self, code: str, date: str) -> tuple | None:
        df = self._fetch_stock_df(code)
        if df is None:
            return None
        try:
            date_col = "日期" if "日期" in df.columns else "date"
            formatted = f"{date[:4]}-{date[4:6]}-{date[6:]}"
            row = df[df[date_col].astype(str) == formatted]
            if row.empty:
                return None
            r = row.iloc[0]
            o = "开盘" if "开盘" in df.columns else "open"
            h = "最高" if "最高" in df.columns else "high"
            l = "最低" if "最低" in df.columns else "low"
            c = "收盘" if "收盘" in df.columns else "close"
            return (float(r[o]), float(r[h]), float(r[l]), float(r[c]))
        except Exception as e:
            logger.warning("获取 %s %s OHLC失败: %s", code, date, e)
            return None

    def _get_rec_close(self, rec: dict, code: str, rec_date: str,
                       source: str) -> float | None:
        cand = self.db.get_candidate(rec_date, code, source)
        if cand and cand.get("close") and float(cand["close"]) > 0:
            return float(cand["close"])
        df = self._fetch_stock_df(code)
        if df is not None and not df.empty:
            try:
                date_col = "日期" if "日期" in df.columns else "date"
                formatted = f"{rec_date[:4]}-{rec_date[4:6]}-{rec_date[6:]}"
                row = df[df[date_col].astype(str) == formatted]
                if not row.empty:
                    c = "收盘" if "收盘" in df.columns else "close"
                    return float(row.iloc[0][c])
            except Exception:
                pass
        return None

    # ------------------------------------------------------------------
    # 统计
    # ------------------------------------------------------------------

    def _compute_statistics(self, results: list[dict], start: str,
                            end: str, source: str) -> dict:
        if not results:
            return {
                "period_start": start, "period_end": end,
                "total_recs": 0, "total_feasible": 0,
                "win_count": 0, "loss_count": 0,
                "win_rate": 0, "avg_close_return": 0,
                "avg_best_return": 0, "avg_worst_return": 0,
                "avg_max_return": 0,
                "max_single_loss": 0, "max_single_gain": 0,
                "entry_feasible_rate": 0, "sharpe_like": 0,
                "source": source,
            }

        feasible = [r for r in results if r["entry_feasible"]]
        if not feasible:
            feasible = results

        n_total = len(results)
        n_feasible = len(feasible)

        close_rets = [r["close_return_pct"] for r in feasible]
        best_rets = [r["best_return_pct"] for r in feasible]
        worst_rets = [r["worst_return_pct"] for r in feasible]
        wins = sum(1 for r in feasible if r["win"])

        mean_close = sum(close_rets) / n_feasible
        mean_best = sum(best_rets) / n_feasible
        mean_worst = sum(worst_rets) / n_feasible

        variance = sum((x - mean_close) ** 2 for x in close_rets) / n_feasible
        std_ret = variance ** 0.5

        return {
            "period_start": start,
            "period_end": end,
            "total_recs": n_total,
            "total_feasible": n_feasible,
            "win_count": wins,
            "loss_count": n_feasible - wins,
            "win_rate": round(wins / n_feasible * 100, 2),
            "avg_close_return": round(mean_close, 4),
            "avg_best_return": round(mean_best, 4),
            "avg_worst_return": round(mean_worst, 4),
            "avg_max_return": round(mean_best, 4),
            "max_single_loss": round(min(worst_rets), 2),
            "max_single_gain": round(max(best_rets), 2),
            "entry_feasible_rate": round(n_feasible / n_total * 100, 2),
            "sharpe_like": round(mean_close / std_ret, 4) if std_ret > 0 else 0,
            "source": source,
        }
