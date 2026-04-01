"""T+1 验证器：用实际行情检验推荐效果。"""

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
        self.win_threshold = vcfg.get("win_threshold_pct", 2.0)
        self.entry_gap_max = vcfg.get("entry_gap_max_pct", 5.0)
        self.stop_loss_pct = vcfg.get("stop_loss_pct", 3.0)
        self._em_fail_count = 0

    def verify_date(self, rec_date: str, source: str = "live") -> list[dict]:
        """验证 rec_date 的推荐在 T+1 的实际表现。"""
        t1_date = self.calendar.next_trading_day(rec_date)
        if not t1_date:
            logger.warning("找不到 %s 的下一个交易日", rec_date)
            return []

        recs = self.db.get_recommendations(rec_date, source)
        if not recs:
            logger.info("%s [%s] 无推荐记录", rec_date, source)
            return []

        results = []
        for rec in recs:
            code = rec["code"]
            try:
                bar = self._fetch_day_bar(code, t1_date)
                if not bar:
                    logger.warning("%s T+1(%s) 无行情数据", code, t1_date)
                    continue

                # 获取 T 日收盘价
                rec_close = self._get_rec_close(rec, code, rec_date, source)
                if not rec_close or rec_close <= 0:
                    logger.warning("%s 无法获取推荐日收盘价", code)
                    continue

                t1_open, t1_high, t1_low, t1_close = bar

                open_ret = (t1_open - rec_close) / rec_close * 100
                max_ret = (t1_high - rec_close) / rec_close * 100
                min_ret = (t1_low - rec_close) / rec_close * 100
                close_ret = (t1_close - rec_close) / rec_close * 100

                entry_feasible = open_ret < self.entry_gap_max
                win = close_ret >= self.win_threshold

                strategy_ret = self._simulate_strategy(
                    rec_close, t1_open, t1_high, t1_low, t1_close, open_ret
                )

                results.append({
                    "rec_date": rec_date,
                    "verify_date": t1_date,
                    "code": code,
                    "name": rec["name"],
                    "rec_close": round(rec_close, 2),
                    "t1_open": t1_open,
                    "t1_high": t1_high,
                    "t1_low": t1_low,
                    "t1_close": t1_close,
                    "open_return_pct": round(open_ret, 2),
                    "max_return_pct": round(max_ret, 2),
                    "min_return_pct": round(min_ret, 2),
                    "close_return_pct": round(close_ret, 2),
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
            wins = sum(r["win"] for r in results)
            logger.info(
                "%s 验证完成: %d 只, 胜 %d 负 %d (胜率 %.1f%%)",
                rec_date, len(results), wins, len(results) - wins,
                wins / len(results) * 100 if results else 0,
            )

        return results

    def verify_batch(self, start_date: str, end_date: str,
                     source: str = "live") -> dict:
        """批量验证日期范围内所有推荐。"""
        trading_days = self.calendar.get_trading_days(start_date, end_date)
        all_results = []

        for day in trading_days:
            results = self.verify_date(day, source)
            all_results.extend(results)

        stats = self._compute_statistics(all_results, start_date, end_date, source)
        if stats.get("total_recs", 0) > 0:
            self.db.save_verification_summary(stats)
        return stats

    def _fetch_stock_df(self, code: str):
        """获取个股全量日线，东方财富失败回退新浪。带熔断。"""
        if self._em_fail_count < 3:
            try:
                df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
                if df is not None and not df.empty:
                    self._em_fail_count = 0
                    return df
            except Exception:
                self._em_fail_count += 1
                if self._em_fail_count == 3:
                    logger.info("验证器：东方财富连续失败3次，切换到新浪源")
        try:
            prefix = "sh" if code.startswith("6") else "sz"
            df = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", adjust="qfq")
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning("获取 %s 日线失败（双源）: %s", code, e)
        return None

    def _fetch_day_bar(self, code: str, date: str) -> tuple | None:
        """获取某日 OHLC。"""
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
            o_col = "开盘" if "开盘" in df.columns else "open"
            h_col = "最高" if "最高" in df.columns else "high"
            l_col = "最低" if "最低" in df.columns else "low"
            c_col = "收盘" if "收盘" in df.columns else "close"
            return (float(r[o_col]), float(r[h_col]),
                    float(r[l_col]), float(r[c_col]))
        except Exception as e:
            logger.warning("获取 %s %s 行情失败: %s", code, date, e)
            return None

    def _get_rec_close(self, rec: dict, code: str, rec_date: str,
                       source: str) -> float | None:
        """获取推荐日 T 的收盘价。"""
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
                    c_col = "收盘" if "收盘" in df.columns else "close"
                    return float(row.iloc[0][c_col])
            except Exception:
                pass
        return None

    def _simulate_strategy(self, rec_close: float, t1_open: float,
                           t1_high: float, t1_low: float, t1_close: float,
                           open_ret: float) -> float:
        """简单策略模拟：高开>=5%不入场，否则开盘买，3%止损。"""
        if open_ret >= self.entry_gap_max:
            return 0.0

        buy_price = t1_open
        stop_price = buy_price * (1 - self.stop_loss_pct / 100)

        if t1_low <= stop_price:
            return (stop_price - rec_close) / rec_close * 100

        return (t1_close - rec_close) / rec_close * 100

    def _compute_statistics(self, results: list[dict], start: str,
                            end: str, source: str) -> dict:
        if not results:
            return {
                "period_start": start, "period_end": end,
                "total_recs": 0, "win_count": 0, "loss_count": 0,
                "win_rate": 0, "avg_close_return": 0, "avg_max_return": 0,
                "max_single_loss": 0, "max_single_gain": 0,
                "entry_feasible_rate": 0, "sharpe_like": 0, "source": source,
            }

        close_rets = [r["close_return_pct"] for r in results]
        max_rets = [r["max_return_pct"] for r in results]
        n = len(results)
        wins = sum(1 for r in results if r["win"])

        mean_ret = sum(close_rets) / n
        variance = sum((x - mean_ret) ** 2 for x in close_rets) / n
        std_ret = variance ** 0.5

        return {
            "period_start": start,
            "period_end": end,
            "total_recs": n,
            "win_count": wins,
            "loss_count": n - wins,
            "win_rate": round(wins / n * 100, 2),
            "avg_close_return": round(mean_ret, 4),
            "avg_max_return": round(sum(max_rets) / n, 4),
            "max_single_loss": round(min(close_rets), 2),
            "max_single_gain": round(max(close_rets), 2),
            "entry_feasible_rate": round(
                sum(r["entry_feasible"] for r in results) / n * 100, 2
            ),
            "sharpe_like": round(mean_ret / std_ret, 4) if std_ret > 0 else 0,
            "source": source,
        }
