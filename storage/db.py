from __future__ import annotations

import logging
import os
import sqlite3

logger = logging.getLogger("trading.storage")


class TradingDB:
    def __init__(self, db_path: str = "storage/trading.db"):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _init_tables(self):
        schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
        with open(schema_path, "r", encoding="utf-8") as f:
            self.conn.executescript(f.read())
        self._migrate_source_column()
        self.conn.commit()

    def _migrate_source_column(self):
        """为旧表添加 source 列（如果不存在）。"""
        for table in ("candidates", "recommendations"):
            cols = [r["name"] for r in self.conn.execute(f"PRAGMA table_info({table})")]
            if "source" not in cols:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN source TEXT DEFAULT 'live'")
                logger.info("已为 %s 表添加 source 列", table)

    # ------------------------------------------------------------------
    # 大盘数据
    # ------------------------------------------------------------------
    def save_daily_market(self, data: dict) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO daily_market
            (date, sh_index_close, sh_index_change, sz_index_close, sz_index_change,
             cyb_index_close, cyb_index_change, northbound_net, margin_balance,
             margin_change, limit_up_count, limit_down_count, failed_limit_rate,
             us_sp500_change, us_nasdaq_change, hk_hsi_change)
            VALUES (:date, :sh_index_close, :sh_index_change, :sz_index_close,
                    :sz_index_change, :cyb_index_close, :cyb_index_change,
                    :northbound_net, :margin_balance, :margin_change,
                    :limit_up_count, :limit_down_count, :failed_limit_rate,
                    :us_sp500_change, :us_nasdaq_change, :hk_hsi_change)""",
            data,
        )
        self.conn.commit()
        logger.info("已保存 %s 大盘数据", data.get("date"))

    def get_daily_market(self, date: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM daily_market WHERE date = ?", (date,)
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # 候选股
    # ------------------------------------------------------------------
    def save_candidates(self, date: str, candidates: list[dict],
                        source: str = "live") -> None:
        for c in candidates:
            self.conn.execute(
                """INSERT OR REPLACE INTO candidates
                (date, code, name, close, change_pct, volume_ratio, turnover_rate,
                 volume_vs_5d_avg, is_limit_up, consecutive_boards, on_dragon_tiger,
                 industry, sonnet_score, sonnet_theme, source)
                VALUES (:date, :code, :name, :close, :change_pct, :volume_ratio,
                        :turnover_rate, :volume_vs_5d_avg, :is_limit_up,
                        :consecutive_boards, :on_dragon_tiger, :industry,
                        :sonnet_score, :sonnet_theme, :source)""",
                {
                    "date": date,
                    "code": c.get("code", ""),
                    "name": c.get("name", ""),
                    "close": c.get("close"),
                    "change_pct": c.get("change_pct"),
                    "volume_ratio": c.get("volume_ratio"),
                    "turnover_rate": c.get("turnover_rate"),
                    "volume_vs_5d_avg": c.get("volume_vs_5d_avg"),
                    "is_limit_up": 1 if c.get("is_limit_up") else 0,
                    "consecutive_boards": c.get("consecutive_boards", 0),
                    "on_dragon_tiger": 1 if c.get("on_dragon_tiger") else 0,
                    "industry": c.get("industry", ""),
                    "sonnet_score": c.get("sonnet_score"),
                    "sonnet_theme": c.get("sonnet_theme"),
                    "source": source,
                },
            )
        self.conn.commit()
        logger.info("已保存 %s 候选股 %d 只 [%s]", date, len(candidates), source)

    def get_candidate(self, date: str, code: str,
                      source: str = "live") -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM candidates WHERE date = ? AND code = ? AND source = ?",
            (date, code, source),
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # 推荐股
    # ------------------------------------------------------------------
    def save_recommendations(self, date: str, recommendations: list[dict],
                             source: str = "live") -> None:
        for r in recommendations:
            self.conn.execute(
                """INSERT OR REPLACE INTO recommendations
                (date, code, name, rank, reason, risk_warning, entry_strategy,
                 opus_score, theme, position_pct, source)
                VALUES (:date, :code, :name, :rank, :reason, :risk_warning,
                        :entry_strategy, :opus_score, :theme, :position_pct, :source)""",
                {
                    "date": date,
                    "code": r.get("code", ""),
                    "name": r.get("name", ""),
                    "rank": r.get("rank"),
                    "reason": r.get("reason", ""),
                    "risk_warning": r.get("risk_warning", ""),
                    "entry_strategy": r.get("entry_strategy", ""),
                    "opus_score": r.get("opus_score"),
                    "theme": r.get("theme", ""),
                    "position_pct": r.get("position_pct"),
                    "source": source,
                },
            )
        self.conn.commit()
        logger.info("已保存 %s 推荐股 %d 只 [%s]", date, len(recommendations), source)

    def get_recommendations(self, date: str, source: str = "live") -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM recommendations WHERE date = ? AND source = ?",
            (date, source),
        ).fetchall()
        return [dict(r) for r in rows]

    def has_recommendations(self, date: str, source: str = "live") -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM recommendations WHERE date = ? AND source = ? LIMIT 1",
            (date, source),
        ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # 验证结果
    # ------------------------------------------------------------------
    def save_verification_results(self, results: list[dict],
                                  source: str = "live") -> None:
        for r in results:
            r["source"] = source
            self.conn.execute(
                """INSERT OR REPLACE INTO verification_results
                (rec_date, verify_date, code, name, rec_close,
                 t1_open, t1_high, t1_low, t1_close,
                 open_return_pct, max_return_pct, min_return_pct, close_return_pct,
                 win, entry_feasible, strategy_return_pct,
                 opus_score, rank, entry_strategy, source)
                VALUES (:rec_date, :verify_date, :code, :name, :rec_close,
                        :t1_open, :t1_high, :t1_low, :t1_close,
                        :open_return_pct, :max_return_pct, :min_return_pct,
                        :close_return_pct, :win, :entry_feasible,
                        :strategy_return_pct, :opus_score, :rank,
                        :entry_strategy, :source)""",
                r,
            )
        self.conn.commit()
        logger.info("已保存 %d 条验证结果 [%s]", len(results), source)

    def save_verification_summary(self, stats: dict) -> None:
        self.conn.execute(
            """INSERT INTO verification_summary
            (period_start, period_end, total_recs, win_count, loss_count,
             win_rate, avg_close_return, avg_max_return,
             max_single_loss, max_single_gain, entry_feasible_rate,
             sharpe_like, source)
            VALUES (:period_start, :period_end, :total_recs, :win_count,
                    :loss_count, :win_rate, :avg_close_return, :avg_max_return,
                    :max_single_loss, :max_single_gain, :entry_feasible_rate,
                    :sharpe_like, :source)""",
            stats,
        )
        self.conn.commit()

    def get_verification_results(self, start: str, end: str,
                                 source: str = "live") -> list[dict]:
        rows = self.conn.execute(
            """SELECT * FROM verification_results
            WHERE rec_date >= ? AND rec_date <= ? AND source = ?
            ORDER BY rec_date, rank""",
            (start, end, source),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 个股日线缓存
    # ------------------------------------------------------------------
    def get_stock_cache(self, code: str, ref_date: str,
                        days: int = 10) -> list[dict]:
        """查询缓存的日线数据。ref_date 格式 YYYY-MM-DD。"""
        rows = self.conn.execute(
            """SELECT trade_date, open, high, low, close, volume, amount
            FROM stock_daily_cache
            WHERE code = ? AND trade_date <= ?
            ORDER BY trade_date DESC LIMIT ?""",
            (code, ref_date, days),
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def save_stock_cache(self, code: str, rows: list[dict]) -> None:
        """批量写入日线缓存（忽略已存在的记录）。"""
        for r in rows:
            self.conn.execute(
                """INSERT OR IGNORE INTO stock_daily_cache
                (code, trade_date, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (code, r["trade_date"], r.get("open"), r.get("high"),
                 r.get("low"), r.get("close"), r.get("volume"), r.get("amount")),
            )
        self.conn.commit()

    # ------------------------------------------------------------------
    # AI 缓存
    # ------------------------------------------------------------------
    def get_ai_cache(self, date: str, stage: str,
                     prompt_hash: str) -> dict | None:
        row = self.conn.execute(
            """SELECT * FROM ai_cache
            WHERE date = ? AND stage = ? AND prompt_hash = ?""",
            (date, stage, prompt_hash),
        ).fetchone()
        return dict(row) if row else None

    def save_ai_cache(self, date: str, stage: str, prompt_hash: str,
                      model: str, response_json: str) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO ai_cache
            (date, stage, prompt_hash, model, response_json)
            VALUES (?, ?, ?, ?, ?)""",
            (date, stage, prompt_hash, model, response_json),
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # 新闻缓存
    # ------------------------------------------------------------------
    def save_news_items(self, items: list[dict]) -> int:
        """批量写入新闻（去重）。返回新增条数。"""
        before = self.conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
        for item in items:
            try:
                self.conn.execute(
                    """INSERT OR IGNORE INTO news_items
                    (news_date, news_time, source, title, content, url, category)
                    VALUES (:news_date, :news_time, :source, :title,
                            :content, :url, :category)""",
                    item,
                )
            except Exception:
                pass
        self.conn.commit()
        after = self.conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
        added = after - before
        logger.info("新闻入库: 提交 %d 条, 新增 %d 条", len(items), added)
        return added

    def get_news_by_date(self, date: str, limit: int = 50) -> list[dict]:
        """按日期查询新闻。date 格式 YYYY-MM-DD。"""
        rows = self.conn.execute(
            """SELECT news_date, news_time, source, title, content, url, category
            FROM news_items
            WHERE news_date = ?
            ORDER BY news_time DESC
            LIMIT ?""",
            (date, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_news_count_by_date(self, date: str) -> int:
        """查询某日新闻条数。"""
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM news_items WHERE news_date = ?",
            (date,),
        ).fetchone()
        return row["cnt"] if row else 0

    def clear_ai_cache(self) -> None:
        self.conn.execute("DELETE FROM ai_cache")
        self.conn.commit()
        logger.info("AI 缓存已清空")

    # ------------------------------------------------------------------
    def close(self):
        self.conn.close()
