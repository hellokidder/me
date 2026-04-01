#!/usr/bin/env python3
"""Regenerate recommendations from existing candidates using composite formula.
Does NOT make API calls — uses pre-existing Sonnet scores from candidates table.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.db import TradingDB
from filter.screener import StockScreener


def composite_score(c):
    """Composite v3: AI*0.6 + boards*1.5 + vol*0.2 - 高涨幅惩罚"""
    ai = c.get("sonnet_score") or 0
    boards = c.get("consecutive_boards") or 0
    vol = c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0
    change = abs(c.get("change_pct") or 0)
    penalty = 2.0 if change > 7 else 0
    return ai * 0.6 + boards * 1.5 + vol * 0.2 - penalty


def regen_for_date(db, date):
    """Regenerate recommendations for a single date."""
    rows = db.conn.execute(
        "SELECT * FROM candidates WHERE date = ? AND source = 'backtest' AND sonnet_score IS NOT NULL",
        (date,),
    ).fetchall()
    if not rows:
        return 0

    candidates = [dict(r) for r in rows]

    # Filter 688/689/8xx/920
    candidates = [c for c in candidates
                  if not c["code"].startswith(("688", "689", "8", "920"))]

    # Sort by composite score
    candidates.sort(key=composite_score, reverse=True)

    # Take top 4 (Round 3: 精简推荐)
    recs = []
    for i, c in enumerate(candidates[:4], 1):
        recs.append({
            "rank": i,
            "code": c["code"],
            "name": c["name"],
            "opus_score": c.get("sonnet_score", 0),
            "theme": c.get("sonnet_theme", ""),
            "reason": "",
            "risk_warning": "",
            "entry_strategy": "观察开盘，涨超5%放弃",
            "position_pct": 15,
        })

    # Clear old recs for this date
    db.conn.execute(
        "DELETE FROM recommendations WHERE date = ? AND source = 'backtest'",
        (date,),
    )
    db.conn.commit()
    db.save_recommendations(date, recs, source="backtest")
    return len(recs)


def main():
    db = TradingDB("storage/trading.db")

    # Find all dates with sonnet scores
    rows = db.conn.execute("""
        SELECT DISTINCT date FROM candidates
        WHERE source = 'backtest' AND date >= '20260301' AND sonnet_score IS NOT NULL
        ORDER BY date
    """).fetchall()

    dates = [r[0] for r in rows]
    print(f"Found {len(dates)} dates with Sonnet scores")

    total = 0
    for date in dates:
        n = regen_for_date(db, date)
        print(f"  {date}: {n} recs")
        total += n

    print(f"\nRegenerated {total} recommendations for {len(dates)} dates")
    db.close()


if __name__ == "__main__":
    main()
