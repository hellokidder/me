#!/usr/bin/env python3
"""Finish remaining backtest days using cached AI + RULES fallback.
Avoids making new API calls — uses existing AI cache or falls back to screener ranking.
"""
import json
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.runner import BacktestRunner, BacktestMode
from backtest.calendar import TradingCalendar
from utils.config_loader import load_config
from utils.logger import setup_logger
from storage.db import TradingDB
from data.collector import MarketDataCollector
from filter.screener import StockScreener
from evaluation.evaluator import PipelineEvaluator, Verdict

config = load_config()
setup_logger(config.get('paths', {}).get('log_dir', 'logs'))

db = TradingDB(config.get('paths', {}).get('db_path', 'storage/trading.db'))
cal = TradingCalendar()


def composite_score(c):
    ai = c.get("sonnet_score") or c.get("score") or 5  # default 5 for unscored
    boards = c.get("consecutive_boards") or 0
    vol = c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0
    change = abs(c.get("change_pct") or 0)
    penalty = 2.0 if change > 7 else 0
    return ai * 0.6 + boards * 1.5 + vol * 0.2 - penalty


def make_recs(candidates, top_n=4):
    """Create recommendation dicts from scored candidates."""
    # Filter 688/689/8xx/920
    filtered = [c for c in candidates
                if not c.get("code", "").startswith(("688", "689", "8", "920"))]
    filtered.sort(key=composite_score, reverse=True)
    recs = []
    for i, c in enumerate(filtered[:top_n], 1):
        recs.append({
            "rank": i,
            "code": c.get("code", ""),
            "name": c.get("name", ""),
            "opus_score": c.get("sonnet_score") or c.get("score") or 5,
            "theme": c.get("sonnet_theme") or c.get("matched_theme") or "",
            "reason": c.get("analysis", ""),
            "risk_warning": c.get("risk", ""),
            "entry_strategy": "观察开盘，涨超5%放弃",
            "position_pct": 15,
        })
    return recs


def fix_scored_candidates(date):
    """For days with AI cache but no sonnet_score in candidates: merge them."""
    cached = db.get_ai_cache(date, "sonnet", "")
    if cached:
        return  # This won't match because prompt_hash won't be ""

    # Find any sonnet cache for this date
    row = db.conn.execute(
        "SELECT response_json FROM ai_cache WHERE date = ? AND stage = 'sonnet'",
        (date,),
    ).fetchone()
    if not row:
        return None

    sonnet_result = json.loads(row[0])
    scored = sonnet_result.get("scored_candidates", [])
    scored_map = {c["code"]: c for c in scored}

    # Update candidates with sonnet scores
    cands = db.conn.execute(
        "SELECT * FROM candidates WHERE date = ? AND source = 'backtest'",
        (date,),
    ).fetchall()
    cands = [dict(r) for r in cands]

    for c in cands:
        s = scored_map.get(c["code"])
        if s:
            c["sonnet_score"] = s.get("score")
            c["sonnet_theme"] = s.get("matched_theme")

    db.save_candidates(date, cands, source="backtest")
    return cands


def run_data_and_screen(date):
    """Collect data + screen for a day (no AI calls)."""
    evaluator = PipelineEvaluator(config)
    collector = MarketDataCollector(config, mode="backtest", cache_db=db)
    market_data = collector.collect_all(date)

    evaluator.evaluate_collection(market_data)

    screener = StockScreener(config, collector)
    candidates = screener.screen(market_data)

    v = evaluator.evaluate_screening(candidates, market_data, attempt=0)
    if v.verdict == Verdict.RETRY:
        retry_config = {
            **config,
            "screening": {**config.get("screening", {}), **v.retry_params},
        }
        screener = StockScreener(retry_config, collector)
        candidates = screener.screen(market_data)

    db.save_candidates(date, candidates, source="backtest")
    return candidates


# Main logic
all_days = cal.get_trading_days('20260301', '20260328')
done = set(r[0] for r in db.conn.execute(
    "SELECT DISTINCT date FROM recommendations WHERE source='backtest' AND date >= '20260301'"
).fetchall())

remaining = [d for d in all_days if d not in done]
print(f"Remaining: {len(remaining)} days", flush=True)

for date in remaining:
    try:
        # Check if we have candidates
        cand_rows = db.conn.execute(
            "SELECT * FROM candidates WHERE date = ? AND source = 'backtest'",
            (date,),
        ).fetchall()

        if cand_rows:
            candidates = [dict(r) for r in cand_rows]
            # Check if we need to merge AI cache
            has_scores = any(c.get("sonnet_score") for c in candidates)
            if not has_scores:
                merged = fix_scored_candidates(date)
                if merged:
                    candidates = merged
                    print(f"  {date}: merged AI cache scores", flush=True)
        else:
            # Need to collect and screen
            print(f"  {date}: collecting data + screening...", flush=True)
            candidates = run_data_and_screen(date)
            time.sleep(2)

        if not candidates:
            db.save_recommendations(date, [], source="backtest")
            print(f"  {date}: no candidates, saved empty", flush=True)
            continue

        recs = make_recs(candidates)
        # Clear old recs
        db.conn.execute(
            "DELETE FROM recommendations WHERE date = ? AND source = 'backtest'",
            (date,),
        )
        db.conn.commit()
        db.save_recommendations(date, recs, source="backtest")
        print(f"  {date}: OK, {len(recs)} recs", flush=True)

    except Exception as e:
        print(f"  {date}: FAIL: {e}", flush=True)
        traceback.print_exc()

# Now regenerate ALL 20 days with composite formula
print("\nRegenerating ALL days with composite formula...", flush=True)
for date in all_days:
    cand_rows = db.conn.execute(
        "SELECT * FROM candidates WHERE date = ? AND source = 'backtest'",
        (date,),
    ).fetchall()
    if not cand_rows:
        continue
    candidates = [dict(r) for r in cand_rows]
    recs = make_recs(candidates)
    db.conn.execute(
        "DELETE FROM recommendations WHERE date = ? AND source = 'backtest'",
        (date,),
    )
    db.conn.commit()
    db.save_recommendations(date, recs, source="backtest")

print("Regeneration complete.", flush=True)

# Verify
print("\nRunning verification...", flush=True)
from verification.verifier import Verifier
verifier = Verifier(db, config)
stats = verifier.verify_batch('20260301', '20260328', source='backtest')

print('\n=== Round 2 Results ===', flush=True)
for k, v in stats.items():
    if isinstance(v, float):
        print(f'{k}: {v:.4f}', flush=True)
    else:
        print(f'{k}: {v}', flush=True)

db.close()
