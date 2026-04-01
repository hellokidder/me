#!/usr/bin/env python3
"""Run remaining backtest days sequentially."""
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.runner import BacktestRunner, BacktestMode
from backtest.calendar import TradingCalendar
from utils.config_loader import load_config
from utils.logger import setup_logger
from storage.db import TradingDB

config = load_config()
setup_logger(config.get('paths', {}).get('log_dir', 'logs'))

db = TradingDB(config.get('paths', {}).get('db_path', 'storage/trading.db'))
cal = TradingCalendar()

# Find remaining days
all_days = cal.get_trading_days('20260301', '20260328')
done = set()
for r in db.conn.execute("SELECT DISTINCT date FROM recommendations WHERE source='backtest' AND date >= '20260301'").fetchall():
    done.add(r[0])

remaining = [d for d in all_days if d not in done]
print(f"Remaining: {len(remaining)} days: {remaining}", flush=True)

runner = BacktestRunner(config, mode=BacktestMode.SONNET)

for d in remaining:
    try:
        print(f"Running {d}...", flush=True)
        runner._run_single_day(d)
        print(f"OK: {d}", flush=True)
    except Exception as e:
        print(f"FAIL: {d}: {e}", flush=True)
        traceback.print_exc()

# Now regenerate ALL days with composite formula
print("\nRegenerating all recommendations with composite formula...", flush=True)
os.system("python3 scripts/regen_recs.py")

# Run verification
print("\nRunning verification...", flush=True)
from verification.verifier import Verifier
verifier = Verifier(runner.db, config)
stats = verifier.verify_batch('20260301', '20260328', source='backtest')

print('\n=== Round 2 Results ===', flush=True)
for k, v in stats.items():
    if isinstance(v, float):
        print(f'{k}: {v:.4f}', flush=True)
    else:
        print(f'{k}: {v}', flush=True)

runner.db.close()
