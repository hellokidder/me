#!/usr/bin/env python3
"""Run backtest for a single day. Usage: python scripts/backtest_one_day.py 20260311"""
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.runner import BacktestRunner, BacktestMode
from utils.config_loader import load_config
from utils.logger import setup_logger

date = sys.argv[1] if len(sys.argv) > 1 else None
if not date:
    print("Usage: python scripts/backtest_one_day.py YYYYMMDD")
    sys.exit(1)

config = load_config()
setup_logger(config.get('paths', {}).get('log_dir', 'logs'))

runner = BacktestRunner(config, mode=BacktestMode.SONNET)

try:
    runner._run_single_day(date)
    print(f"OK: {date}")
except Exception as e:
    traceback.print_exc()
    print(f"FAIL: {date}: {e}")
finally:
    runner.db.close()
