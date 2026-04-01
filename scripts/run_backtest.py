#!/usr/bin/env python3
"""Run backtest with robust error handling."""
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.runner import BacktestRunner, BacktestMode
from utils.config_loader import load_config
from utils.logger import setup_logger

config = load_config()
setup_logger(config.get('paths', {}).get('log_dir', 'logs'))

runner = BacktestRunner(config, mode=BacktestMode.SONNET)

try:
    stats = runner.run('20260301', '20260328', skip_existing=True)
    print('\n=== Results ===')
    for k, v in stats.items():
        if isinstance(v, float):
            print(f'{k}: {v:.4f}')
        else:
            print(f'{k}: {v}')
except Exception as e:
    traceback.print_exc()
    print(f'\nFailed: {e}')
