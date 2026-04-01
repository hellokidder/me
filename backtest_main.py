"""回测系统入口。"""

import argparse
import sys

from utils.config_loader import load_config
from utils.logger import setup_logger
from backtest.runner import BacktestRunner, BacktestMode
from verification.verifier import Verifier
from storage.db import TradingDB


def main():
    parser = argparse.ArgumentParser(description="A股超短线回测系统")
    parser.add_argument("--start", required=True, help="起始日期 YYYYMMDD")
    parser.add_argument("--end", required=True, help="结束日期 YYYYMMDD")
    parser.add_argument(
        "--mode", choices=["rules", "sonnet", "full"],
        default="sonnet", help="回测模式 (default: sonnet)"
    )
    parser.add_argument("--no-skip", action="store_true",
                        help="不跳过已有结果的日期")
    parser.add_argument("--verify-only", action="store_true",
                        help="仅验证已有推荐结果")
    parser.add_argument("--stats", action="store_true",
                        help="仅查看已有统计")

    args = parser.parse_args()
    config = load_config()
    setup_logger(config.get("paths", {}).get("log_dir", "logs"))

    if args.stats:
        _show_stats(config, args.start, args.end)
        return

    if args.verify_only:
        _verify_only(config, args.start, args.end)
        return

    mode = BacktestMode(args.mode)
    runner = BacktestRunner(config, mode=mode)
    stats = runner.run(args.start, args.end, skip_existing=not args.no_skip)
    _print_stats(stats)


def _verify_only(config: dict, start: str, end: str):
    paths = config.get("paths", {})
    db = TradingDB(paths.get("db_path", "storage/trading.db"))
    verifier = Verifier(db, config)
    stats = verifier.verify_batch(start, end, source="backtest")
    db.close()
    _print_stats(stats)


def _show_stats(config: dict, start: str, end: str):
    paths = config.get("paths", {})
    db = TradingDB(paths.get("db_path", "storage/trading.db"))
    results = db.get_verification_results(start, end, source="backtest")
    db.close()

    if not results:
        print("无回测验证数据")
        return

    wins = sum(1 for r in results if r["win"])
    n = len(results)
    close_rets = [r["close_return_pct"] for r in results]
    mean_ret = sum(close_rets) / n

    print(f"期间: {start} ~ {end}")
    print(f"总推荐数: {n}")
    print(f"胜率: {wins/n*100:.1f}%")
    print(f"平均收益: {mean_ret:.2f}%")
    print(f"最大盈利: {max(close_rets):.2f}%")
    print(f"最大亏损: {min(close_rets):.2f}%")


def _print_stats(stats: dict):
    if stats.get("total_recs", 0) == 0:
        print("无验证数据")
        return
    print(f"\n===== 回测统计 =====")
    print(f"总推荐数: {stats['total_recs']}")
    print(f"胜率: {stats['win_rate']:.1f}%")
    print(f"平均收益: {stats['avg_close_return']:.2f}%")
    print(f"平均最大收益: {stats['avg_max_return']:.2f}%")
    print(f"最大单笔亏损: {stats['max_single_loss']:.2f}%")
    print(f"最大单笔盈利: {stats['max_single_gain']:.2f}%")
    print(f"可入场率: {stats['entry_feasible_rate']:.1f}%")
    print(f"Sharpe-like: {stats['sharpe_like']:.4f}")


if __name__ == "__main__":
    main()
