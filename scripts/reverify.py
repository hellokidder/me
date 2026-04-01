# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""T+1 超短线验证 — 修正版

操作逻辑:
  T-1: 盘后分析，生成推荐(观察名单)
  T:   9:30 开盘观察，涨跌幅在 [gap_min, gap_max] 区间内买入
  T+1: 卖出，报告最好/最坏/收盘三种情况
  注意: T日买入后当天无法卖出，无止损
"""
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_loader import load_config
from utils.logger import setup_logger
from storage.db import TradingDB
from verification.verifier import Verifier

config = load_config()
setup_logger(config.get('paths', {}).get('log_dir', 'logs'))

db = TradingDB(config.get('paths', {}).get('db_path', 'storage/trading.db'))
vcfg = config.get("verification", {})
gap_min = vcfg.get("entry_gap_min_pct", -2.0)
gap_max = vcfg.get("entry_gap_max_pct", 3.0)

# 清除旧结果
print("=" * 60)
print("T+1 超短线回测验证")
print("=" * 60)
print(f"入场条件: T日开盘涨跌幅在 [{gap_min}%, +{gap_max}%) 区间")
print(f"买入价:   T日开盘价")
print(f"卖出:     T+1日 (报告最好/最坏/收盘)")
print(f"止损:     无 (T日无法卖出)")
print()

db.conn.execute("DELETE FROM verification_results WHERE source = 'backtest'")
db.conn.execute("DELETE FROM verification_summary WHERE source = 'backtest'")
db.conn.commit()

verifier = Verifier(db, config)
stats = verifier.verify_batch('20260301', '20260328', source='backtest')

# 读取结果
rows = db.conn.execute("""
    SELECT * FROM verification_results
    WHERE source = 'backtest' AND rec_date >= '20260301'
    ORDER BY rec_date, rank
""").fetchall()
results = [dict(r) for r in rows]
feasible = [r for r in results if r.get("entry_feasible")]
skipped = [r for r in results if not r.get("entry_feasible")]

print(f"\n{'='*60}")
print(f"总推荐: {len(results)}")
print(f"可入场: {len(feasible)} ({len(feasible)/len(results)*100:.0f}%)")
print(f"放弃:   {len(skipped)} ({len(skipped)/len(results)*100:.0f}%)")

if not feasible:
    print("无可入场推荐")
    db.close()
    sys.exit()

# === T+1 表现 ===
best_rets = [r["best_return_pct"] for r in feasible]
worst_rets = [r["worst_return_pct"] for r in feasible]
close_rets = [r["close_return_pct"] for r in feasible]
open_rets = [r["open_return_pct"] for r in feasible]
buy_day_rets = [r["buy_day_return_pct"] for r in feasible]

wins_close = sum(1 for r in close_rets if r >= 0)
wins_open = sum(1 for r in open_rets if r >= 0)

print(f"\n--- T+1 表现 (基于 T日开盘买入价) ---")
print(f"最好情况(盘中最高):  平均 {sum(best_rets)/len(best_rets):+.2f}%")
print(f"最坏情况(盘中最低):  平均 {sum(worst_rets)/len(worst_rets):+.2f}%")
print(f"T+1 开盘:           平均 {sum(open_rets)/len(open_rets):+.2f}%")
print(f"T+1 收盘:           平均 {sum(close_rets)/len(close_rets):+.2f}%")
print(f"T日当天(买入到收盘): 平均 {sum(buy_day_rets)/len(buy_day_rets):+.2f}%")

print(f"\n--- 胜率 ---")
print(f"T+1 收盘不亏(>=0%): {wins_close}/{len(feasible)} = {wins_close/len(feasible)*100:.1f}%")
print(f"T+1 开盘不亏(>=0%): {wins_open}/{len(feasible)} = {wins_open/len(feasible)*100:.1f}%")

# 盘中命中率
print(f"\n--- T+1 盘中最高命中率 ---")
for pct in [1, 2, 3, 5, 7]:
    hit = sum(1 for r in feasible if r["best_return_pct"] >= pct)
    print(f"  涨 >= {pct}%: {hit}/{len(feasible)} = {hit/len(feasible)*100:.1f}%")

print(f"\n--- T+1 盘中最低风险 ---")
for pct in [1, 2, 3, 5, 7]:
    hit = sum(1 for r in feasible if r["worst_return_pct"] <= -pct)
    print(f"  跌 >= {pct}%: {hit}/{len(feasible)} = {hit/len(feasible)*100:.1f}%")

# === 按 Rank ===
print(f"\n--- 按 Rank ---")
by_rank = defaultdict(list)
for r in feasible:
    by_rank[r.get("rank", 0)].append(r)

print(f"{'Rank':>4} {'数量':>4} {'T+1最好':>8} {'T+1最坏':>8} {'T+1收盘':>8} {'T日表现':>8} {'收盘>=0%':>8}")
for rank in sorted(by_rank.keys()):
    rs = by_rank[rank]
    avg_best = sum(r["best_return_pct"] for r in rs) / len(rs)
    avg_worst = sum(r["worst_return_pct"] for r in rs) / len(rs)
    avg_close = sum(r["close_return_pct"] for r in rs) / len(rs)
    avg_buy = sum(r["buy_day_return_pct"] for r in rs) / len(rs)
    wr = sum(1 for r in rs if r["close_return_pct"] >= 0) / len(rs) * 100
    print(f"{rank:>4} {len(rs):>4} {avg_best:>+8.2f}% {avg_worst:>+8.2f}% {avg_close:>+8.02f}% {avg_buy:>+8.2f}% {wr:>7.0f}%")

# === 按评分 ===
print(f"\n--- 按评分 ---")
high = [r for r in feasible if (r.get("opus_score") or 0) >= 8]
low = [r for r in feasible if (r.get("opus_score") or 0) < 8]
for label, group in [("高分(>=8)", high), ("低分(<8)", low)]:
    if group:
        avg_best = sum(r["best_return_pct"] for r in group) / len(group)
        avg_worst = sum(r["worst_return_pct"] for r in group) / len(group)
        avg_close = sum(r["close_return_pct"] for r in group) / len(group)
        wr = sum(1 for r in group if r["close_return_pct"] >= 0) / len(group) * 100
        print(f"  {label}: {len(group)}只, 最好 {avg_best:+.2f}%, 最坏 {avg_worst:+.2f}%, "
              f"收盘 {avg_close:+.2f}%, 不亏率 {wr:.0f}%")

# === 按 T日表现分组 ===
print(f"\n--- T日表现 vs T+1结果 ---")
print("(T日买入后到收盘的涨跌 → 对T+1的预示)")
t_buckets = defaultdict(list)
for r in feasible:
    bd = r["buy_day_return_pct"]
    if bd < -3:
        t_buckets["T日大跌(<-3%)"].append(r)
    elif bd < 0:
        t_buckets["T日小跌(0~-3%)"].append(r)
    elif bd < 3:
        t_buckets["T日小涨(0~3%)"].append(r)
    elif bd < 7:
        t_buckets["T日强势(3~7%)"].append(r)
    else:
        t_buckets["T日涨停(>=7%)"].append(r)

for label in ["T日大跌(<-3%)", "T日小跌(0~-3%)", "T日小涨(0~3%)", "T日强势(3~7%)", "T日涨停(>=7%)"]:
    group = t_buckets.get(label, [])
    if group:
        avg_best = sum(r["best_return_pct"] for r in group) / len(group)
        avg_worst = sum(r["worst_return_pct"] for r in group) / len(group)
        avg_close = sum(r["close_return_pct"] for r in group) / len(group)
        wr = sum(1 for r in group if r["close_return_pct"] >= 0) / len(group) * 100
        print(f"  {label}: {len(group)}只, T+1最好 {avg_best:+.2f}%, T+1收盘 {avg_close:+.2f}%, 不亏率 {wr:.0f}%")

# === 逐日明细 ===
print(f"\n--- 逐日明细 (可入场) ---")
print(f"{'分析日':<10} {'买入日':<10} {'代码':<8} {'名称':<8} {'入场Gap':>7} {'T日表现':>7} {'T+1最好':>7} {'T+1最坏':>7} {'T+1收盘':>7}")
for r in feasible:
    print(f"{r['rec_date']:<10} {r.get('buy_date',''):<10} {r['code']:<8} {r['name']:<8} "
          f"{r['entry_gap_pct']:>+6.1f}% {r['buy_day_return_pct']:>+6.1f}% "
          f"{r['best_return_pct']:>+6.1f}% {r['worst_return_pct']:>+6.1f}% {r['close_return_pct']:>+6.1f}%")

db.close()
print(f"\n{'='*60}")
print("完成")
