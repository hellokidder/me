# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""3月完整回测 — 当前框架

逻辑:
  T-1: 盘后分析生成观察名单(基于 AI+规则复合排序 top4)
  T:   开盘观察，涨跌幅在 [-2%, +3%) 区间内买入
  T+1: 卖出，无止损
  胜负: T+1 收盘 >= 0% 不亏即赢
"""
import os, sys, json, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_loader import load_config
from utils.logger import setup_logger
from storage.db import TradingDB
from backtest.calendar import TradingCalendar
from data.collector import MarketDataCollector
from filter.screener import StockScreener
from evaluation.evaluator import PipelineEvaluator, Verdict
from verification.verifier import Verifier

config = load_config()
setup_logger(config.get('paths', {}).get('log_dir', 'logs'))
db = TradingDB(config.get('paths', {}).get('db_path', 'storage/trading.db'))
cal = TradingCalendar()

SOURCE = 'backtest_march'
START, END = '20260301', '20260328'
all_days = cal.get_trading_days(START, END)

vcfg = config.get("verification", {})
print("=" * 70)
print("3月完整回测")
print("=" * 70)
print(f"区间: {START} ~ {END} ({len(all_days)} 个交易日)")
print(f"入场: T日开盘 gap 在 [{vcfg.get('entry_gap_min_pct',-2)}%, +{vcfg.get('entry_gap_max_pct',3)}%) 内")
print(f"卖出: T+1 收盘")
print(f"胜负: T+1 收盘 >= 0%")
print(f"止损: 无")
print()

# 清理
for table in ('candidates', 'recommendations', 'verification_results'):
    db.conn.execute(f"DELETE FROM {table} WHERE source = ?", (SOURCE,))
db.conn.execute("DELETE FROM verification_summary WHERE source = ?", (SOURCE,))
db.conn.commit()


def composite_score(c):
    ai = (c.get("sonnet_score") or 5) * 0.6
    boards = (c.get("consecutive_boards") or 0) * 1.5
    vol = (c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0) * 0.2
    change = abs(c.get("change_pct") or 0)
    penalty = 2.0 if change > 7 else 0
    return ai + boards + vol - penalty


# === Phase 1: 采集 + 筛选 + 推荐 ===
print("Phase 1: 生成推荐")
print("-" * 70)

for i, date in enumerate(all_days):
    # 采集
    collector = MarketDataCollector(config, mode="backtest", cache_db=db)
    market_data = collector.collect_all(date)

    # 筛选
    screener = StockScreener(config, collector)
    candidates = screener.screen(market_data)

    evaluator = PipelineEvaluator(config)
    v = evaluator.evaluate_screening(candidates, market_data, attempt=0)
    if v.verdict == Verdict.RETRY:
        retry_config = {**config, "screening": {**config.get("screening", {}), **v.retry_params}}
        screener = StockScreener(retry_config, collector)
        candidates = screener.screen(market_data)

    # 从 AI 缓存补充 Sonnet 评分
    row = db.conn.execute(
        "SELECT response_json FROM ai_cache WHERE date = ? AND stage = 'sonnet'",
        (date,),
    ).fetchone()
    if row:
        sonnet_result = json.loads(row[0])
        scored_map = {c["code"]: c for c in sonnet_result.get("scored_candidates", [])}
        for cand in candidates:
            scored = scored_map.get(cand["code"])
            if scored:
                cand["sonnet_score"] = scored.get("score")
                cand["sonnet_theme"] = scored.get("matched_theme")

    db.save_candidates(date, candidates, source=SOURCE)

    if not candidates:
        db.save_recommendations(date, [], source=SOURCE)
        print(f"  [{i+1:>2}/{len(all_days)}] {date}: 无候选")
        continue

    # 过滤 + 排序 + top4
    filtered = [c for c in candidates
                if not c.get("code", "").startswith(("688", "689", "8", "920"))]
    filtered.sort(key=composite_score, reverse=True)

    recs = []
    for j, c in enumerate(filtered[:4], 1):
        recs.append({
            "rank": j, "code": c.get("code", ""), "name": c.get("name", ""),
            "opus_score": c.get("sonnet_score") or 5,
            "theme": c.get("sonnet_theme") or c.get("industry", ""),
            "reason": "", "risk_warning": "",
            "entry_strategy": "9:30-10:00观察，开盘涨幅-2%~+3%区间可入场",
            "position_pct": 20,
        })
    db.save_recommendations(date, recs, source=SOURCE)

    names = ", ".join(f"{r['name']}({r['code']})" for r in recs)
    has_ai = "AI" if row else "规则"
    print(f"  [{i+1:>2}/{len(all_days)}] {date} [{has_ai}]: {names}")

    if i < len(all_days) - 1:
        time.sleep(0.5)

# === Phase 2: 验证 ===
print(f"\nPhase 2: 验证")
print("-" * 70)
verifier = Verifier(db, config)
stats = verifier.verify_batch(START, END, source=SOURCE)

# === Phase 3: 结果 ===
rows = db.conn.execute("""
    SELECT v.*, c.consecutive_boards, c.volume_vs_5d_avg as cand_vol, c.change_pct as t1_change
    FROM verification_results v
    LEFT JOIN candidates c ON v.rec_date = c.date AND v.code = c.code AND c.source = ?
    WHERE v.source = ? ORDER BY v.rec_date, v.rank
""", (SOURCE, SOURCE)).fetchall()
results = [dict(r) for r in rows]
feasible = [r for r in results if r.get("entry_feasible")]
skipped = [r for r in results if not r.get("entry_feasible")]

print(f"\n{'='*70}")
print("3月回测结果")
print("=" * 70)

print(f"\n总推荐: {len(results)}")
print(f"可入场: {len(feasible)} ({len(feasible)/len(results)*100:.0f}%)")
print(f"放弃(超出区间): {len(skipped)} ({len(skipped)/len(results)*100:.0f}%)")

if not feasible:
    print("无可入场推荐")
    db.close()
    sys.exit()

best = [r["best_return_pct"] for r in feasible]
worst = [r["worst_return_pct"] for r in feasible]
close = [r["close_return_pct"] for r in feasible]
opn = [r["open_return_pct"] for r in feasible]
bday = [r["buy_day_return_pct"] for r in feasible]
wins = sum(1 for x in close if x >= 0)
n = len(feasible)

print(f"\n--- 核心指标 ---")
print(f"不亏率(T+1收盘>=0%): {wins}/{n} = {wins/n*100:.1f}%")
print(f"T+1 平均收盘收益:    {sum(close)/n:+.2f}%")
print(f"T+1 平均最好(盘中高): {sum(best)/n:+.2f}%")
print(f"T+1 平均最坏(盘中低): {sum(worst)/n:+.2f}%")
print(f"T+1 平均开盘:        {sum(opn)/n:+.2f}%")
print(f"T日当天平均表现:      {sum(bday)/n:+.2f}%")
print(f"单笔最大盈利:         {max(best):+.2f}%")
print(f"单笔最大亏损:         {min(worst):+.2f}%")

# 累计收益 (等仓位，每天投入20%*可入场数)
total_return = sum(close)
print(f"\n累计收益(等权): {total_return:+.2f}% ({n}笔交易)")
print(f"平均每日收益:   {total_return/len(all_days):+.2f}% (按{len(all_days)}交易日)")

# Sharpe
variance = sum((x - sum(close)/n)**2 for x in close) / n
std = variance ** 0.5
sharpe = (sum(close)/n) / std if std > 0 else 0
print(f"Sharpe-like:    {sharpe:.3f}")

# 盘中命中率
print(f"\n--- T+1 盘中命中率 ---")
for pct in [1, 2, 3, 5]:
    hit = sum(1 for r in feasible if r["best_return_pct"] >= pct)
    print(f"  涨>={pct}%: {hit}/{n} = {hit/n*100:.1f}%")

print(f"\n--- T+1 盘中风险 ---")
for pct in [1, 2, 3, 5]:
    hit = sum(1 for r in feasible if r["worst_return_pct"] <= -pct)
    print(f"  跌>={pct}%: {hit}/{n} = {hit/n*100:.1f}%")

# 按 Rank
print(f"\n--- 按 Rank ---")
by_rank = defaultdict(list)
for r in feasible:
    by_rank[r.get("rank", 0)].append(r)
print(f"{'Rank':>4} {'N':>3} {'最好':>7} {'最坏':>7} {'收盘':>7} {'不亏率':>6} {'盘中>=3%':>8}")
for rank in sorted(by_rank.keys()):
    rs = by_rank[rank]
    ab = sum(r["best_return_pct"] for r in rs) / len(rs)
    aw = sum(r["worst_return_pct"] for r in rs) / len(rs)
    ac = sum(r["close_return_pct"] for r in rs) / len(rs)
    wr = sum(1 for r in rs if r["close_return_pct"] >= 0) / len(rs) * 100
    h3 = sum(1 for r in rs if r["best_return_pct"] >= 3) / len(rs) * 100
    print(f"{rank:>4} {len(rs):>3} {ab:>+7.2f}% {aw:>+7.2f}% {ac:>+7.2f}% {wr:>5.0f}% {h3:>7.0f}%")

# 按 T 日表现
print(f"\n--- T日表现 vs T+1 ---")
t_buckets = defaultdict(list)
for r in feasible:
    bd = r["buy_day_return_pct"]
    if bd < -3: t_buckets["T跌>3%"].append(r)
    elif bd < 0: t_buckets["T小跌"].append(r)
    elif bd < 3: t_buckets["T小涨(0~3%)"].append(r)
    elif bd < 7: t_buckets["T强势(3~7%)"].append(r)
    else: t_buckets["T涨停(>=7%)"].append(r)

print(f"{'T日表现':<16} {'N':>3} {'T+1收盘':>8} {'T+1最好':>8} {'不亏率':>6}")
for label in ["T跌>3%", "T小跌", "T小涨(0~3%)", "T强势(3~7%)", "T涨停(>=7%)"]:
    group = t_buckets.get(label, [])
    if group:
        ac = sum(r["close_return_pct"] for r in group) / len(group)
        ab = sum(r["best_return_pct"] for r in group) / len(group)
        wr = sum(1 for r in group if r["close_return_pct"] >= 0) / len(group) * 100
        print(f"{label:<16} {len(group):>3} {ac:>+8.2f}% {ab:>+8.2f}% {wr:>5.0f}%")

# 按评分
print(f"\n--- 按 AI 评分 ---")
for label, lo, hi in [("8.5+", 8.5, 99), ("7.5~8.4", 7.5, 8.5), ("6.5~7.4", 6.5, 7.5), ("<6.5", 0, 6.5)]:
    group = [r for r in feasible if lo <= (r.get("opus_score") or 5) < hi]
    if group:
        ac = sum(r["close_return_pct"] for r in group) / len(group)
        wr = sum(1 for r in group if r["close_return_pct"] >= 0) / len(group) * 100
        h3 = sum(1 for r in group if r["best_return_pct"] >= 3) / len(group) * 100
        print(f"  {label:>8}: {len(group):>3}只, 收盘 {ac:+.2f}%, 不亏率 {wr:.0f}%, 盘中>=3% {h3:.0f}%")

# 按连板数
print(f"\n--- 按连板数 ---")
by_boards = defaultdict(list)
for r in feasible:
    b = r.get("consecutive_boards") or 0
    by_boards[b].append(r)
print(f"{'连板':>4} {'N':>3} {'T+1收盘':>8} {'不亏率':>6}")
for b in sorted(by_boards.keys()):
    group = by_boards[b]
    ac = sum(r["close_return_pct"] for r in group) / len(group)
    wr = sum(1 for r in group if r["close_return_pct"] >= 0) / len(group) * 100
    print(f"{b:>4} {len(group):>3} {ac:>+8.2f}% {wr:>5.0f}%")

# 按周分析
print(f"\n--- 按周 ---")
by_week = defaultdict(list)
for r in feasible:
    d = r["rec_date"]
    week = f"{d[:6]}W{(int(d[6:])-1)//7+1}"
    by_week[week].append(r)
print(f"{'周':>10} {'N':>3} {'收盘':>7} {'不亏率':>6}")
for week in sorted(by_week.keys()):
    group = by_week[week]
    ac = sum(r["close_return_pct"] for r in group) / len(group)
    wr = sum(1 for r in group if r["close_return_pct"] >= 0) / len(group) * 100
    print(f"{week:>10} {len(group):>3} {ac:>+7.2f}% {wr:>5.0f}%")

# 逐日明细
print(f"\n--- 逐日明细 ---")
print(f"{'T-1':<10} {'T买入':<10} {'代码':<8} {'名称':<8} {'评分':>4} "
      f"{'Gap':>6} {'T日':>6} {'T+1最好':>7} {'T+1最坏':>7} {'T+1收盘':>7} {'入场':>4}")
for r in results:
    f_mark = "Y" if r.get("entry_feasible") else "-"
    score = r.get("opus_score") or 0
    print(f"{r['rec_date']:<10} {r.get('buy_date',''):<10} {r['code']:<8} {r['name']:<8} "
          f"{score:>4.1f} {r['entry_gap_pct']:>+5.1f}% {r['buy_day_return_pct']:>+5.1f}% "
          f"{r['best_return_pct']:>+6.1f}% {r['worst_return_pct']:>+6.1f}% "
          f"{r['close_return_pct']:>+6.1f}% {f_mark:>4}")

db.close()
print(f"\n{'='*70}")
print("完成")
