# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""近7个交易日回测 - 全新跑，验证无前瞻偏差。

使用独立 source='backtest_7d' 避免污染已有数据。
回测流程: T-1分析 -> T日开盘买入(gap在[-2%,+3%)内) -> T+1卖出

前瞻偏差检查点:
  [1] 数据采集: collector 传 mode='backtest'，新闻只查库+央视历史
  [2] 日线数据: reference_date 截断，只看 T-1 及之前
  [3] 涨停池/龙虎榜: AkShare 按日期查询当日数据
  [4] 北向资金: fallback 有 <= date 过滤
  [5] 验证阶段: 单独获取 T 日和 T+1 的 OHLC（这是"答案"，只用于评估不用于选股）
"""
import os
import sys
import time
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

SOURCE = 'backtest_7d'

# === 确定回测区间 ===
# 近7个交易日: 需要 T-1(分析) + T(买入) + T+1(卖出) 都有数据
# 最晚的 T-1 = 需要 T+1 有行情数据的最近日期
# 保守取: 到 20260327 (T=0328, T+1=0330/0331)
all_days = cal.get_trading_days('20260301', '20260328')
test_days = all_days[-7:]  # 最近7个交易日

print("=" * 60)
print("近7日回测 (全新运行)")
print("=" * 60)
print(f"分析日(T-1): {test_days[0]} ~ {test_days[-1]}")
print(f"买入日(T):   {cal.next_trading_day(test_days[0])} ~ {cal.next_trading_day(test_days[-1])}")
print(f"卖出日(T+1): 各自的下一个交易日")
print(f"Source: {SOURCE}")
print()

# === 清除旧数据 ===
for table in ('candidates', 'recommendations', 'verification_results'):
    db.conn.execute(f"DELETE FROM {table} WHERE source = ?", (SOURCE,))
db.conn.execute("DELETE FROM verification_summary WHERE source = ?", (SOURCE,))
db.conn.commit()

# === Phase 1: 数据采集 + 筛选 + 生成推荐 ===
print("Phase 1: 数据采集 + 筛选 (不调 AI，用规则排序)")
print("-" * 60)


def composite_score(c):
    """复合 v3: AI*0.6 + boards*1.5 + vol*0.2 - 高涨幅惩罚"""
    ai = c.get("sonnet_score") or 5  # 无AI评分时默认5
    boards = c.get("consecutive_boards") or 0
    vol = c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0
    change = abs(c.get("change_pct") or 0)
    penalty = 2.0 if change > 7 else 0
    return ai * 0.6 + boards * 1.5 + vol * 0.2 - penalty


for i, date in enumerate(test_days):
    print(f"\n[{i+1}/{len(test_days)}] T-1={date}")

    # 采集 (backtest 模式，不调实时接口)
    collector = MarketDataCollector(config, mode="backtest", cache_db=db)
    market_data = collector.collect_all(date)

    # 筛选
    screener = StockScreener(config, collector)
    candidates = screener.screen(market_data)

    evaluator = PipelineEvaluator(config)
    v = evaluator.evaluate_screening(candidates, market_data, attempt=0)
    if v.verdict == Verdict.RETRY:
        retry_config = {
            **config,
            "screening": {**config.get("screening", {}), **v.retry_params},
        }
        screener = StockScreener(retry_config, collector)
        candidates = screener.screen(market_data)

    # 尝试从已有 AI cache 补充 sonnet 分数
    row = db.conn.execute(
        "SELECT response_json FROM ai_cache WHERE date = ? AND stage = 'sonnet'",
        (date,),
    ).fetchone()
    if row:
        import json
        sonnet_result = json.loads(row[0])
        scored_map = {c["code"]: c for c in sonnet_result.get("scored_candidates", [])}
        for cand in candidates:
            scored = scored_map.get(cand["code"])
            if scored:
                cand["sonnet_score"] = scored.get("score")
                cand["sonnet_theme"] = scored.get("matched_theme")
        print(f"  已从 AI 缓存补充 Sonnet 评分")

    db.save_candidates(date, candidates, source=SOURCE)

    if not candidates:
        db.save_recommendations(date, [], source=SOURCE)
        print(f"  无候选，跳过")
        continue

    # 过滤 + 排序 + 取 top4
    filtered = [c for c in candidates
                if not c.get("code", "").startswith(("688", "689", "8", "920"))]
    filtered.sort(key=composite_score, reverse=True)

    recs = []
    for j, c in enumerate(filtered[:4], 1):
        recs.append({
            "rank": j,
            "code": c.get("code", ""),
            "name": c.get("name", ""),
            "opus_score": c.get("sonnet_score") or 5,
            "theme": c.get("sonnet_theme") or c.get("industry", ""),
            "reason": "",
            "risk_warning": "",
            "entry_strategy": "9:30-10:00观察，开盘涨幅-2%~+3%区间可入场",
            "position_pct": 20,
        })

    db.save_recommendations(date, recs, source=SOURCE)
    print(f"  候选 {len(candidates)} -> 推荐 {len(recs)}: "
          + ", ".join(f"{r['name']}({r['code']})" for r in recs))

    if i < len(test_days) - 1:
        time.sleep(1)

# === Phase 2: 验证 (这里才用 T 和 T+1 的数据 — "看答案") ===
print(f"\n{'='*60}")
print("Phase 2: 验证 (获取 T 日和 T+1 的行情数据)")
print("-" * 60)

verifier = Verifier(db, config)
stats = verifier.verify_batch(test_days[0], test_days[-1], source=SOURCE)

# === Phase 3: 结果分析 ===
rows = db.conn.execute("""
    SELECT * FROM verification_results
    WHERE source = ? ORDER BY rec_date, rank
""", (SOURCE,)).fetchall()
results = [dict(r) for r in rows]
feasible = [r for r in results if r.get("entry_feasible")]
skipped = [r for r in results if not r.get("entry_feasible")]

print(f"\n{'='*60}")
print("回测结果")
print("=" * 60)

vcfg = config.get("verification", {})
print(f"入场条件: 开盘涨跌幅在 [{vcfg.get('entry_gap_min_pct', -2)}%, +{vcfg.get('entry_gap_max_pct', 3)}%) 内")
print(f"总推荐: {len(results)}")
print(f"可入场: {len(feasible)} ({len(feasible)/len(results)*100:.0f}%)" if results else "")
print(f"放弃:   {len(skipped)}")

if not feasible:
    print("\n无可入场推荐")
    db.close()
    sys.exit()

best_rets = [r["best_return_pct"] for r in feasible]
worst_rets = [r["worst_return_pct"] for r in feasible]
close_rets = [r["close_return_pct"] for r in feasible]
open_rets = [r["open_return_pct"] for r in feasible]
buy_day_rets = [r["buy_day_return_pct"] for r in feasible]

wins = sum(1 for x in close_rets if x >= 0)

print(f"\n--- T+1 表现 (vs T日开盘买入价) ---")
print(f"最好(盘中最高): {sum(best_rets)/len(best_rets):+.2f}%")
print(f"最坏(盘中最低): {sum(worst_rets)/len(worst_rets):+.2f}%")
print(f"T+1 开盘:      {sum(open_rets)/len(open_rets):+.2f}%")
print(f"T+1 收盘:      {sum(close_rets)/len(close_rets):+.2f}%")
print(f"T日当天表现:    {sum(buy_day_rets)/len(buy_day_rets):+.2f}%")
print(f"不亏率(收盘>=0%): {wins}/{len(feasible)} = {wins/len(feasible)*100:.1f}%")

print(f"\n--- 盘中命中率 ---")
for pct in [1, 2, 3, 5]:
    hit = sum(1 for r in feasible if r["best_return_pct"] >= pct)
    print(f"  T+1涨>={pct}%: {hit}/{len(feasible)} = {hit/len(feasible)*100:.1f}%")

print(f"\n--- 按 Rank ---")
by_rank = defaultdict(list)
for r in feasible:
    by_rank[r.get("rank", 0)].append(r)
print(f"{'Rank':>4} {'N':>3} {'T+1最好':>8} {'T+1最坏':>8} {'T+1收盘':>8} {'不亏率':>6}")
for rank in sorted(by_rank.keys()):
    rs = by_rank[rank]
    ab = sum(r["best_return_pct"] for r in rs) / len(rs)
    aw = sum(r["worst_return_pct"] for r in rs) / len(rs)
    ac = sum(r["close_return_pct"] for r in rs) / len(rs)
    wr = sum(1 for r in rs if r["close_return_pct"] >= 0) / len(rs) * 100
    print(f"{rank:>4} {len(rs):>3} {ab:>+8.2f}% {aw:>+8.2f}% {ac:>+8.02f}% {wr:>5.0f}%")

print(f"\n--- T日表现 vs T+1 ---")
t_buckets = defaultdict(list)
for r in feasible:
    bd = r["buy_day_return_pct"]
    if bd < -3:
        t_buckets["T日跌>3%"].append(r)
    elif bd < 0:
        t_buckets["T日小跌"].append(r)
    elif bd < 3:
        t_buckets["T日小涨"].append(r)
    else:
        t_buckets["T日强势>3%"].append(r)

for label in ["T日跌>3%", "T日小跌", "T日小涨", "T日强势>3%"]:
    group = t_buckets.get(label, [])
    if group:
        ac = sum(r["close_return_pct"] for r in group) / len(group)
        wr = sum(1 for r in group if r["close_return_pct"] >= 0) / len(group) * 100
        print(f"  {label}: {len(group)}只, T+1收盘 {ac:+.2f}%, 不亏率 {wr:.0f}%")

print(f"\n--- 逐日明细 ---")
print(f"{'T-1':>10} {'T(买)':>10} {'代码':>7} {'名称':<8} {'Gap':>6} {'T日':>6} {'T+1最好':>7} {'T+1最坏':>7} {'T+1收盘':>7} {'入场':>4}")
for r in results:
    f_mark = "Y" if r.get("entry_feasible") else "-"
    print(f"{r['rec_date']:>10} {r.get('buy_date',''):>10} {r['code']:>7} {r['name']:<8} "
          f"{r['entry_gap_pct']:>+5.1f}% {r['buy_day_return_pct']:>+5.1f}% "
          f"{r['best_return_pct']:>+6.1f}% {r['worst_return_pct']:>+6.1f}% "
          f"{r['close_return_pct']:>+6.1f}% {f_mark:>4}")

db.close()
print(f"\n{'='*60}")
print("完成")
