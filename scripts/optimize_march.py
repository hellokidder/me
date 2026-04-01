# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""逐项优化回测 — 3月数据

对每个优化建议:
  1. 从已有 candidates 中按新规则筛选/排序
  2. 生成推荐 (source=opt_XXX)
  3. 验证 (获取 T 和 T+1 OHLC)
  4. 计算指标并与基线对比
  5. 保留有效优化，回退无效优化

基线: backtest_march (原始 composite_score top4, 无额外过滤)
"""
import os, sys, json, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_loader import load_config
from utils.logger import setup_logger
from storage.db import TradingDB
from backtest.calendar import TradingCalendar
from verification.verifier import Verifier

config = load_config()
setup_logger(config.get('paths', {}).get('log_dir', 'logs'))
db = TradingDB(config.get('paths', {}).get('db_path', 'storage/trading.db'))
cal = TradingCalendar()

START, END = '20260301', '20260328'
all_days = cal.get_trading_days(START, END)
BASELINE_SOURCE = 'backtest_march'


# ================================================================
# 公用函数
# ================================================================

def composite_score_v3(c):
    """原始排序函数 (与 backtest_march.py 一致)"""
    ai = (c.get("sonnet_score") or 5) * 0.6
    boards = (c.get("consecutive_boards") or 0) * 1.5
    vol = (c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0) * 0.2
    change = abs(c.get("change_pct") or 0)
    penalty = 2.0 if change > 7 else 0
    return ai + boards + vol - penalty


def get_candidates_for_date(date):
    """从 DB 读取该日全部候选"""
    rows = db.conn.execute(
        "SELECT * FROM candidates WHERE date = ? AND source = ?",
        (date, BASELINE_SOURCE),
    ).fetchall()
    return [dict(r) for r in rows]


def base_filter(candidates):
    """基础过滤: 排除科创/北交所"""
    return [c for c in candidates
            if not c.get("code", "").startswith(("688", "689", "8", "920"))]


def generate_and_verify(source, filter_fn, score_fn, top_n=4):
    """对全月数据执行: 筛选→推荐→验证→统计"""
    # 清理旧数据
    for table in ('recommendations', 'verification_results'):
        db.conn.execute(f"DELETE FROM {table} WHERE source = ?", (source,))
    db.conn.execute("DELETE FROM verification_summary WHERE source = ?", (source,))
    db.conn.commit()

    # Phase 1: 生成推荐
    for date in all_days:
        candidates = get_candidates_for_date(date)
        if not candidates:
            db.save_recommendations(date, [], source=source)
            continue

        filtered = filter_fn(candidates)
        if not filtered:
            db.save_recommendations(date, [], source=source)
            continue

        filtered.sort(key=score_fn, reverse=True)
        recs = []
        for j, c in enumerate(filtered[:top_n], 1):
            recs.append({
                "rank": j, "code": c.get("code", ""), "name": c.get("name", ""),
                "opus_score": c.get("sonnet_score") or 5,
                "theme": c.get("sonnet_theme") or c.get("industry", ""),
                "reason": "", "risk_warning": "",
                "entry_strategy": "9:30-10:00观察",
                "position_pct": 20,
            })
        db.save_recommendations(date, recs, source=source)

    # Phase 2: 验证
    verifier = Verifier(db, config)
    verifier.verify_batch(START, END, source=source)

    # Phase 3: 统计
    return compute_metrics(source)


def compute_metrics(source):
    """从 verification_results 计算核心指标"""
    rows = db.conn.execute(
        "SELECT * FROM verification_results WHERE source = ? ORDER BY rec_date, rank",
        (source,),
    ).fetchall()
    results = [dict(r) for r in rows]
    feasible = [r for r in results if r.get("entry_feasible")]

    if not feasible:
        return {
            "source": source, "total": len(results), "feasible": 0,
            "win_rate_close": 0, "win_rate_open": 0,
            "avg_close": 0, "avg_open": 0, "avg_best": 0, "avg_worst": 0,
            "total_return_close": 0, "total_return_open": 0,
            "sharpe": 0, "hit_3pct": 0,
        }

    n = len(feasible)
    close = [r["close_return_pct"] for r in feasible]
    opn = [r["open_return_pct"] for r in feasible]
    best = [r["best_return_pct"] for r in feasible]
    worst = [r["worst_return_pct"] for r in feasible]
    wins_close = sum(1 for x in close if x >= 0)
    wins_open = sum(1 for x in opn if x >= 0)
    hit3 = sum(1 for x in best if x >= 3)

    avg_c = sum(close) / n
    var = sum((x - avg_c) ** 2 for x in close) / n
    std = var ** 0.5

    return {
        "source": source,
        "total": len(results),
        "feasible": n,
        "win_rate_close": round(wins_close / n * 100, 1),
        "win_rate_open": round(wins_open / n * 100, 1),
        "avg_close": round(avg_c, 2),
        "avg_open": round(sum(opn) / n, 2),
        "avg_best": round(sum(best) / n, 2),
        "avg_worst": round(sum(worst) / n, 2),
        "total_return_close": round(sum(close), 2),
        "total_return_open": round(sum(opn), 2),
        "sharpe": round(avg_c / std, 3) if std > 0 else 0,
        "hit_3pct": round(hit3 / n * 100, 1),
    }


def print_metrics(m, label=""):
    """打印指标摘要"""
    if label:
        print(f"\n{'='*60}")
        print(f"  {label}")
        print(f"{'='*60}")
    print(f"  总推荐: {m['total']}, 可入场: {m['feasible']}")
    print(f"  不亏率(收盘): {m['win_rate_close']}%  |  不亏率(开盘): {m['win_rate_open']}%")
    print(f"  平均收益 — 收盘: {m['avg_close']:+.2f}%  开盘: {m['avg_open']:+.2f}%")
    print(f"  平均最好: {m['avg_best']:+.2f}%  最坏: {m['avg_worst']:+.2f}%")
    print(f"  累计收益 — 收盘: {m['total_return_close']:+.2f}%  开盘: {m['total_return_open']:+.2f}%")
    print(f"  Sharpe: {m['sharpe']:.3f}  |  盘中>=3%: {m['hit_3pct']}%")


def compare(baseline, test, label):
    """对比 test vs baseline, 返回结论"""
    delta_wr = test["win_rate_close"] - baseline["win_rate_close"]
    delta_avg = test["avg_close"] - baseline["avg_close"]
    delta_open_wr = test["win_rate_open"] - baseline["win_rate_open"]
    delta_open_avg = test["avg_open"] - baseline["avg_open"]

    print(f"\n  vs 基线:")
    print(f"    不亏率(收盘): {delta_wr:+.1f}pp  平均收益(收盘): {delta_avg:+.2f}pp")
    print(f"    不亏率(开盘): {delta_open_wr:+.1f}pp  平均收益(开盘): {delta_open_avg:+.2f}pp")

    # 判定: 收盘胜率 或 开盘胜率 有提升，且平均收益不恶化
    improved = (delta_wr > 0 and delta_avg >= -0.3) or (delta_avg > 0.2) or \
               (delta_open_wr > 0 and delta_open_avg >= -0.3) or (delta_open_avg > 0.2)

    if improved:
        print(f"  → 结论: ✓ 有效，保留")
    else:
        print(f"  → 结论: ✗ 无效/恶化，回退")

    return {
        "label": label,
        "keep": improved,
        "delta_wr_close": delta_wr,
        "delta_avg_close": delta_avg,
        "delta_wr_open": delta_open_wr,
        "delta_avg_open": delta_open_avg,
        "metrics": test,
    }


# ================================================================
# 基线
# ================================================================
print("=" * 60)
print("3月优化回测 — 逐项 A/B 测试")
print("=" * 60)

# 直接从已有数据读基线指标
baseline = compute_metrics(BASELINE_SOURCE)
print_metrics(baseline, "基线 (backtest_march)")

conclusions = []

# ================================================================
# O1: T+1 开盘卖出 (仅改评估标准, 不改筛选)
# ================================================================
# 这个不需要重跑, 直接用 baseline 的 open_return_pct
print_metrics(baseline, "O1: T+1 开盘卖出 (评估角度)")
o1_result = {
    "label": "O1: T+1 开盘卖出",
    "keep": baseline["win_rate_open"] > baseline["win_rate_close"] or
            baseline["avg_open"] > baseline["avg_close"],
    "delta_wr_close": 0,
    "delta_avg_close": 0,
    "delta_wr_open": baseline["win_rate_open"] - baseline["win_rate_close"],
    "delta_avg_open": baseline["avg_open"] - baseline["avg_close"],
    "metrics": baseline,
    "note": "卖出时点选择，不改变选股。对比收盘卖 vs 开盘卖。"
}
print(f"\n  开盘卖 vs 收盘卖:")
print(f"    胜率: {baseline['win_rate_open']}% vs {baseline['win_rate_close']}%"
      f"  ({o1_result['delta_wr_open']:+.1f}pp)")
print(f"    收益: {baseline['avg_open']:+.2f}% vs {baseline['avg_close']:+.2f}%"
      f"  ({o1_result['delta_avg_open']:+.2f}pp)")

if baseline["avg_open"] > baseline["avg_close"]:
    print(f"  → 结论: ✓ 开盘卖更优")
    o1_result["keep"] = True
elif baseline["win_rate_open"] > baseline["win_rate_close"]:
    print(f"  → 结论: △ 胜率略优但收益无优势")
    o1_result["keep"] = False
else:
    print(f"  → 结论: ✗ 开盘卖无优势")
    o1_result["keep"] = False

conclusions.append(o1_result)

# ================================================================
# O2: AI >= 7.5 过滤
# ================================================================
print(f"\n{'~'*60}")
print("运行 O2: AI >= 7.5 过滤...")

def filter_o2(candidates):
    base = base_filter(candidates)
    return [c for c in base if (c.get("sonnet_score") or 5) >= 7.5]

m2 = generate_and_verify("opt_ai75", filter_o2, composite_score_v3)
print_metrics(m2, "O2: AI >= 7.5 过滤")
c2 = compare(baseline, m2, "O2: AI >= 7.5 过滤")
conclusions.append(c2)

# ================================================================
# O3: 量比 > 5 过滤掉
# ================================================================
print(f"\n{'~'*60}")
print("运行 O3: 过滤量比 > 5...")

def filter_o3(candidates):
    base = base_filter(candidates)
    return [c for c in base
            if (c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0) <= 5]

m3 = generate_and_verify("opt_vol5", filter_o3, composite_score_v3)
print_metrics(m3, "O3: 过滤量比 > 5")
c3 = compare(baseline, m3, "O3: 过滤量比 > 5")
conclusions.append(c3)

# ================================================================
# O4: 优先首板 (连板=1 加权)
# ================================================================
print(f"\n{'~'*60}")
print("运行 O4: 优先首板...")

def score_o4(c):
    """首板加分: 连板=1 额外 +3"""
    base = composite_score_v3(c)
    boards = c.get("consecutive_boards") or 0
    if boards == 1:
        base += 3.0
    elif boards >= 3:
        base -= 2.0  # 高位接力惩罚
    return base

m4 = generate_and_verify("opt_board1", base_filter, score_o4)
print_metrics(m4, "O4: 优先首板 (连板=1)")
c4 = compare(baseline, m4, "O4: 优先首板 (连板=1)")
conclusions.append(c4)

# ================================================================
# O5: 过滤 T-1 涨幅 5%-8%
# ================================================================
print(f"\n{'~'*60}")
print("运行 O5: 过滤T-1涨幅5-8%...")

def filter_o5(candidates):
    base = base_filter(candidates)
    return [c for c in base
            if not (5 <= (c.get("change_pct") or 0) < 8)]

m5 = generate_and_verify("opt_chg58", filter_o5, composite_score_v3)
print_metrics(m5, "O5: 过滤T-1涨幅5-8%")
c5 = compare(baseline, m5, "O5: 过滤T-1涨幅5-8%")
conclusions.append(c5)

# ================================================================
# O6: 龙虎榜优先
# ================================================================
print(f"\n{'~'*60}")
print("运行 O6: 龙虎榜优先...")

def score_o6(c):
    """龙虎榜 +2 加权"""
    base = composite_score_v3(c)
    if c.get("on_dragon_tiger"):
        base += 2.0
    return base

m6 = generate_and_verify("opt_dt", base_filter, score_o6)
print_metrics(m6, "O6: 龙虎榜优先")
c6 = compare(baseline, m6, "O6: 龙虎榜优先")
conclusions.append(c6)

# ================================================================
# 组合: 保留所有有效优化，一起跑
# ================================================================
kept = [c for c in conclusions if c["keep"]]
print(f"\n{'='*60}")
print(f"有效优化: {len(kept)}/{len(conclusions)}")
for c in conclusions:
    status = "✓ 保留" if c["keep"] else "✗ 回退"
    print(f"  {status}: {c['label']}")

if len(kept) > 1:  # 只有多个有效优化才值得测组合
    print(f"\n{'~'*60}")
    print("运行组合优化 (所有有效规则叠加)...")

    def filter_combined(candidates):
        base = base_filter(candidates)
        result = base
        # 根据保留的优化叠加规则
        kept_labels = {c["label"] for c in kept}
        if "O2: AI >= 7.5 过滤" in kept_labels:
            result = [c for c in result if (c.get("sonnet_score") or 5) >= 7.5]
        if "O3: 过滤量比 > 5" in kept_labels:
            result = [c for c in result
                      if (c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0) <= 5]
        if "O5: 过滤T-1涨幅5-8%" in kept_labels:
            result = [c for c in result
                      if not (5 <= (c.get("change_pct") or 0) < 8)]
        return result

    def score_combined(c):
        base = composite_score_v3(c)
        kept_labels = {k["label"] for k in kept}
        if "O4: 优先首板 (连板=1)" in kept_labels:
            boards = c.get("consecutive_boards") or 0
            if boards == 1:
                base += 3.0
            elif boards >= 3:
                base -= 2.0
        if "O6: 龙虎榜优先" in kept_labels:
            if c.get("on_dragon_tiger"):
                base += 2.0
        return base

    mc = generate_and_verify("opt_combined", filter_combined, score_combined)
    print_metrics(mc, "组合优化")
    cc = compare(baseline, mc, "组合优化")
    conclusions.append(cc)


# ================================================================
# 总结表
# ================================================================
print(f"\n{'='*60}")
print("总结对比表")
print("=" * 60)
print(f"{'优化项':<24} {'可入场':>4} {'胜率(收)':>8} {'收益(收)':>8} "
      f"{'胜率(开)':>8} {'收益(开)':>8} {'结论':>6}")
print("-" * 80)

# 基线行
print(f"{'基线':<24} {baseline['feasible']:>4} "
      f"{baseline['win_rate_close']:>7.1f}% {baseline['avg_close']:>+7.2f}% "
      f"{baseline['win_rate_open']:>7.1f}% {baseline['avg_open']:>+7.2f}% {'—':>6}")

for c in conclusions:
    m = c["metrics"]
    status = "保留" if c["keep"] else "回退"
    label = c["label"][:22]
    print(f"{label:<24} {m['feasible']:>4} "
          f"{m['win_rate_close']:>7.1f}% {m['avg_close']:>+7.2f}% "
          f"{m['win_rate_open']:>7.1f}% {m['avg_open']:>+7.2f}% {status:>6}")

# 保存结论到 JSON
output = {
    "baseline": baseline,
    "optimizations": conclusions,
    "kept": [c["label"] for c in conclusions if c["keep"]],
    "reverted": [c["label"] for c in conclusions if not c["keep"]],
}
with open("output/optimize_march_results.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

db.close()
print(f"\n结果已保存到 output/optimize_march_results.json")
print("完成")
