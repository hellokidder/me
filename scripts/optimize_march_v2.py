# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""逐项优化回测 v2 — 3月数据 (含封单/情绪/市值等新维度)

基线: 上一轮组合优化 opt_combined (AI>=7.5 + 量比<=5 + 首板优先 + T-1涨幅5-8%过滤 + 龙虎榜优先)
新增测试: O7-O15 (情绪周期/封单/20cm排除/换手率/量比下限/gap/市值/卖出策略)
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
    """原始排序函数"""
    ai = (c.get("sonnet_score") or 5) * 0.6
    boards = (c.get("consecutive_boards") or 0) * 1.5
    vol = (c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0) * 0.2
    change = abs(c.get("change_pct") or 0)
    penalty = 2.0 if change > 7 else 0
    return ai + boards + vol - penalty


def prev_combined_score(c):
    """上一轮组合优化的排序函数 (v1 结论)"""
    base = composite_score_v3(c)
    boards = c.get("consecutive_boards") or 0
    if boards == 1:
        base += 3.0
    elif boards >= 3:
        base -= 2.0
    if c.get("on_dragon_tiger"):
        base += 2.0
    return base


def prev_combined_filter(candidates):
    """上一轮组合优化的过滤函数 (v1 结论)"""
    result = [c for c in candidates
              if not c.get("code", "").startswith(("688", "689", "8", "920"))]
    result = [c for c in result if (c.get("sonnet_score") or 5) >= 7.5]
    result = [c for c in result
              if (c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0) <= 5]
    result = [c for c in result
              if not (5 <= (c.get("change_pct") or 0) < 8)]
    return result


def get_candidates_for_date(date):
    rows = db.conn.execute(
        "SELECT * FROM candidates WHERE date = ? AND source = ?",
        (date, BASELINE_SOURCE),
    ).fetchall()
    return [dict(r) for r in rows]


def get_sentiment_for_date(date):
    """获取该日情绪数据 (涨停数/跌停数/炸板率)"""
    # 先查 daily_market
    row = db.conn.execute(
        "SELECT limit_up_count, limit_down_count, failed_limit_rate FROM daily_market WHERE date = ?",
        (f"{date[:4]}-{date[4:6]}-{date[6:]}" if "-" not in date else date,),
    ).fetchone()
    if row and row[0] is not None:
        return {"limit_up": row[0], "limit_down": row[1], "failed_rate": row[2]}

    # Fallback: 从候选中估算
    cands = get_candidates_for_date(date)
    limit_up = sum(1 for c in cands if c.get("is_limit_up"))
    return {"limit_up": limit_up, "limit_down": None, "failed_rate": None}


def generate_and_verify(source, filter_fn, score_fn, top_n=4,
                        entry_gap_min=None, entry_gap_max=None,
                        skip_dates=None):
    """对全月数据执行: 筛选→推荐→验证→统计"""
    for table in ('recommendations', 'verification_results'):
        db.conn.execute(f"DELETE FROM {table} WHERE source = ?", (source,))
    db.conn.execute("DELETE FROM verification_summary WHERE source = ?", (source,))
    db.conn.commit()

    for date in all_days:
        if skip_dates and date in skip_dates:
            db.save_recommendations(date, [], source=source)
            continue

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

    # 验证 — 可能需要自定义 gap 参数
    if entry_gap_min is not None or entry_gap_max is not None:
        custom_config = {**config}
        vcfg = {**config.get("verification", {})}
        if entry_gap_min is not None:
            vcfg["entry_gap_min_pct"] = entry_gap_min
        if entry_gap_max is not None:
            vcfg["entry_gap_max_pct"] = entry_gap_max
        custom_config["verification"] = vcfg
        verifier = Verifier(db, custom_config)
    else:
        verifier = Verifier(db, config)

    verifier.verify_batch(START, END, source=source)
    return compute_metrics(source)


def compute_metrics(source):
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
        "source": source, "total": len(results), "feasible": n,
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


def compute_metrics_conditional_sell(source):
    """条件卖出: T+1高开>5%用开盘价, 否则用收盘价"""
    rows = db.conn.execute(
        "SELECT * FROM verification_results WHERE source = ? ORDER BY rec_date, rank",
        (source,),
    ).fetchall()
    feasible = [dict(r) for r in rows if r["entry_feasible"]]
    if not feasible:
        return None

    n = len(feasible)
    rets = []
    for r in feasible:
        if r["open_return_pct"] >= 5:
            rets.append(r["open_return_pct"])
        else:
            rets.append(r["close_return_pct"])

    wins = sum(1 for x in rets if x >= 0)
    avg = sum(rets) / n
    return {
        "n": n,
        "win_rate": round(wins / n * 100, 1),
        "avg_return": round(avg, 2),
        "total_return": round(sum(rets), 2),
    }


def print_metrics(m, label=""):
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
    delta_wr = test["win_rate_close"] - baseline["win_rate_close"]
    delta_avg = test["avg_close"] - baseline["avg_close"]
    delta_open_wr = test["win_rate_open"] - baseline["win_rate_open"]
    delta_open_avg = test["avg_open"] - baseline["avg_open"]

    print(f"\n  vs 基线:")
    print(f"    不亏率(收盘): {delta_wr:+.1f}pp  平均收益(收盘): {delta_avg:+.2f}pp")
    print(f"    不亏率(开盘): {delta_open_wr:+.1f}pp  平均收益(开盘): {delta_open_avg:+.2f}pp")

    improved = (delta_wr > 0 and delta_avg >= -0.3) or (delta_avg > 0.2) or \
               (delta_open_wr > 0 and delta_open_avg >= -0.3) or (delta_open_avg > 0.2)

    if improved:
        print(f"  → 结论: ✓ 有效，保留")
    else:
        print(f"  → 结论: ✗ 无效/恶化，回退")

    return {
        "label": label, "keep": improved,
        "delta_wr_close": delta_wr, "delta_avg_close": delta_avg,
        "delta_wr_open": delta_open_wr, "delta_avg_open": delta_open_avg,
        "metrics": test,
    }


# ================================================================
# 开始
# ================================================================
print("=" * 60)
print("3月优化回测 v2 — 新维度 (封单/情绪/市值)")
print("=" * 60)

# ================================================================
# 基线: 上一轮组合优化
# ================================================================
baseline_m = generate_and_verify("v2_baseline", prev_combined_filter, prev_combined_score)
print_metrics(baseline_m, "基线 (v1组合优化: AI>=7.5+量比<=5+首板+过滤5-8%+龙虎榜)")

conclusions = []

# ================================================================
# O7: 情绪周期门控
# ================================================================
print(f"\n{'~'*60}")
print("运行 O7: 情绪周期门控 (涨停<20 跳过)...")

# 获取每日情绪数据，决定哪些天跳过
skip_cold = set()
for date in all_days:
    sent = get_sentiment_for_date(date)
    lu = sent.get("limit_up") or 0
    # 也从涨停池 DataFrame 行数估算 (candidates 中 is_limit_up 的比例不完整)
    # 用一个简单规则: 如果当日候选中涨停不足3只，可能是冷清日
    cands = get_candidates_for_date(date)
    lu_in_cands = sum(1 for c in cands if c.get("is_limit_up"))
    if lu < 20 and lu_in_cands < 5:
        skip_cold.add(date)

if skip_cold:
    print(f"  跳过冷清日 ({len(skip_cold)}): {', '.join(sorted(skip_cold))}")
else:
    print(f"  无冷清日被跳过 (情绪数据不足以判断)")

m7 = generate_and_verify("opt_v2_emotion", prev_combined_filter, prev_combined_score,
                          skip_dates=skip_cold)
print_metrics(m7, "O7: 情绪周期门控")
c7 = compare(baseline_m, m7, "O7: 情绪周期门控")
conclusions.append(c7)

# ================================================================
# O8: 封成比 >= 0.1 过滤 (仅有数据日期)
# ================================================================
print(f"\n{'~'*60}")
print("运行 O8: 封成比>=0.1 过滤...")

def filter_o8(candidates):
    base = prev_combined_filter(candidates)
    result = []
    for c in base:
        seal = c.get("seal_money") or 0
        amount = c.get("turnover_amount") or 0
        if seal > 0 and amount > 0:
            ratio = seal / amount
            if ratio < 0.1:
                continue  # 封成比太低，排除
        result.append(c)
    return result

m8 = generate_and_verify("opt_v2_seal", filter_o8, prev_combined_score)
print_metrics(m8, "O8: 封成比>=0.1 过滤")
c8 = compare(baseline_m, m8, "O8: 封成比>=0.1 过滤")
conclusions.append(c8)

# ================================================================
# O9: 封板时间偏好 (10:00前加权, 14:00后惩罚)
# ================================================================
print(f"\n{'~'*60}")
print("运行 O9: 封板时间偏好...")

def score_o9(c):
    base = prev_combined_score(c)
    st = c.get("seal_time") or ""
    if st and len(st) >= 4:
        # Format: HHMMSS or HHMM
        try:
            hhmm = int(st[:4])
            if hhmm <= 1000:
                base += 2.0  # 10:00前封板
            elif hhmm >= 1400:
                base -= 3.0  # 14:00后封板
        except (ValueError, TypeError):
            pass
    return base

m9 = generate_and_verify("opt_v2_stime", prev_combined_filter, score_o9)
print_metrics(m9, "O9: 封板时间偏好")
c9 = compare(baseline_m, m9, "O9: 封板时间偏好")
conclusions.append(c9)

# ================================================================
# O10: 排除创业板 300/301 (20cm)
# ================================================================
print(f"\n{'~'*60}")
print("运行 O10: 排除创业板20cm...")

def filter_o10(candidates):
    base = prev_combined_filter(candidates)
    return [c for c in base
            if not c.get("code", "").startswith(("300", "301"))]

m10 = generate_and_verify("opt_v2_no20cm", filter_o10, prev_combined_score)
print_metrics(m10, "O10: 排除创业板20cm")
c10 = compare(baseline_m, m10, "O10: 排除创业板20cm")
conclusions.append(c10)

# ================================================================
# O11: 换手率收紧 3-8% (>10% 惩罚)
# ================================================================
print(f"\n{'~'*60}")
print("运行 O11: 换手率收紧...")

def score_o11(c):
    base = prev_combined_score(c)
    tr = c.get("turnover_rate") or 0
    if tr > 10:
        base -= 2.0
    elif 5 <= tr <= 8:
        base += 0.5  # 最佳区间小加分
    return base

m11 = generate_and_verify("opt_v2_tr", prev_combined_filter, score_o11)
print_metrics(m11, "O11: 换手率收紧 (>10%惩罚)")
c11 = compare(baseline_m, m11, "O11: 换手率收紧")
conclusions.append(c11)

# ================================================================
# O12: 量比下限 >= 1.5
# ================================================================
print(f"\n{'~'*60}")
print("运行 O12: 量比下限>=1.5...")

def filter_o12(candidates):
    base = prev_combined_filter(candidates)
    return [c for c in base
            if (c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0) >= 1.5]

m12 = generate_and_verify("opt_v2_volmin", filter_o12, prev_combined_score)
print_metrics(m12, "O12: 量比下限>=1.5")
c12 = compare(baseline_m, m12, "O12: 量比下限>=1.5")
conclusions.append(c12)

# ================================================================
# O13: 入场gap调整 [+1%, +5%]
# ================================================================
print(f"\n{'~'*60}")
print("运行 O13: 入场gap [+1%, +5%]...")

m13 = generate_and_verify("opt_v2_gap", prev_combined_filter, prev_combined_score,
                           entry_gap_min=1.0, entry_gap_max=5.0)
print_metrics(m13, "O13: 入场gap [+1%, +5%]")
c13 = compare(baseline_m, m13, "O13: 入场gap [+1%, +5%]")
conclusions.append(c13)

# ================================================================
# O14: 流通市值 10-50亿
# ================================================================
print(f"\n{'~'*60}")
print("运行 O14: 流通市值10-50亿...")

def filter_o14(candidates):
    base = prev_combined_filter(candidates)
    result = []
    for c in base:
        cap = c.get("float_market_cap") or 0
        cap_yi = cap / 1e8  # 转亿
        if cap_yi > 0:  # 有市值数据
            if cap_yi < 10 or cap_yi > 50:
                continue
        # 无市值数据的保留 (不过滤)
        result.append(c)
    return result

m14 = generate_and_verify("opt_v2_cap", filter_o14, prev_combined_score)
print_metrics(m14, "O14: 流通市值10-50亿")
c14 = compare(baseline_m, m14, "O14: 流通市值10-50亿")
conclusions.append(c14)

# ================================================================
# O15: 条件卖出 (T+1高开>5%开盘卖, 否则收盘卖)
# ================================================================
print(f"\n{'~'*60}")
print("分析 O15: 条件卖出策略...")

# 这个不需要重跑验证，直接在基线数据上分析
cond_sell = compute_metrics_conditional_sell("v2_baseline")
if cond_sell:
    print(f"\n  条件卖出 vs 固定收盘卖:")
    print(f"    条件卖出: 胜率 {cond_sell['win_rate']}%, 平均 {cond_sell['avg_return']:+.2f}%, 累计 {cond_sell['total_return']:+.2f}%")
    print(f"    固定收盘: 胜率 {baseline_m['win_rate_close']}%, 平均 {baseline_m['avg_close']:+.2f}%, 累计 {baseline_m['total_return_close']:+.2f}%")
    delta_wr = cond_sell["win_rate"] - baseline_m["win_rate_close"]
    delta_avg = cond_sell["avg_return"] - baseline_m["avg_close"]
    o15_keep = delta_avg > 0.1 or delta_wr > 2
    print(f"    差异: 胜率 {delta_wr:+.1f}pp, 收益 {delta_avg:+.2f}pp")
    print(f"  → 结论: {'✓ 有效' if o15_keep else '✗ 无效'}")
    conclusions.append({
        "label": "O15: 条件卖出(高开>5%开盘卖)",
        "keep": o15_keep,
        "delta_wr_close": delta_wr, "delta_avg_close": delta_avg,
        "delta_wr_open": 0, "delta_avg_open": 0,
        "metrics": {**baseline_m, "win_rate_close": cond_sell["win_rate"],
                    "avg_close": cond_sell["avg_return"],
                    "total_return_close": cond_sell["total_return"]},
    })

# ================================================================
# 组合: 基线 + 所有有效新规则
# ================================================================
kept = [c for c in conclusions if c["keep"]]
print(f"\n{'='*60}")
print(f"v2 有效优化: {len(kept)}/{len(conclusions)}")
for c in conclusions:
    status = "✓ 保留" if c["keep"] else "✗ 回退"
    print(f"  {status}: {c['label']}")

if kept:
    print(f"\n{'~'*60}")
    print("运行 v2 组合优化 (v1组合 + 所有有效v2规则)...")

    kept_labels = {c["label"] for c in kept}

    def filter_v2_combined(candidates):
        result = prev_combined_filter(candidates)
        if "O8: 封成比>=0.1 过滤" in kept_labels:
            filtered = []
            for c in result:
                seal = c.get("seal_money") or 0
                amount = c.get("turnover_amount") or 0
                if seal > 0 and amount > 0 and seal / amount < 0.1:
                    continue
                filtered.append(c)
            result = filtered
        if "O10: 排除创业板20cm" in kept_labels:
            result = [c for c in result
                      if not c.get("code", "").startswith(("300", "301"))]
        if "O12: 量比下限>=1.5" in kept_labels:
            result = [c for c in result
                      if (c.get("volume_vs_5d_avg") or c.get("volume_ratio") or 0) >= 1.5]
        if "O14: 流通市值10-50亿" in kept_labels:
            filtered = []
            for c in result:
                cap = c.get("float_market_cap") or 0
                cap_yi = cap / 1e8
                if cap_yi > 0 and (cap_yi < 10 or cap_yi > 50):
                    continue
                filtered.append(c)
            result = filtered
        return result

    def score_v2_combined(c):
        base = prev_combined_score(c)
        if "O9: 封板时间偏好" in kept_labels:
            st = c.get("seal_time") or ""
            if st and len(st) >= 4:
                try:
                    hhmm = int(st[:4])
                    if hhmm <= 1000:
                        base += 2.0
                    elif hhmm >= 1400:
                        base -= 3.0
                except (ValueError, TypeError):
                    pass
        if "O11: 换手率收紧" in kept_labels:
            tr = c.get("turnover_rate") or 0
            if tr > 10:
                base -= 2.0
            elif 5 <= tr <= 8:
                base += 0.5
        return base

    # 情绪门控
    skip = skip_cold if "O7: 情绪周期门控" in kept_labels else None

    # Gap 参数
    gap_min = 1.0 if "O13: 入场gap [+1%, +5%]" in kept_labels else None
    gap_max = 5.0 if "O13: 入场gap [+1%, +5%]" in kept_labels else None

    mc = generate_and_verify("opt_v2_combined", filter_v2_combined, score_v2_combined,
                              skip_dates=skip, entry_gap_min=gap_min, entry_gap_max=gap_max)
    print_metrics(mc, "v2 组合优化")
    cc = compare(baseline_m, mc, "v2 组合优化")
    conclusions.append(cc)

    # 条件卖出在组合上的效果
    cond_combined = compute_metrics_conditional_sell("opt_v2_combined")
    if cond_combined:
        print(f"\n  v2组合 + 条件卖出: 胜率 {cond_combined['win_rate']}%, "
              f"平均 {cond_combined['avg_return']:+.2f}%, 累计 {cond_combined['total_return']:+.2f}%")


# ================================================================
# 总结表
# ================================================================
print(f"\n{'='*60}")
print("v2 总结对比表")
print("=" * 60)
print(f"{'优化项':<28} {'可入场':>4} {'胜率(收)':>8} {'收益(收)':>8} "
      f"{'胜率(开)':>8} {'收益(开)':>8} {'结论':>6}")
print("-" * 84)

print(f"{'v1组合基线':<28} {baseline_m['feasible']:>4} "
      f"{baseline_m['win_rate_close']:>7.1f}% {baseline_m['avg_close']:>+7.2f}% "
      f"{baseline_m['win_rate_open']:>7.1f}% {baseline_m['avg_open']:>+7.2f}% {'—':>6}")

for c in conclusions:
    m = c["metrics"]
    status = "保留" if c["keep"] else "回退"
    label = c["label"][:26]
    print(f"{label:<28} {m['feasible']:>4} "
          f"{m['win_rate_close']:>7.1f}% {m['avg_close']:>+7.2f}% "
          f"{m['win_rate_open']:>7.1f}% {m['avg_open']:>+7.2f}% {status:>6}")

# 保存
output = {
    "baseline": baseline_m,
    "optimizations": conclusions,
    "kept": [c["label"] for c in conclusions if c["keep"]],
    "reverted": [c["label"] for c in conclusions if not c["keep"]],
}
os.makedirs("output", exist_ok=True)
with open("output/optimize_march_v2_results.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

db.close()
print(f"\n结果已保存到 output/optimize_march_v2_results.json")
print("完成")
