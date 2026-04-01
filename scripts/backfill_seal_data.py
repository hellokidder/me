# -*- coding: utf-8 -*-
"""补采3月封单数据 — 从 AkShare 涨停池提取封板资金/时间/市值，更新 candidates 表。"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
from storage.db import TradingDB
from backtest.calendar import TradingCalendar

db = TradingDB('storage/trading.db')
cal = TradingCalendar()
SOURCE = 'backtest_march'

all_days = cal.get_trading_days('20260301', '20260328')
updated_total = 0
no_data_days = []

print("补采3月封单数据")
print("=" * 60)

for date in all_days:
    try:
        df = ak.stock_zt_pool_em(date=date)
    except Exception as e:
        print(f"  {date}: API失败 - {e}")
        no_data_days.append(date)
        continue

    if df is None or df.empty:
        print(f"  {date}: 无数据(已过期)")
        no_data_days.append(date)
        continue

    # Build lookup by code
    seal_map = {}
    for _, row in df.iterrows():
        code = str(row.get("代码", "")).zfill(6)
        seal_map[code] = {
            "seal_money": float(row.get("封板资金", 0) or 0),
            "seal_time": str(row.get("首次封板时间", "")),
            "reopen_count": int(row.get("炸板次数", 0) or 0),
            "turnover_amount": float(row.get("成交额", 0) or 0),
            "float_market_cap": float(row.get("流通市值", 0) or 0),
        }

    # Update candidates in DB
    candidates = db.conn.execute(
        "SELECT code FROM candidates WHERE date = ? AND source = ?",
        (date, SOURCE),
    ).fetchall()

    updated = 0
    for row in candidates:
        code = row[0]
        if code in seal_map:
            s = seal_map[code]
            db.conn.execute("""
                UPDATE candidates SET
                    seal_money = ?, seal_time = ?, reopen_count = ?,
                    turnover_amount = ?, float_market_cap = ?
                WHERE date = ? AND code = ? AND source = ?
            """, (s["seal_money"], s["seal_time"], s["reopen_count"],
                  s["turnover_amount"], s["float_market_cap"],
                  date, code, SOURCE))
            updated += 1

    db.conn.commit()
    updated_total += updated
    print(f"  {date}: 涨停池 {len(df)} 只, 匹配更新 {updated}/{len(candidates)} 条候选")
    time.sleep(0.5)

print(f"\n{'='*60}")
print(f"完成: 更新 {updated_total} 条候选记录")
print(f"无数据日期 ({len(no_data_days)}): {', '.join(no_data_days)}")

# Verify
rows = db.conn.execute("""
    SELECT COUNT(*) as total,
           SUM(CASE WHEN seal_money IS NOT NULL AND seal_money > 0 THEN 1 ELSE 0 END) as with_seal,
           SUM(CASE WHEN float_market_cap IS NOT NULL AND float_market_cap > 0 THEN 1 ELSE 0 END) as with_cap
    FROM candidates WHERE source = ?
""", (SOURCE,)).fetchone()
print(f"\n验证: {rows[0]} 总候选, {rows[1]} 有封单数据, {rows[2]} 有市值数据")

db.close()
