import logging

import akshare as ak

logger = logging.getLogger("trading.data.northbound")


def fetch_northbound_flow(date: str) -> dict:
    result = {"northbound_net": None}
    try:
        df = ak.stock_hsgt_hist_em(symbol="北向资金")
        if df is not None and not df.empty:
            target = f"{date[:4]}-{date[4:6]}-{date[6:]}"
            matched = df[df["日期"].astype(str).str.startswith(target)]
            if not matched.empty:
                val = matched.iloc[-1].get("当日成交净买额")
            else:
                # 只用 <= target 的数据，防止前瞻偏差
                df_filtered = df[df["日期"].astype(str) <= target]
                if not df_filtered.empty:
                    val = df_filtered.iloc[-1].get("当日成交净买额")
                    logger.info("未匹配到 %s 北向数据，使用最近历史数据", date)
                else:
                    val = None
                    logger.warning("无 %s 及之前的北向资金数据", date)

            import math
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                result["northbound_net"] = round(float(val), 2)
                logger.info("获取北向资金成功: %.2f 亿", result["northbound_net"])
            else:
                logger.warning("北向资金数据为空值")
    except Exception as e:
        logger.warning("获取北向资金失败: %s", e)
    return result
