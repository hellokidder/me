import logging

import akshare as ak

logger = logging.getLogger("trading.data.margin")


def fetch_margin_data(date: str) -> dict:
    result = {"margin_balance": None, "margin_change": None}
    total_balance = 0.0

    # 上交所融资融券
    try:
        df = ak.stock_margin_sse(start_date=date, end_date=date)
        if df is not None and not df.empty:
            # 融资余额列名可能不同，尝试常见名
            for col in ["融资余额", "融资余额(元)", "rzye"]:
                if col in df.columns:
                    val = float(df.iloc[-1][col])
                    # 如果单位是元，转为亿
                    if val > 1e10:
                        val = val / 1e8
                    total_balance += val
                    break
            logger.info("获取上交所融资融券成功")
    except Exception as e:
        logger.warning("获取上交所融资融券失败: %s", e)

    # 深交所融资融券
    try:
        df = ak.stock_margin_szse(start_date=date, end_date=date)
        if df is not None and not df.empty:
            for col in ["融资余额", "融资余额(元)", "rzye"]:
                if col in df.columns:
                    val = float(df.iloc[-1][col])
                    if val > 1e10:
                        val = val / 1e8
                    total_balance += val
                    break
            logger.info("获取深交所融资融券成功")
    except Exception as e:
        logger.warning("获取深交所融资融券失败: %s", e)

    if total_balance > 0:
        result["margin_balance"] = round(total_balance, 2)

    return result
