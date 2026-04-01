from __future__ import annotations

import logging

import akshare as ak

logger = logging.getLogger("trading.data.overnight")


def fetch_overnight_markets(date: str | None = None) -> dict:
    """获取隔夜外盘数据。date 非 None 时取 < date 的最后一行（回测模式）。"""
    result = {
        "us_sp500_change": None,
        "us_nasdaq_change": None,
        "hk_hsi_change": None,
    }

    formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:]}" if date else None

    # 美股 S&P 500
    try:
        df = ak.index_us_stock_sina(symbol=".INX")
        if df is not None and len(df) >= 2:
            if formatted_date:
                df = df[df["date"].astype(str) < formatted_date]
            if len(df) >= 2:
                last = float(df.iloc[-1]["close"])
                prev = float(df.iloc[-2]["close"])
                if prev > 0:
                    result["us_sp500_change"] = round((last - prev) / prev * 100, 2)
                logger.info("获取标普500成功")
    except Exception as e:
        logger.warning("获取标普500失败: %s", e)

    # 美股 NASDAQ
    try:
        df = ak.index_us_stock_sina(symbol=".IXIC")
        if df is not None and len(df) >= 2:
            if formatted_date:
                df = df[df["date"].astype(str) < formatted_date]
            if len(df) >= 2:
                last = float(df.iloc[-1]["close"])
                prev = float(df.iloc[-2]["close"])
                if prev > 0:
                    result["us_nasdaq_change"] = round((last - prev) / prev * 100, 2)
                logger.info("获取纳斯达克成功")
    except Exception as e:
        logger.warning("获取纳斯达克失败: %s", e)

    # 恒生指数
    try:
        df = ak.stock_hk_index_daily_em(symbol="HSI")
        if df is not None and len(df) >= 2:
            if formatted_date:
                df = df[df["date"].astype(str) < formatted_date]
            if len(df) >= 2:
                last = float(df.iloc[-1]["close"])
                prev = float(df.iloc[-2]["close"])
                if prev > 0:
                    result["hk_hsi_change"] = round((last - prev) / prev * 100, 2)
                logger.info("获取恒生指数成功")
    except Exception as e:
        logger.warning("获取恒生指数失败: %s", e)

    return result
