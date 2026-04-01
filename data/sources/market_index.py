from __future__ import annotations

import logging

import akshare as ak

logger = logging.getLogger("trading.data.market_index")

INDEX_MAP = {
    "sh000001": "上证指数",
    "sz399001": "深证成指",
    "sz399006": "创业板指",
}


def fetch_market_indices(date: str | None = None) -> dict:
    """获取大盘指数。date 非 None 时按日期截断（回测模式）。"""
    result = {
        "sh_index_close": None, "sh_index_change": None,
        "sz_index_close": None, "sz_index_change": None,
        "cyb_index_close": None, "cyb_index_change": None,
    }
    prefix_map = {"sh000001": "sh", "sz399001": "sz", "sz399006": "cyb"}

    formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:]}" if date else None

    # 优先使用东方财富历史接口
    for symbol, prefix in prefix_map.items():
        try:
            df = ak.stock_zh_index_daily_em(symbol=symbol)
            if df is not None and not df.empty:
                if formatted_date:
                    df = df[df["date"].astype(str) <= formatted_date]
                if len(df) >= 2:
                    last = df.iloc[-1]
                    result[f"{prefix}_index_close"] = float(last["close"])
                    prev_close = float(df.iloc[-2]["close"])
                    if prev_close > 0:
                        change = (float(last["close"]) - prev_close) / prev_close * 100
                        result[f"{prefix}_index_change"] = round(change, 2)
                    logger.info("获取 %s 成功（东方财富）", INDEX_MAP[symbol])
        except Exception as e:
            logger.warning("获取 %s 失败（东方财富）: %s", INDEX_MAP[symbol], e)

    # 如果东方财富接口全部失败且非回测模式，降级到新浪实时接口
    if all(result[f"{p}_index_close"] is None for p in prefix_map.values()):
        if date is None:
            logger.info("东方财富指数接口不可用，降级到新浪接口")
            try:
                df = ak.stock_zh_index_spot_sina()
                if df is not None and not df.empty:
                    for symbol, prefix in prefix_map.items():
                        row = df[df["代码"] == symbol]
                        if not row.empty:
                            r = row.iloc[0]
                            result[f"{prefix}_index_close"] = float(r["最新价"])
                            result[f"{prefix}_index_change"] = round(float(r["涨跌幅"]), 2)
                            logger.info("获取 %s 成功（新浪）", INDEX_MAP[symbol])
            except Exception as e:
                logger.warning("新浪指数接口也失败: %s", e)
        else:
            logger.warning("回测模式下东方财富指数接口不可用，无法获取历史指数")

    return result
