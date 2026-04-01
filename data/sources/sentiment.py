import logging

import pandas as pd

logger = logging.getLogger("trading.data.sentiment")


def calc_market_sentiment(
    limit_up_pool: pd.DataFrame,
    failed_limit_pool: pd.DataFrame,
    limit_down_pool: pd.DataFrame,
) -> dict:
    limit_up_count = len(limit_up_pool) if not limit_up_pool.empty else 0
    limit_down_count = len(limit_down_pool) if not limit_down_pool.empty else 0
    failed_count = len(failed_limit_pool) if not failed_limit_pool.empty else 0

    total = limit_up_count + failed_count
    failed_rate = round(failed_count / total * 100, 1) if total > 0 else 0.0

    logger.info(
        "市场情绪: 涨停 %d, 跌停 %d, 炸板率 %.1f%%",
        limit_up_count, limit_down_count, failed_rate,
    )

    return {
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "failed_limit_rate": failed_rate,
    }
