import logging

import akshare as ak
import pandas as pd

logger = logging.getLogger("trading.data.dragon_tiger")


def fetch_dragon_tiger_list(date: str) -> pd.DataFrame:
    try:
        df = ak.stock_lhb_detail_em(
            start_date=date, end_date=date
        )
        if df is not None and not df.empty:
            logger.info("获取龙虎榜详情成功，共 %d 条", len(df))
            return df
    except Exception as e:
        logger.warning("获取龙虎榜详情失败: %s", e)
    return pd.DataFrame()


def fetch_institutional_trading(date: str) -> pd.DataFrame:
    try:
        df = ak.stock_lhb_jgmmtj_em(start_date=date, end_date=date)
        if df is not None and not df.empty:
            logger.info("获取机构买卖统计成功，共 %d 条", len(df))
            return df
    except Exception as e:
        logger.warning("获取机构买卖统计失败: %s", e)
    return pd.DataFrame()
