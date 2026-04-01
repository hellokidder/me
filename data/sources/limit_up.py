import logging

import akshare as ak
import pandas as pd

logger = logging.getLogger("trading.data.limit_up")


def fetch_limit_up_pool(date: str) -> pd.DataFrame:
    try:
        df = ak.stock_zt_pool_em(date=date)
        if df is not None and not df.empty:
            logger.info("获取涨停池成功，共 %d 只", len(df))
            return df
    except Exception as e:
        logger.warning("获取涨停池失败: %s", e)
    return pd.DataFrame()


def fetch_failed_limit_up(date: str) -> pd.DataFrame:
    try:
        df = ak.stock_zt_pool_zbgc_em(date=date)
        if df is not None and not df.empty:
            logger.info("获取炸板池成功，共 %d 只", len(df))
            return df
    except Exception as e:
        logger.warning("获取炸板池失败: %s", e)
    return pd.DataFrame()


def fetch_limit_down_pool(date: str) -> pd.DataFrame:
    try:
        df = ak.stock_zt_pool_dtgc_em(date=date)
        if df is not None and not df.empty:
            logger.info("获取跌停池成功，共 %d 只", len(df))
            return df
    except Exception as e:
        logger.warning("获取跌停池失败: %s", e)
    return pd.DataFrame()


def fetch_strong_pool(date: str) -> pd.DataFrame:
    try:
        df = ak.stock_zt_pool_strong_em(date=date)
        if df is not None and not df.empty:
            logger.info("获取强势股池成功，共 %d 只", len(df))
            return df
    except Exception as e:
        logger.warning("获取强势股池失败: %s", e)
    return pd.DataFrame()
