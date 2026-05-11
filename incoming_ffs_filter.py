"""
Incoming rows eligible for Free-For-Sale (FFS) calculations.

CUSTOMER must contain STOCK (case-insensitive), matching the dashboard Incoming
FOR STOCK rule; exact NONSTOCK is excluded. On error or missing CUSTOMER, full
Incoming is returned (safe fallback).

Shared by dashboard, comparison tab, and reporting pipeline.
"""

import pandas as pd

from reporting.logger import get_logger

logger = get_logger(__name__)


def filter_incoming_for_ffs(incoming_df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only Incoming rows that count toward FFS (stock-type CUSTOMER).

    Returns a copy of incoming_df when unfiltered; never mutates the input.
    """
    if incoming_df is None:
        return pd.DataFrame()
    if incoming_df.empty:
        return incoming_df.copy()

    try:
        customer_col = None
        for c in incoming_df.columns:
            if str(c).strip().upper() == "CUSTOMER":
                customer_col = c
                break
        if customer_col is None:
            logger.info(
                "FFS Incoming: no CUSTOMER column; using full Incoming"
            )
            return incoming_df.copy()

        def is_stock_customer(customer_value) -> bool:
            if pd.isna(customer_value):
                return False
            normalized = str(customer_value).strip().upper()
            if normalized == "NONSTOCK":
                return False
            return "STOCK" in normalized

        mask = incoming_df[customer_col].map(is_stock_customer).fillna(False)
        filtered = incoming_df.loc[mask].copy()
        logger.info(
            "FFS Incoming STOCK filter: kept %d / %d rows",
            len(filtered),
            len(incoming_df),
        )
        return filtered
    except Exception:
        logger.warning(
            "FFS Incoming STOCK filter failed; using full Incoming",
            exc_info=True,
        )
        return incoming_df.copy()
