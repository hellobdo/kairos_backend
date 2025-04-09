#!/usr/bin/env python3
"""
ADR Indicator

The Average Daily Range (ADR) indicator calculates the average of the daily range
over the past period. This indicator helps identify the trend direction
and potential support/resistance levels.
"""

import numpy as np
import pandas as pd
from indicators.helpers.column_utils import normalize_columns

def calculate_indicator(df: pd.DataFrame, period: int, adr_threshold: float) -> pd.DataFrame:
    """
    Calculate Average Daily Range with a period window.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        
    Returns:
        DataFrame with ADR column added and is_indicator flag
    """
    # checks if period is passed as a parameter and valid integer
    if period is None:
        raise ValueError("Period is required")
    if not isinstance(period, int):
        raise ValueError("Period must be an integer")
    
    # Normalize column names to lowercase
    df = normalize_columns(df)
    
    # Calculate period ADR
    df['daily_range'] = df['high'] - df['low']
    df['daily_range_percentage'] = df['daily_range'] / df['open']
    df['adr'] = df['daily_range_percentage'].rolling(window=period).mean()

    condition1 = df['adr'] > adr_threshold

    df['is_indicator'] = condition1
    
    return df