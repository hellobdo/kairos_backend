#!/usr/bin/env python3
"""
SMA Indicator

The Simple Moving Average (SMA) calculates the average of closing prices
over the past period. This indicator helps identify the trend direction
and potential support/resistance levels.
"""

import numpy as np
import pandas as pd
from indicators.helpers.column_utils import normalize_columns

def calculate_indicator(df: pd.DataFrame, period: int, sma_threshold: float) -> pd.DataFrame:
    """
    Calculate Simple Moving Average with a period window.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        
    Returns:
        DataFrame with SMA column added and is_indicator flag
    """
    # checks if period is passed as a parameter and valid integer
    if period is None:
        raise ValueError("Period is required")
    if not isinstance(period, int):
        raise ValueError("Period must be an integer")
    
    # Normalize column names to lowercase
    df = normalize_columns(df)
    
    # Ensure we have enough data points
    if len(df) < period:
        # Add SMA column with NaN values if not enough data
        df['SMA'] = np.nan
        return df
    
    # Calculate period Simple Moving Average
    df['SMA'] = df['close'].rolling(window=period).mean()

    condition1 = df['SMA'] > sma_threshold

    df['is_indicator'] = condition1
    
    return df