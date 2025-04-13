#!/usr/bin/env python3
"""
SMA Indicator

The Simple Moving Average (SMA) calculates the average of closing prices
over the past period. This indicator helps identify the trend direction
and potential support/resistance levels.
"""

import numpy as np
import pandas as pd

def calculate_indicator(df: pd.DataFrame, period: int) -> pd.DataFrame:
    """
    Calculate Simple Moving Average with a period window.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        
    Returns:
        DataFrame with sma column added
    """
    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    df_copy = df.copy()
    
    # Ensure we have enough data points
    if len(df_copy) < period:
        # Add SMA column with NaN values if not enough data
        df_copy['sma'] = np.nan
        return df_copy
    
    # Calculate period Simple Moving Average
    df_copy['sma'] = df_copy['close'].rolling(window=period).mean()
    
    return df_copy