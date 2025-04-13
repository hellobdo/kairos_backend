#!/usr/bin/env python3
"""
ADR Indicator

The Average Daily Range (ADR) indicator calculates the average of the daily range
over the past period. This indicator helps identify the trend direction
and potential support/resistance levels.
"""

import numpy as np
import pandas as pd

def calculate_indicator(df: pd.DataFrame, period: int) -> pd.DataFrame:
    """
    Calculate Average Daily Range with a period window.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        
    Returns:
        DataFrame with ADR column added
    """
    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    df_copy = df.copy()
    
    # Ensure we have enough data points
    if len(df_copy) < period:
        # Add adr column with NaN values if not enough data
        df_copy['adr'] = np.nan
        return df_copy
    
    # Calculate period ADR
    df_copy['daily_range'] = df_copy['high'] - df_copy['low']
    df_copy['daily_range_percentage'] = df_copy['daily_range'] / df_copy['open']
    df_copy['adr'] = df_copy['daily_range_percentage'].rolling(window=period).mean() * 100
    
    return df_copy