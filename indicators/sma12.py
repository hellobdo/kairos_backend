#!/usr/bin/env python3
"""
SMA12 Indicator

The Simple Moving Average (SMA) 12 calculates the average of closing prices
over the past 12 periods. This indicator helps identify the trend direction
and potential support/resistance levels.
"""

import numpy as np
import pandas as pd

def calculate_indicator(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Simple Moving Average with a 12-period window.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        
    Returns:
        DataFrame with SMA12 column added and is_above_sma12 flag
    """
    df = df.copy()
    
    # Handle both uppercase and lowercase column names
    close_price = 'Close' if 'Close' in df.columns else 'close'
    
    # Ensure we have enough data points
    if len(df) < 12:
        # Add SMA12 column with NaN values if not enough data
        df['SMA12'] = np.nan
        df['is_above_sma12'] = False
        return df
    
    # Calculate 12-period Simple Moving Average
    df['SMA12'] = df[close_price].rolling(window=12).mean()
    
    # Add is_above_sma12 flag (starting with 'is_' to be compatible with plot_helper)
    df['is_above_sma12'] = df[close_price] > df['SMA12']
    
    return df