#!/usr/bin/env python3
"""
T-Shaped Candle Indicator

A T-shaped candle has:
1. Small body (open-close)
2. Long lower shadow
3. Lower shadow at least 2.5x longer than upper shadow
"""

import numpy as np
import pandas as pd

def calculate_indicator(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate T-shaped candle pattern.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        tight_threshold: Maximum body size relative to open price
        
    Returns:
        DataFrame with is_t_shaped column added
    """
    df = df.copy()
    
    # Handle both uppercase and lowercase column names
    open_price = 'Open' if 'Open' in df.columns else 'open'
    high_price = 'High' if 'High' in df.columns else 'high'
    low_price = 'Low' if 'Low' in df.columns else 'low'
    close_price = 'Close' if 'Close' in df.columns else 'close'
    
    # Calculate absolute difference between open and close
    abs_delta_open_close_perc = (abs(df[close_price] - df[open_price]) / df[open_price])
    
    # Calculate threshold based on close price
    tight_threshold = np.where(df[close_price] < 80, 0.12, 0.25)
    
    # tight body condition
    condition1 = abs_delta_open_close_perc < tight_threshold

    # lower shadow exists
    condition2 = df[low_price] < df[open_price]
    
    condition3 = (
        # Only calculate ratio where there's a lower shadow to avoid division by zero
        ((df[open_price] - df[low_price]) > (2.5 * abs(df[high_price] - df[close_price]))) &
        ((df[open_price] - df[low_price]) > (2.5 * abs(df[high_price] - df[open_price])))
    )

    # close price is bigger than open price
    condition4 = df[close_price] > df[open_price]
    
    # Combine conditions
    df['is_t_shaped'] = condition1 & condition2 & condition3 & condition4
    
    return df