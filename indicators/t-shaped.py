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
from indicators.helpers.column_utils import normalize_ohlc_columns

def calculate_indicator(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate T-shaped candle pattern.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        tight_threshold: Maximum body size relative to open price
        
    Returns:
        DataFrame with is_indicator column added
    """
    # Normalize column names to lowercase
    df = normalize_ohlc_columns(df)
    
    # Calculate absolute difference between open and close
    abs_delta_open_close_perc = (abs(df['close'] - df['open']) / df['open'])
    
    # Calculate threshold based on close price
    tight_threshold = np.where(df['close'] < 80, 0.12, 0.25)
    
    # tight body condition
    condition1 = abs_delta_open_close_perc < tight_threshold

    # lower shadow exists
    condition2 = df['low'] < df['open']
    
    condition3 = (
        # Only calculate ratio where there's a lower shadow to avoid division by zero
        ((df['open'] - df['low']) > (2.5 * abs(df['high'] - df['close']))) &
        ((df['open'] - df['low']) > (2.5 * abs(df['high'] - df['open'])))
    )
    
    # close price is bigger than open price
    condition4 = df['close'] > df['open']
    
    # Combine conditions
    df['is_indicator'] = (condition1 & condition2 & condition3 & condition4)
    
    return df