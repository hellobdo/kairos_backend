#!/usr/bin/env python3
"""
ADV Indicator

The Average Daily Volume (ADV) indicator calculates the average of the daily volume
over the past period. This indicator helps identify the trend direction
and potential support/resistance levels.
"""

import numpy as np
import pandas as pd
from indicators.helpers.column_utils import normalize_columns

def calculate_indicator(df: pd.DataFrame, period: int, adv_threshold: float) -> pd.DataFrame:
    """
    Calculate Average Daily Volume with a period window.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        
    Returns:
        DataFrame with ADV column added and is_indicator flag
    """
    # checks if period is passed as a parameter and valid integer
    if period is None:
        raise ValueError("Period is required")
    if not isinstance(period, int):
        raise ValueError("Period must be an integer")
    
    # Normalize column names to lowercase
    df = normalize_columns(df)
    
    # Calculate ADV
    df['adv'] = df['volume'].rolling(window=period).mean()

    condition1 = df['adv'] > adv_threshold

    df['is_indicator'] = condition1
    
    return df