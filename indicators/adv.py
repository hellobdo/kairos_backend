#!/usr/bin/env python3
"""
ADV Indicator

The Average Daily Volume (ADV) indicator calculates the average of the daily volume
over the past period. This indicator helps identify the trend direction
and potential support/resistance levels.
"""

import numpy as np
import pandas as pd

def calculate_indicator(df: pd.DataFrame, period: int) -> pd.DataFrame:
    """
    Calculate Average Daily Volume with a period window.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        
    Returns:
        DataFrame with ADV column added
    """    
    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    df_copy = df.copy()
    
    # Ensure we have enough data points
    if len(df_copy) < period:
        # Add adv column with NaN values if not enough data
        df_copy['adv'] = np.nan
        return df_copy

    df_copy['adv'] = df_copy['volume'].rolling(window=period).mean()
    
    return df_copy