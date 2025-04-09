#!/usr/bin/env python3
"""
Threshold Filter Indicator

The Threshold Filter indicator checks if the value of a column is greater than a threshold.
"""

import pandas as pd
from indicators.helpers.column_utils import normalize_columns

def calculate_indicator(df: pd.DataFrame, column_name: str, threshold: float) -> pd.DataFrame:
    """
    Calculate Threshold Filter.
    
    Args:
        df: DataFrame with OHLCV columns (can be uppercase or lowercase)
        
    Returns:
        DataFrame with column_name column added and is_indicator flag
    """

    # Normalize column names to lowercase
    df = normalize_columns(df)

    condition1 = df[column_name] > threshold

    df['is_indicator'] = condition1
    
    return df