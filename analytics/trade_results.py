import pandas as pd
import numpy as np

def check_columns(df: pd.DataFrame) -> bool:

    required_columns = [
        "num_executions",
        "symbol",
        "direction",
        "quantity",
        "entry_price",
        "capital_required",
        "exit_price",
        "stop_price",
        "take_profit_price",
        "risk_reward",
        "risk_amount_per_share",
        "is_winner",
        "risk_per_trade",
        "perc_return",
        "status",
        "exit_type",
        "end_date",
        "end_time",
        "duration_hours",
        "commission",
        "start_date",
        "start_time",
        "week",
        "month",
        "year"
    ]

    return True if all(col in df.columns for col in required_columns) else False


def calculate_accuracy(df: pd.DataFrame) -> float:
    """
    Calculate the trading accuracy as the ratio of winning trades to total trades.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with an 'is_winner' column (1 for winning trades, 0 for losing trades)
        
    Returns
    -------
    float
        The accuracy ratio (winning trades / total trades), ranging from 0.0 to 1.0
        Returns 0.0 if there are no trades in the DataFrame
    
    Examples
    --------
    >>> df = pd.DataFrame({'trade_id': [1, 2, 3, 4], 'is_winner': [1, 0, 1, 1]})
    >>> calculate_accuracy(df)
    0.75
    """
    # Check if the DataFrame is empty
    if df.empty:
        return 0.0
    
    # Check if 'is_winner' column exists
    if 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain an 'is_winner' column")
    
    # Count winning trades (where is_winner == 1)
    winning_trades = df['is_winner'].sum()
    
    # Count total trades
    total_trades = len(df)
    
    # Calculate accuracy
    accuracy = winning_trades / total_trades if total_trades > 0 else 0.0
    
    return accuracy