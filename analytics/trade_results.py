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

def calculate_risk_per_trade(df: pd.DataFrame) -> float:
    """
    Calculate the average risk per trade from a DataFrame of trade data.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with a 'risk_per_trade' column
        
    Returns
    -------
    float
        The mean risk per trade across all trades
        Returns 0.0 if there are no trades or all values are NaN
    
    Examples
    --------
    >>> df = pd.DataFrame({'trade_id': [1, 2, 3], 'risk_per_trade': [0.02, 0.01, 0.03]})
    >>> calculate_risk_per_trade(df)
    0.02
    """
    # Check if the DataFrame is empty
    if df.empty:
        return 0.0
    
    # Check if 'risk_per_trade' column exists
    if 'risk_per_trade' not in df.columns:
        raise ValueError("DataFrame must contain a 'risk_per_trade' column")
    
    # Calculate the mean risk per trade, ignoring NaN values
    mean_risk = df['risk_per_trade'].mean()
    
    # Handle case where all values might be NaN
    return mean_risk if not pd.isna(mean_risk) else 0.0
    
def calculate_average_risk_reward_on_losses(df: pd.DataFrame) -> float:
    """
    Calculate the average risk reward ratio on losing trades from a DataFrame of trade data.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_reward' and 'is_winner' columns
        
    Returns
    -------
    float
        The average risk-reward ratio on losing trades
        Returns 0.0 if there are no losing trades or all values are NaN
    
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'trade_id': [1, 2, 3, 4],
    ...     'risk_reward': [2.5, 1.8, 3.0, 2.0],
    ...     'is_winner': [1, 1, 0, 1]
    ... })
    >>> calculate_average_risk_reward_on_losses(df)
    3.0
    """
    # Check if the DataFrame is empty
    if df.empty:
        return 0.0
    
    # Check if required columns exist
    if 'risk_reward' not in df.columns or 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain 'risk_reward' and 'is_winner' columns")
    
    # Filter for losing trades
    losing_trades = df[df['is_winner'] == 0]
    
    # If no losing trades, return 0.0
    if losing_trades.empty:
        return 0.0
    
    # Calculate the mean risk-reward ratio on losing trades, ignoring NaN values
    mean_rr = losing_trades['risk_reward'].abs().mean()
    
    # Handle case where all values might be NaN
    return mean_rr if not pd.isna(mean_rr) else 0.0
    
def calculate_average_risk_reward_on_wins(df: pd.DataFrame) -> float:
    """
    Calculate the average risk reward ratio on winning trades from a DataFrame of trade data.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_reward' and 'is_winner' columns
        
    Returns
    -------
    float
        The average risk-reward ratio on winning trades
        Returns 0.0 if there are no winning trades or all values are NaN
    
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'trade_id': [1, 2, 3, 4],
    ...     'risk_reward': [2.5, 1.8, 3.0, 2.0],
    ...     'is_winner': [1, 0, 1, 1]
    ... })
    >>> calculate_average_risk_reward_on_wins(df)
    2.5
    """
    # Check if the DataFrame is empty
    if df.empty:
        return 0.0
    
    # Check if required columns exist
    if 'risk_reward' not in df.columns or 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain 'risk_reward' and 'is_winner' columns")
    
    # Filter for winning trades
    winning_trades = df[df['is_winner'] == 1]
    
    # If no winning trades, return 0.0
    if winning_trades.empty:
        return 0.0
    
    # Calculate the mean risk-reward ratio on winning trades, ignoring NaN values
    mean_rr = winning_trades['risk_reward'].mean()
    
    # Handle case where all values might be NaN
    return mean_rr if not pd.isna(mean_rr) else 0.0


def calculate_average_return_per_trade(df: pd.DataFrame) -> float:
    """
    Calculate the average percentage return per trade from a DataFrame of trade data.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with a 'perc_return' column
        
    Returns
    -------
    float
        The mean percentage return across all trades
        Returns 0.0 if there are no trades or all values are NaN
    
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'trade_id': [1, 2, 3, 4],
    ...     'perc_return': [2.5, -1.0, 3.0, 1.5]
    ... })
    >>> calculate_average_return_per_trade(df)
    1.5
    """
    # Check if the DataFrame is empty
    if df.empty:
        return 0.0
    
    # Check if 'perc_return' column exists
    if 'perc_return' not in df.columns:
        raise ValueError("DataFrame must contain a 'perc_return' column")
    
    # Calculate the mean percentage return, ignoring NaN values
    mean_return = df['perc_return'].mean()
    
    # Handle case where all values might be NaN
    return mean_return if not pd.isna(mean_return) else 0.0
    
def calculate_total_return(df: pd.DataFrame) -> float:
    """
    Calculate the total percentage return from a DataFrame of trade data.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with a 'perc_return' column
        
    Returns
    -------
    float
        The sum of percentage returns across all trades
        Returns 0.0 if there are no trades or all values are NaN
    
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'trade_id': [1, 2, 3, 4],
    ...     'perc_return': [2.5, -1.0, 3.0, 1.5]
    ... })
    >>> calculate_total_return(df)
    6.0
    """
    # Check if the DataFrame is empty
    if df.empty:
        return 0.0
    
    # Check if 'perc_return' column exists
    if 'perc_return' not in df.columns:
        raise ValueError("DataFrame must contain a 'perc_return' column")
    
    # Calculate the sum of percentage returns, ignoring NaN values
    total_return = df['perc_return'].sum()
    
    # Handle case where all values might be NaN
    return total_return if not pd.isna(total_return) else 0.0
    
def calculate_average_duration(df: pd.DataFrame) -> float:
    """
    Calculate the average duration of trades from a DataFrame of trade data.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with a 'duration_hours' column
        
    Returns
    -------
    float
        The mean duration in hours across all trades
        Returns 0.0 if there are no trades or all values are NaN
    
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'trade_id': [1, 2, 3, 4],
    ...     'duration_hours': [2.5, 1.0, 3.0, 1.5]
    ... })
    >>> calculate_average_duration(df)
    2.0
    """
    # Check if the DataFrame is empty
    if df.empty:
        return 0.0
    
    # Check if 'duration_hours' column exists
    if 'duration_hours' not in df.columns:
        raise ValueError("DataFrame must contain a 'duration_hours' column")
    
    # Calculate the mean duration, ignoring NaN values
    mean_duration = df['duration_hours'].mean()
    
    # Handle case where all values might be NaN
    return mean_duration if not pd.isna(mean_duration) else 0.0
    
