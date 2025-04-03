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


def calculate_accuracy(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the trading accuracy as the ratio of winning trades to total trades, grouped by period.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'is_winner' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing accuracy values for each time period
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'is_winner': [1, 0, 1, 1],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_accuracy(df)
    2024-01-15    0.50
    2024-01-16    1.00
    Name: accuracy, dtype: float64
    """
    # Check if required columns exist
    if 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain an 'is_winner' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Group by period and calculate accuracy
    grouped = df.groupby('period')
    winning_trades = grouped['is_winner'].sum()
    total_trades = grouped.size()
    
    # Calculate accuracy for each group
    accuracy = winning_trades / total_trades
    
    # Name the series for identification
    accuracy.name = 'accuracy'
    
    return accuracy

def calculate_risk_per_trade(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average risk per trade from a DataFrame of trade data, grouped by period.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_per_trade' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average risk per trade for each time period
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'risk_per_trade': [0.02, 0.01, 0.03, 0.02],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_risk_per_trade(df)
    2024-01-15    0.015
    2024-01-16    0.025
    Name: avg_risk_per_trade, dtype: float64
    """
    # Check if required columns exist
    if 'risk_per_trade' not in df.columns:
        raise ValueError("DataFrame must contain a 'risk_per_trade' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate mean risk per trade for each period
    risk_per_trade = df.groupby('period')['risk_per_trade'].mean()
    
    # Name the series for identification
    risk_per_trade.name = 'avg_risk_per_trade'
    
    return risk_per_trade

def calculate_average_risk_reward_on_losses(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average risk reward ratio on losing trades, grouped by period.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_reward', 'is_winner' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average risk-reward ratio on losing trades for each time period
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'risk_reward': [2.5, 1.8, 3.0, 2.0],
    ...     'is_winner': [1, 0, 0, 1],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_average_risk_reward_on_losses(df)
    2024-01-15    1.8
    2024-01-16    3.0
    Name: avg_risk_reward_losses, dtype: float64
    """
    # Check if required columns exist
    if 'risk_reward' not in df.columns or 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain 'risk_reward' and 'is_winner' columns")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Filter for losing trades and calculate mean risk-reward ratio for each period
    losing_trades = df[df['is_winner'] == 0]
    risk_reward_losses = losing_trades.groupby('period')['risk_reward'].mean()
    
    # Name the series for identification
    risk_reward_losses.name = 'avg_risk_reward_losses'
    
    return risk_reward_losses

def calculate_average_risk_reward_on_wins(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average risk reward ratio on winning trades, grouped by period.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_reward', 'is_winner' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average risk-reward ratio on winning trades for each time period
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'risk_reward': [2.5, 1.8, 3.0, 2.0],
    ...     'is_winner': [1, 0, 1, 1],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_average_risk_reward_on_wins(df)
    2024-01-15    2.5
    2024-01-16    2.5
    Name: avg_risk_reward_wins, dtype: float64
    """
    # Check if required columns exist
    if 'risk_reward' not in df.columns or 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain 'risk_reward' and 'is_winner' columns")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Filter for winning trades and calculate mean risk-reward ratio for each period
    winning_trades = df[df['is_winner'] == 1]
    risk_reward_wins = winning_trades.groupby('period')['risk_reward'].mean()
    
    # Name the series for identification
    risk_reward_wins.name = 'avg_risk_reward_wins'
    
    return risk_reward_wins

def calculate_average_return_per_trade(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average percentage return per trade, grouped by period.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'perc_return' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average percentage return for each time period
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'perc_return': [2.5, -1.0, 3.0, 1.5],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_average_return_per_trade(df)
    2024-01-15    0.75
    2024-01-16    2.25
    Name: avg_return_per_trade, dtype: float64
    """
    # Check if required columns exist
    if 'perc_return' not in df.columns:
        raise ValueError("DataFrame must contain a 'perc_return' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate mean percentage return for each period
    avg_return = df.groupby('period')['perc_return'].mean()
    
    # Name the series for identification
    avg_return.name = 'avg_return_per_trade'
    
    return avg_return

def calculate_total_return(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the total percentage return, grouped by period.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'perc_return' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing total percentage return for each time period
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'perc_return': [2.5, -1.0, 3.0, 1.5],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_total_return(df)
    2024-01-15    1.5
    2024-01-16    4.5
    Name: total_return, dtype: float64
    """
    # Check if required columns exist
    if 'perc_return' not in df.columns:
        raise ValueError("DataFrame must contain a 'perc_return' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate total percentage return for each period
    total_return = df.groupby('period')['perc_return'].sum()
    
    # Name the series for identification
    total_return.name = 'total_return'
    
    return total_return

def calculate_average_duration(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average duration of trades in hours, grouped by period.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'duration_hours' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average duration in hours for each time period
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'duration_hours': [2.5, 1.0, 3.0, 1.5],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_average_duration(df)
    2024-01-15    1.75
    2024-01-16    2.25
    Name: avg_duration_hours, dtype: float64
    """
    # Check if required columns exist
    if 'duration_hours' not in df.columns:
        raise ValueError("DataFrame must contain a 'duration_hours' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate mean duration for each period
    avg_duration = df.groupby('period')['duration_hours'].mean()
    
    # Name the series for identification
    avg_duration.name = 'avg_duration_hours'
    
    return avg_duration

def generate_periods(df: pd.DataFrame, group_by: str) -> pd.Series:
    """
    Generate a period Series based on the grouping parameter.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'start_date', 'week', 'month', and 'year' columns
    group_by : str
        Time period to group by ('day', 'week', 'month', 'year')
        
    Returns
    -------
    pd.Series
        Series containing properly formatted period strings for grouping
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'start_date': ['2024-01-15', '2024-01-16'],
    ...     'week': ['02', '03'],
    ...     'month': ['01', '01'],
    ...     'year': ['2024', '2024']
    ... })
    >>> generate_periods(df, 'day')
    0    2024-01-15
    1    2024-01-16
    Name: period, dtype: object
    >>> generate_periods(df, 'week')
    0    2024-W02
    1    2024-W03
    Name: period, dtype: object
    >>> generate_periods(df, 'month')
    0    2024-01
    1    2024-01
    Name: period, dtype: object
    """    
    # Generate period strings based on group_by
    if group_by == 'day':
        period = df['start_date']
    elif group_by == 'week':
        # Ensure week is zero-padded to 2 digits
        period = df['year'] + '-W' + df['week'].str.zfill(2)
    elif group_by == 'month':
        # Ensure month is zero-padded to 2 digits
        period = df['year'] + '-' + df['month'].str.zfill(2)
    else:  # year
        period = df['year']
    
    # Name the series for identification
    period.name = 'period'
    
    return period

def run_report(df: pd.DataFrame, group_by: str) -> pd.DataFrame:
    """
    Run the report on the trade data.
    """

        # Validate group_by parameter
    valid_groups = {'day', 'week', 'month', 'year'}
    if group_by not in valid_groups:
        raise ValueError(f"group_by must be one of {valid_groups}")
    
    return df