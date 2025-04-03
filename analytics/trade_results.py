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
    Calculate the trading accuracy as the ratio of winning trades to total trades.
    Returns both period-by-period accuracy and total accuracy.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'is_winner' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing accuracy values for each period plus the total
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'is_winner': [1, 0, 1, 1],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_accuracy(df)
    2024-01-15    0.50
    2024-01-16    1.00
    Total         0.75
    Name: accuracy, dtype: float64
    """
    # Check if required columns exist
    if 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain an 'is_winner' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate accuracy by period
    grouped = df.groupby('period')
    winning_trades = grouped['is_winner'].sum()
    total_trades = grouped.size()
    accuracy = winning_trades / total_trades
    
    # Calculate total accuracy
    total_accuracy = df['is_winner'].mean()
    
    # Append total to the Series
    accuracy['Total'] = total_accuracy
    
    # Name the series for identification
    accuracy.name = 'accuracy'
    
    return accuracy

def calculate_risk_per_trade(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average risk per trade from a DataFrame of trade data.
    Returns both period-by-period risk and total risk.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_per_trade' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average risk per trade for each period plus the total
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'risk_per_trade': [0.02, 0.01, 0.03, 0.02],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_risk_per_trade(df)
    2024-01-15    0.015
    2024-01-16    0.025
    Total         0.020
    Name: avg_risk_per_trade, dtype: float64
    """
    # Check if required columns exist
    if 'risk_per_trade' not in df.columns:
        raise ValueError("DataFrame must contain a 'risk_per_trade' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate mean risk per trade for each period
    risk_per_trade = df.groupby('period')['risk_per_trade'].mean()
    
    # Calculate total average risk
    total_risk = df['risk_per_trade'].mean()
    
    # Append total to the Series
    risk_per_trade['Total'] = total_risk
    
    # Name the series for identification
    risk_per_trade.name = 'avg_risk_per_trade'
    
    return risk_per_trade

def calculate_average_risk_reward_on_losses(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average risk reward ratio on losing trades.
    Returns both period-by-period and total averages.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_reward', 'is_winner' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average risk-reward ratio on losing trades for each period plus the total
        
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
    Total         2.4
    Name: avg_risk_reward_losses, dtype: float64
    """
    # Check if required columns exist
    if 'risk_reward' not in df.columns or 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain 'risk_reward' and 'is_winner' columns")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Filter for losing trades
    losing_trades = df[df['is_winner'] == 0]
    
    # Calculate mean risk-reward ratio for each period
    risk_reward_losses = losing_trades.groupby('period')['risk_reward'].mean()
    
    # Calculate total average for losing trades
    total_risk_reward = losing_trades['risk_reward'].mean()
    
    # Append total to the Series
    risk_reward_losses['Total'] = total_risk_reward
    
    # Name the series for identification
    risk_reward_losses.name = 'avg_risk_reward_losses'
    
    return risk_reward_losses

def calculate_average_risk_reward_on_wins(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average risk reward ratio on winning trades.
    Returns both period-by-period and total averages.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_reward', 'is_winner' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average risk-reward ratio on winning trades for each period plus the total
        
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
    Total         2.5
    Name: avg_risk_reward_wins, dtype: float64
    """
    # Check if required columns exist
    if 'risk_reward' not in df.columns or 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain 'risk_reward' and 'is_winner' columns")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Filter for winning trades
    winning_trades = df[df['is_winner'] == 1]
    
    # Calculate mean risk-reward ratio for each period
    risk_reward_wins = winning_trades.groupby('period')['risk_reward'].mean()
    
    # Calculate total average for winning trades
    total_risk_reward = winning_trades['risk_reward'].mean()
    
    # Append total to the Series
    risk_reward_wins['Total'] = total_risk_reward
    
    # Name the series for identification
    risk_reward_wins.name = 'avg_risk_reward_wins'
    
    return risk_reward_wins

def calculate_average_return_per_trade(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average percentage return per trade.
    Returns both period-by-period and total averages.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'perc_return' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average percentage return for each period plus the total
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'perc_return': [2.5, -1.0, 3.0, 1.5],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_average_return_per_trade(df)
    2024-01-15    0.75
    2024-01-16    2.25
    Total         1.50
    Name: avg_return_per_trade, dtype: float64
    """
    # Check if required columns exist
    if 'perc_return' not in df.columns:
        raise ValueError("DataFrame must contain a 'perc_return' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate mean percentage return for each period
    avg_return = df.groupby('period')['perc_return'].mean()
    
    # Calculate total average return
    total_return = df['perc_return'].mean()
    
    # Append total to the Series
    avg_return['Total'] = total_return
    
    # Name the series for identification
    avg_return.name = 'avg_return_per_trade'
    
    return avg_return

def calculate_total_return(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the total percentage return.
    Returns both period-by-period and overall total returns.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'perc_return' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing total percentage return for each period plus the overall total
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'perc_return': [2.5, -1.0, 3.0, 1.5],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_total_return(df)
    2024-01-15    1.5
    2024-01-16    4.5
    Total         6.0
    Name: total_return, dtype: float64
    """
    # Check if required columns exist
    if 'perc_return' not in df.columns:
        raise ValueError("DataFrame must contain a 'perc_return' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate total percentage return for each period
    total_return = df.groupby('period')['perc_return'].sum()
    
    # Calculate overall total return
    overall_total = df['perc_return'].sum()
    
    # Append total to the Series
    total_return['Total'] = overall_total
    
    # Name the series for identification
    total_return.name = 'total_return'
    
    return total_return

def calculate_average_duration(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average duration of trades in hours.
    Returns both period-by-period and total averages.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'duration_hours' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average duration in hours for each period plus the total
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'duration_hours': [2.5, 1.0, 3.0, 1.5],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_average_duration(df)
    2024-01-15    1.75
    2024-01-16    2.25
    Total         2.00
    Name: avg_duration_hours, dtype: float64
    """
    # Check if required columns exist
    if 'duration_hours' not in df.columns:
        raise ValueError("DataFrame must contain a 'duration_hours' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate mean duration for each period
    avg_duration = df.groupby('period')['duration_hours'].mean()
    
    # Calculate total average duration
    total_duration = df['duration_hours'].mean()
    
    # Append total to the Series
    avg_duration['Total'] = total_duration
    
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

def calculate_nr_of_trades(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the number of trades.
    Returns both period-by-period and total counts.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'period' column
        
    Returns
    -------
    pd.Series
        Series containing number of trades for each period plus the total
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'trade_id': [1, 2, 3, 4],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_nr_of_trades(df)
    2024-01-15    2
    2024-01-16    2
    Total         4
    Name: nr_trades, dtype: int64
    """
    # Check if required column exists
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Count trades for each period
    nr_trades = df.groupby('period').size()
    
    # Calculate total number of trades
    total_trades = len(df)
    
    # Append total to the Series
    nr_trades['Total'] = total_trades
    
    # Name the series for identification
    nr_trades.name = 'nr_trades'
    
    return nr_trades

def run_report(df: pd.DataFrame, group_by: str) -> pd.DataFrame:
    """
    Run the report on the trade data.
    """

    # Validate group_by parameter
    valid_groups = {'day', 'week', 'month', 'year'}
    if group_by not in valid_groups:
        raise ValueError(f"group_by must be one of {valid_groups}")
    
    return df