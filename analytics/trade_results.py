import pandas as pd
from api.yf import download_data

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
    
    return accuracy

def calculate_risk_per_trade_perc(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the average risk per trade from a DataFrame of trade data.
    Returns both period-by-period risk and total risk.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data with 'risk_per_trade_perc' and 'period' columns
        
    Returns
    -------
    pd.Series
        Series containing average risk per trade for each period plus the total
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'risk_per_trade_perc': [0.02, 0.01, 0.03, 0.02],
    ...     'period': ['2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16']
    ... })
    >>> calculate_risk_per_trade_perc(df)
    2024-01-15    0.015
    2024-01-16    0.025
    Total         0.020
    Name: avg_risk_per_trade, dtype: float64
    """
    # Check if required columns exist
    if 'risk_per_trade_perc' not in df.columns:
        raise ValueError("DataFrame must contain a 'risk_per_trade_perc' column")
    if 'period' not in df.columns:
        raise ValueError("DataFrame must contain a 'period' column")
    
    # Calculate mean risk per trade for each period
    risk_per_trade_perc = df.groupby('period')['risk_per_trade_perc'].mean()
    
    # Calculate total average risk
    total_risk_perc = df['risk_per_trade_perc'].mean()
    
    # Append total to the Series
    risk_per_trade_perc['Total'] = total_risk_perc
    
    return risk_per_trade_perc

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
    if 'risk_reward' not in df.columns:
        raise ValueError("DataFrame must contain a 'risk_reward' column")
    if 'is_winner' not in df.columns:
        raise ValueError("DataFrame must contain a 'is_winner' column")
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
    
    # Validate group_by parameter
    valid_groups = {'day', 'week', 'month', 'year'}
    if group_by not in valid_groups:
        raise ValueError(f"group_by must be one of {valid_groups}")
    
    # Generate period strings based on group_by
    if group_by == 'day':
        try:
            print(f"generate_periods: Using 'start_date' column: {df['start_date'].head().tolist()}")
            period = df['start_date']
        except Exception as e:
            print(f"generate_periods: Error processing day periods: {str(e)}")
            raise
    elif group_by == 'week':
        try:
            # Ensure week is zero-padded to 2 digits and year is string
            print(f"generate_periods: Using 'year' column: {df['year'].head().tolist()}")
            print(f"generate_periods: Using 'week' column: {df['week'].head().tolist()}")
            
            # Convert to datetime if needed to ensure consistent week formatting
            if 'date' in df.columns:
                # Use the date column to get consistent week numbers
                dates = pd.to_datetime(df['date'])
                # Get ISO year and week numbers (more consistent across year boundaries)
                iso_years = dates.dt.isocalendar().year.astype(str)
                iso_weeks = dates.dt.isocalendar().week.astype(str).str.zfill(2)
                period = iso_years + '-W' + iso_weeks
            else:
                # Fall back to existing year and week columns
                period = df['year'].astype(str) + '-W' + df['week'].astype(str).str.zfill(2)
        except Exception as e:
            print(f"generate_periods: Error processing week periods: {str(e)}")
            raise
    elif group_by == 'month':
        try:
            # Ensure month is zero-padded to 2 digits and year is string
            print(f"generate_periods: Using 'year' column: {df['year'].head().tolist()}")
            print(f"generate_periods: Using 'month' column: {df['month'].head().tolist()}")
            period = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)
        except Exception as e:
            print(f"generate_periods: Error processing month periods: {str(e)}")
            raise
    else:  # year
        try:
            print(f"generate_periods: Using 'year' column: {df['year'].head().tolist()}")
            period = df['year'].astype(str)
        except Exception as e:
            print(f"generate_periods: Error processing year periods: {str(e)}")
            raise
    
    # Name the series for identification
    period.name = 'period'
    print(f"generate_periods: Generated {len(period)} periods, first few: {period.head().tolist()}")
    
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
    
    return nr_trades

def run_report(df: pd.DataFrame, group_by: str, settings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the report on the trade data, combining all metrics in a specific order.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data
    group_by : str
        Time period to group by ('day', 'week', 'month', 'year')
    settings_df : pd.DataFrame
        DataFrame containing backtest settings
        
    Returns
    -------
    pd.DataFrame
        DataFrame containing all metrics organized by period, with a Total row
        Columns are ordered as:
        - period
        - nr_trades (number of trades)
        - accuracy
        - avg_duration_hours (average duration)
        - avg_risk_per_trade (risk per trade)
        - avg_risk_reward_wins (average win)
        - avg_risk_reward_losses (average loss)
        - avg_return_per_trade (average return)
        - total_return
    """
    df = df.copy()
    
    # Generate period column
    df['period'] = generate_periods(df, group_by)
    
    # Calculate all metrics
    metrics = {
        'nr_trades': calculate_nr_of_trades(df),
        'accuracy': calculate_accuracy(df),
        'avg_duration_hours': calculate_average_duration(df),
        'avg_risk_per_trade_perc': calculate_risk_per_trade_perc(df),
        'avg_risk_reward_wins': calculate_average_risk_reward_on_wins(df),
        'avg_risk_reward_losses': calculate_average_risk_reward_on_losses(df),
        'avg_return_per_trade': calculate_average_return_per_trade(df),
        'total_return': calculate_total_return(df)
    }
    
    # Combine all metrics into a DataFrame
    result = pd.DataFrame(metrics)
    
    # Reset index to make period a column
    result = result.reset_index().rename(columns={'index': 'period'})

    # Generate comparison data
    comparison_data = generate_comparison_data(group_by, settings_df)

    # Merge with comparison data
    result = pd.merge(result, comparison_data, on='period', how='left')
    
    # Ensure columns are in the desired order
    column_order = [
        'period',
        'nr_trades',
        'accuracy',
        'avg_duration_hours',
        'avg_risk_per_trade_perc',
        'avg_risk_reward_wins',
        'avg_risk_reward_losses',
        'avg_return_per_trade',
        'total_return',
        'spy_perc_return',
        'qqq_perc_return'
    ]
    
    return result[column_order]

def get_backtest_timeframe(settings_df: pd.DataFrame) -> dict:
    """
    Get the date range for the backtest for filtering comparison data.
    Finds the range from the last business day before first trade to last trade end.
    
    Parameters
    ----------
    settings_df : pd.DataFrame
        DataFrame containing backtest settings
        
    Returns
    -------
    dict
        Dictionary containing 'start_date' and 'end_date' for the backtest timeframe
    """
    
    # Convert dates to datetime
    start_date = pd.to_datetime(settings_df['backtesting_start'].iloc[0])
    end_date = pd.to_datetime(settings_df['backtesting_end'].iloc[0])
    
    # Create business day range ending exactly at min_trade_date with one extra day before
    # This gives us exactly the previous business day
    earliest_date = pd.bdate_range(end=start_date, periods=2)[0]
    earliest_date = earliest_date.strftime('%Y-%m-%d')
    print(f"get_backtest_timeframe: Original min date: {start_date}, Previous business day: {earliest_date}")

    end_date = end_date.strftime('%Y-%m-%d')
    print(f"get_backtest_timeframe: Date range: {earliest_date} to {end_date}")
    
    # Return the start and end dates as a dictionary
    return start_date, end_date

def generate_comparison_data(group_by: str, settings_df: pd.DataFrame, tickers: list[str] = ["SPY", "QQQ"]) -> pd.DataFrame:
    """
    Generate comparison data for market benchmarks.
    
    Parameters
    ----------
    group_by : str
        Time period to group the data by ('day', 'week', 'month', 'year')
    settings_df : pd.DataFrame
        DataFrame containing backtest settings
    tickers : list[str], optional
        List of ticker symbols to download data for, by default ["SPY", "QQQ"]
        
    Returns
    -------
    pd.DataFrame
        DataFrame with columns for period and each ticker's percentage returns
    """
    
    # Get timeframe dictionary with start_date and end_date
    start_date, end_date = get_backtest_timeframe(settings_df)
    
    # Download data with adjusted start date
    benchmark_data = download_data(tickers, start=start_date, end=end_date)
    
    # Process each ticker and prepare a list for concatenation
    ticker_dfs = []
    
    for ticker in tickers:
        # Filter benchmark data for the current ticker
        ticker_df = benchmark_data[benchmark_data['ticker'] == ticker].copy()
        
        # Add period column
        ticker_df['period'] = generate_periods(ticker_df, group_by)
        
        # Calculate returns 
        ticker_df = calculate_returns_based_on_close(ticker_df, group_by)
        
        # Group by period and keep only the last row per period
        ticker_df = ticker_df[['period', 'perc_return', 'ticker']].drop_duplicates(subset=['period'])
        
        # Rename the column to include ticker name
        ticker_df = ticker_df.rename(columns={'perc_return': f'{ticker.lower()}_perc_return'})
        
        ticker_dfs.append(ticker_df[['period', f'{ticker.lower()}_perc_return']])
    
    # If no data was processed, return empty DataFrame
    if not ticker_dfs:
        return pd.DataFrame(columns=['period'])
    
    # Merge all ticker DataFrames on period
    result = ticker_dfs[0]
    for df in ticker_dfs[1:]:
        result = pd.merge(result, df, on='period', how='outer')
    
    return result

def calculate_returns_based_on_close(df: pd.DataFrame, group_by: str) -> pd.DataFrame:
    """
    Calculate returns based on closing prices.
    
    For all groupings: (last_close - first_close) / first_close
    Also calculates total return from first to last date.
    
    Args:
        df: DataFrame with 'close' prices, 'date', 'ticker', and 'period' columns
        group_by: String indicating the time period ('day', 'week', 'month', 'year')
    
    Returns:
        DataFrame with percentage returns for each period, including a 'Total' row
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Sort by date to ensure proper ordering
    df = df.sort_values('date')
    
    # Calculate total return from first to last date
    first_close = df['close'].iloc[0]
    last_close = df['close'].iloc[-1]
    total_return = (last_close - first_close) / first_close
    
    # For all periods, calculate returns using first and last prices in each period
    period_first = df.groupby('period')['close'].first()
    period_last = df.groupby('period')['close'].last()
    
    # Log close prices for month or year grouping
    if group_by in ['month', 'year']:
        print(f"\nClose prices for {group_by} grouping ({df['ticker'].iloc[0]}):")
        for period in period_first.index:
            print(f"  {period}: First: {period_first[period]:.2f}, Last: {period_last[period]:.2f}")
    
    # Calculate period returns
    period_return = (period_last - period_first) / period_first
    
    # Map the period returns back to the original DataFrame
    df['perc_return'] = df['period'].map(period_return)
    
    # Create a total row
    total_row = df.iloc[-1].copy()
    total_row['period'] = 'Total'
    total_row['perc_return'] = total_return
    
    # Append the total row to the DataFrame
    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    
    return df