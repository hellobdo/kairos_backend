import pandas as pd
from analytics.download_comparison_data import download_data

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

def run_report(df: pd.DataFrame, group_by: str) -> pd.DataFrame:
    """
    Run the report on the trade data, combining all metrics in a specific order.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing trade data
    group_by : str
        Time period to group by ('day', 'week', 'month', 'year')
        
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
    comparison_data = generate_comparison_data(df, group_by)
    
    spy_df = comparison_data['SPY']
    spy_returns = spy_df.groupby('period')['perc_return'].sum().to_dict()
    qqq_df = comparison_data['QQQ']
    qqq_returns = qqq_df.groupby('period')['perc_return'].sum().to_dict()
    
    # Add SPY and QQQ returns to result DataFrame
    result['spy_perc_return'] = result['period'].map(spy_returns)
    result['qqq_perc_return'] = result['period'].map(qqq_returns)
    
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

def get_backtest_timeframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the unique dates from the backtest for filtering comparison data.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing backtest data with date information
        
    Returns
    -------
    pd.DataFrame
        DataFrame containing only the unique dates from the backtest
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    print(f"get_backtest_timeframe: DataFrame shape: {df.shape}")
    print(f"get_backtest_timeframe: Columns available: {df.columns.tolist()}")
    
    # Extract unique dates from the DataFrame
    if 'start_date' in df.columns:
        print(f"get_backtest_timeframe: Found 'start_date' column with {df['start_date'].count()} non-null values")
        print(f"get_backtest_timeframe: Sample values: {df['start_date'].head().tolist()}")
        
        # Make sure dates are in datetime format
        dates = pd.to_datetime(df['start_date']).unique()
        print(f"get_backtest_timeframe: Extracted {len(dates)} unique dates")
        if len(dates) > 0:
            print(f"get_backtest_timeframe: First few dates: {dates[:5]}")
            
        return pd.DataFrame({'date': dates})
    else:
        print("get_backtest_timeframe: ERROR - 'start_date' column not found!")
        available_cols = ", ".join(df.columns.tolist())
        raise ValueError(f"DataFrame must contain 'start_date' column. Available columns: {available_cols}")

def generate_comparison_data(df: pd.DataFrame, group_by: str) -> dict:
    """
    Generate comparison data for SPY and QQQ
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing backtest data
    group_by : str
        Time period to group the data by ('day', 'week', 'month', 'year')
        
    Returns
    -------
    dict
        Dictionary containing returns for SPY and QQQ
    """
    # Get market data and backtest dates
    print("Calling download_data()...")
    data_dict = download_data()
    print(f"Downloaded data for tickers: {list(data_dict.keys())}")
    
    print("Getting backtest timeframe...")
    unique_dates = get_backtest_timeframe(df)
    print(f"Found {len(unique_dates)} unique dates in backtest")
    if len(unique_dates) > 0:
        print(f"First few backtest dates: {unique_dates['date'].head().tolist()}")
        print(f"Backtest date type: {type(unique_dates['date'].iloc[0])}")
    
    # Convert backtest dates to string for consistent comparison
    print("Converting backtest dates to string format...")
    unique_dates['date'] = pd.to_datetime(unique_dates['date']).dt.strftime('%Y-%m-%d')
    if len(unique_dates) > 0:
        print(f"First few formatted backtest dates: {unique_dates['date'].head().tolist()}")
    
    returns = {}

    # Process each ticker's data
    for ticker, ticker_df in data_dict.items():
        print(f"\nProcessing {ticker}...")
        print(f"Original columns: {ticker_df.columns.tolist()}")
        print(f"Original shape: {ticker_df.shape}")
        print(f"First few market dates: {ticker_df['date'].head().tolist()}")
        print(f"Market date type: {type(ticker_df['date'].iloc[0])}")
        
        # Convert ticker dates to same format as backtest dates
        print("Converting market dates to string format...")
        ticker_df['date'] = pd.to_datetime(ticker_df['date']).dt.strftime('%Y-%m-%d')
        print(f"First few formatted market dates: {ticker_df['date'].head().tolist()}")
        
        # Filter by dates in unique_dates
        print("Filtering dates...")
        print(f"Looking for these {len(unique_dates)} backtest dates in market data")
        
        # More detailed debugging
        common_dates = set(ticker_df['date']).intersection(set(unique_dates['date']))
        print(f"Found {len(common_dates)} common dates between backtest and {ticker}")
        if len(common_dates) > 0:
            print(f"First few common dates: {list(common_dates)[:5]}")
        else:
            print("No common dates found! Sample comparison:")
            print(f"First 5 backtest dates: {unique_dates['date'].head().tolist()}")
            print(f"First 5 market dates: {ticker_df['date'].head().tolist()}")
            
            # Check date ranges
            backtest_min = min(unique_dates['date']) if len(unique_dates) > 0 else "No dates"
            backtest_max = max(unique_dates['date']) if len(unique_dates) > 0 else "No dates"
            market_min = min(ticker_df['date']) if len(ticker_df) > 0 else "No dates"
            market_max = max(ticker_df['date']) if len(ticker_df) > 0 else "No dates"
            print(f"Backtest date range: {backtest_min} to {backtest_max}")
            print(f"{ticker} date range: {market_min} to {market_max}")
        
        filtered_df = ticker_df[ticker_df['date'].isin(unique_dates['date'])]
        print(f"After filtering, shape: {filtered_df.shape} ({len(filtered_df)} rows remain for {ticker})")
        
        if len(filtered_df) == 0:
            print(f"Warning: No matching dates found for {ticker}")
            continue
            
        # Add period column
        try:
            print("Generating periods...")
            filtered_df['period'] = generate_periods(filtered_df, group_by)
            print(f"Periods generated for {ticker}")
        except Exception as e:
            print(f"Error generating periods: {str(e)}")
            print(f"DataFrame columns: {filtered_df.columns.tolist()}")
            continue
            
        # Calculate returns
        try:
            print("Calculating returns...")
            returns_df = calculate_returns_based_on_close_and_open(filtered_df, group_by)
            print(f"Returns calculated for {ticker}")
        except Exception as e:
            print(f"Error calculating returns: {str(e)}")
            continue
        
        # Store in dictionary
        returns[ticker] = returns_df
    
    print(f"Completed generate_comparison_data, returns contain data for: {list(returns.keys())}")
    return returns


def calculate_returns_based_on_close_and_open(df: pd.DataFrame, group_by: str) -> pd.DataFrame:
    """
    Calculate the returns based on the first open and last close price in each period.
    Also adds a 'Total' row with the performance from first date to last date.
    
    Args:
        df: DataFrame with 'open', 'close' prices and 'period' column
        group_by: String indicating the time period ('day', 'week', 'month', 'year')
    
    Returns:
        DataFrame with percentage returns for each period, including a 'Total' row
    """
    if group_by == 'day':
        # For daily returns, use the daily open and close directly
        df['perc_return'] = (df['close'] - df['open']) / df['open']
        result_df = df.copy()
    else:
        # For week, month, year: group by period and calculate return using
        # first open and last close of each period
        try:
            result_df = df.groupby(['ticker', 'period']).agg({
                'open': 'first',  # First open price of the period
                'close': 'last',  # Last close price of the period
                'date': 'first',  # Keep the first date for reference
            }).reset_index()
            
            # Calculate percentage return for each period
            result_df['perc_return'] = (result_df['close'] - result_df['open']) / result_df['open']
        except Exception as e:
            print(f"Error in aggregation: {str(e)}")
            print(f"Available columns: {df.columns.tolist()}")
            print(f"First few rows: {df.head().to_dict()}")
            raise
    
    # Add a 'Total' row using first open and last close across all dates
    try:
        if len(df) > 0:
            # Sort by date to get first and last point
            sorted_df = df.sort_values('date')
            
            # Get the very first open price
            first_open = sorted_df['open'].iloc[0]
            
            # Get the very last close price
            last_close = sorted_df['close'].iloc[-1]
            
            # Calculate total return
            total_return = (last_close - first_open) / first_open
            
            # Create a Total row with only essential columns
            total_row = pd.DataFrame([{
                'period': 'Total',
                'perc_return': total_return
            }])
            
            # Append total row to result
            result_df = pd.concat([result_df, total_row], ignore_index=True)
    except Exception as e:
        print(f"Error calculating total return: {str(e)}")
        # Silently fail for total calculation
    
    return result_df