#!/usr/bin/env python3
"""
Trade Analysis Utility

This script processes trading execution data from CSV files to identify complete trades
(entries and exits) and generate performance metrics. It automatically finds the most
recent trades CSV file in the logs directory, cleans the data, and groups related
executions together based on symbol and position tracking.

Key features:
- Timestamp cleaning and formatting
- Position tracking per symbol
- Trade identification based on position changes
- Performance metrics calculation (duration, avg price, etc.)
- HTML report generation
- Automatic report opening in browser
- Stop price calculation based on strategy parameters

Usage:
    python process_trades.py                                                            # Process the latest trades file
    python process_trades.py --stop_loss 0.8 --side buy --risk_reward 2 --risk_per_trade 0.005   # Process with strategy parameters
    
In code:
    from process_trades import get_latest_trades_file, clean_trades_file, identify_trades, generate_html_report
    
    # Process specific file
    file_path = get_latest_trades_file()
    cleaned_data, rejected_trades = clean_trades_file(file_path)
    trades_df, trades_summary = identify_trades(cleaned_data)
    html_file = generate_html_report(trades_df, trades_summary, "trade_report.html")
    
    # With strategy parameters
    strategy_params = {'stop_loss': 0.8, 'side': 'buy', 'risk_reward': 2, 'risk_per_trade': 0.005}
    html_file = generate_html_report(trades_df, trades_summary, "trade_report.html", strategy_params=strategy_params)
"""
import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, time
from pathlib import Path
import sys
import webbrowser
import argparse

def get_latest_trades_file():
    """
    Find the most recent trades CSV file in the logs directory.
    This function is kept as a fallback when no specific file path is provided.
    
    Returns:
        Path: Path to the latest trades file, or None if none found
    """
    # Get the logs directory path
    logs_dir = Path("logs")
    
    # Find all CSV files with 'trades' in the name
    trade_files = list(logs_dir.glob("*trades*.csv"))
    
    if not trade_files:
        print("No trade CSV files found in logs directory")
        return None
    
    # Get the latest file based on creation time
    latest_file = max(trade_files, key=os.path.getctime)
    
    print(f"Latest trades file found: {latest_file}")
    print(f"Created at: {datetime.fromtimestamp(os.path.getctime(latest_file))}")
    
    return latest_file

def clean_trades_file(file_path):
    """
    Clean and prepare the trades file for analysis
    
    Args:
        file_path: Path to the trades CSV file
        
    Returns:
        tuple: (cleaned_df, rejected_trades_df)
            - cleaned_df: DataFrame with cleaned and filtered trade data
            - rejected_trades_df: DataFrame containing rejected trades that didn't match criteria
    """
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Make a copy for rejected trades
    rejected_trades = []
    
    # Clean the time column to keep only 'YYYY-MM-DD HH:MM:SS' format
    df['time'] = df['time'].str.slice(0, 19)
    
    # Convert time column to datetime
    df['time'] = pd.to_datetime(df['time'])
    
    # Create date and time columns from the datetime
    df['date'] = df['time'].dt.date
    df['time_of_day'] = df['time'].dt.time
    
    # Rename time to execution_timestamp
    df = df.rename(columns={'time': 'execution_timestamp'})
    
    # Drop specified columns (keeping identifier)
    columns_to_drop = ['multiplier', 'asset.strike', 'asset.multiplier']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    # Filter out trades with quantity NOT > 0
    initial_count = len(df)
    zero_quantity_mask = ~(df['filled_quantity'] > 0)
    
    # Store rejected zero quantity trades
    if any(zero_quantity_mask):
        rejected_zero_qty = df[zero_quantity_mask].copy()
        rejected_zero_qty['rejection_reason'] = "Quantity not greater than zero"
        rejected_trades.append(rejected_zero_qty)
    
    # Filter out zero quantity trades
    df = df[~zero_quantity_mask]
    filtered_count = initial_count - len(df)
    print(f"Filtered out {filtered_count} rows where filled_quantity is not greater than zero")
    
    # Combine all rejected trades into a single DataFrame
    rejected_trades_df = pd.concat(rejected_trades) if rejected_trades else pd.DataFrame()
    
    print(f"Cleaned time column and dropped columns: {', '.join(columns_to_drop)}")
    print(f"Remaining columns: {', '.join(df.columns)}")
    
    return df, rejected_trades_df

def identify_trades(df, strategy_side):
    """
    Group executions into complete trades by tracking open positions per symbol.
    Each time open_volume goes from 0 to non-zero and back to 0, it's a complete trade.
    Rejects orders that don't match the strategy pattern (e.g., buy orders in a sell strategy with no position).
    
    Args:
        df: DataFrame containing cleaned trade executions
        strategy_side: The main side of the strategy ('buy' or 'sell')
            - For 'buy' strategies: buy orders open positions, sell orders close them
            - For 'sell' strategies: sell orders open positions, buy orders close them
        
    Returns:
        tuple: (trades_df, trades_summary, rejected_trades_df)
            - trades_df: Original DataFrame with trade_id and open_volume columns
            - trades_summary: Summary DataFrame with metrics for each complete trade
            - rejected_trades_df: DataFrame containing rejected trades
            
    Raises:
        ValueError: If any executions are missing trade_id assignments
    """
    # Make a copy to avoid modifying the original
    df = df.copy().sort_values('execution_timestamp')
    
    # Initialize columns
    df['trade_id'] = np.nan
    df['open_volume'] = 0
    df['is_entry'] = False  # Flag for entry executions
    df['is_exit'] = False   # Flag for exit executions
    df['is_rejected'] = False  # Flag for rejected executions
    
    # Define opening and closing sides based on strategy
    if strategy_side == 'buy':
        # For buy strategy: opening orders should be buy
        opening_sides = ['buy', 'buy_to_cover', 'buy_to_close']
        closing_sides = ['sell', 'sell_to_close', 'sell_short']
    else:  # sell strategy
        # For sell strategy: opening orders should be sell
        opening_sides = ['sell', 'sell_to_close', 'sell_short']
        closing_sides = ['buy', 'buy_to_cover', 'buy_to_close']
    
    # Initialize variables
    current_trade_id = 0  # Start with 0 so first trade is 1
    open_positions = {}  # Dictionary to track open positions per symbol
    position_trade_ids = {}  # Dictionary to track trade_id for open positions
    rejected_trades = []  # List to store rejected trades
    
    # Process each execution
    for idx, row in df.iterrows():
        side = row['side']
        symbol = row['symbol']
        quantity = row['filled_quantity']
        
        # Initialize symbol tracking if not exists
        if symbol not in open_positions:
            open_positions[symbol] = 0
            position_trade_ids[symbol] = None
            
        # Get previous position value
        prev_position = open_positions[symbol]
        
        # Check if this is an unknown order type
        if side not in opening_sides and side not in closing_sides:
            df.at[idx, 'is_rejected'] = True
            rejected_row = df.loc[[idx]].copy()
            rejected_row['rejection_reason'] = f"Unknown order type '{side}' for {strategy_side} strategy"
            rejected_trades.append(rejected_row)
            print(f"Rejected unknown side '{side}' at row {idx}")
            continue
        
        # For a buy strategy:
        # - If there's no position and we have a sell order, reject it
        # For a sell strategy:
        # - If there's no position and we have a buy order, reject it
        if prev_position == 0:
            if (strategy_side == 'buy' and side in closing_sides) or \
               (strategy_side == 'sell' and side in closing_sides):
                df.at[idx, 'is_rejected'] = True
                rejected_row = df.loc[[idx]].copy()
                rejected_row['rejection_reason'] = f"No open position for {symbol}, cannot {side}"
                rejected_trades.append(rejected_row)
                print(f"Rejected {side} order with no open position for {symbol} at row {idx}")
                continue
        
        # Update position based on side and strategy type
        if strategy_side == 'buy':
            # For buy strategies: buys increase position, sells decrease
            if side in opening_sides:
                open_positions[symbol] += quantity
            elif side in closing_sides:
                open_positions[symbol] -= quantity
        else:  # strategy_side == 'sell'
            # For sell strategies: sells increase position (negative), buys decrease
            if side in opening_sides:
                open_positions[symbol] += quantity  # Increase negative position
            elif side in closing_sides:
                open_positions[symbol] -= quantity  # Decrease negative position
        
        # Check if we're starting a new position (entry)
        if prev_position == 0 and open_positions[symbol] != 0:
            # Starting a new position from zero
            current_trade_id += 1
            position_trade_ids[symbol] = current_trade_id
            df.at[idx, 'is_entry'] = True  # Mark as entry
            print(f"New trade {current_trade_id} started for {symbol}: {side} {quantity}")
        
        # Assign trade_id from the current position's trade
        df.at[idx, 'trade_id'] = position_trade_ids[symbol]
        df.at[idx, 'open_volume'] = open_positions[symbol]
        
        # Check if position was closed (exit)
        if prev_position != 0 and open_positions[symbol] == 0:
            df.at[idx, 'is_exit'] = True  # Mark as exit
            print(f"Trade {position_trade_ids[symbol]} closed for {symbol}")
            position_trade_ids[symbol] = None
    
    # Check for any remaining open positions
    for symbol, volume in open_positions.items():
        if volume != 0:
            print(f"Warning: Ending with open position of {volume} for {symbol} (Trade ID: {position_trade_ids[symbol]})")
    
    # Create a rejected trades DataFrame
    rejected_trades_df = pd.concat(rejected_trades) if rejected_trades else pd.DataFrame()
    
    # Filter out rejected trades from the main DataFrame
    df = df[~df['is_rejected']]
    
    # Verify all executions have a valid trade_id
    missing_trade_ids = df['trade_id'].isna().sum()
    if missing_trade_ids > 0:
        print(f"ERROR: {missing_trade_ids} executions are missing trade_id assignments")
        problematic_rows = df[df['trade_id'].isna()]
        print("\nProblematic rows:")
        print(problematic_rows[['execution_timestamp', 'symbol', 'side', 'filled_quantity']])
        raise ValueError("Trade identification failed: Some executions have no trade_id assigned")
    
    # Create a summary of trades with improved metrics
    trades_summary = []
    
    # Process each trade
    for trade_id in df['trade_id'].unique():
        trade_df = df[df['trade_id'] == trade_id]
        
        # Extract basic information
        symbol = trade_df['symbol'].iloc[0]
        
        # Get entry and exit timestamps
        entry_row = trade_df[trade_df['is_entry']].iloc[0] if any(trade_df['is_entry']) else trade_df.iloc[0]
        exit_row = trade_df[trade_df['is_exit']].iloc[0] if any(trade_df['is_exit']) else trade_df.iloc[-1]
        
        # Extract date and time
        start_date = entry_row['date']
        start_time = entry_row['time_of_day']
        end_date = exit_row['date']
        end_time = exit_row['time_of_day']
        
        # Calculate duration in hours
        start_timestamp = entry_row['execution_timestamp']
        end_timestamp = exit_row['execution_timestamp']
        duration_hours = (end_timestamp - start_timestamp).total_seconds() / 3600
        
        # Get entry and exit prices
        entry_price = entry_row['price']
        exit_price = exit_row['price']
        
        # Get quantity (use the entry quantity)
        quantity = entry_row['filled_quantity']
        
        # Number of executions
        num_executions = len(trade_df)
        
        # Create trade summary record
        trade_summary = {
            'trade_id': trade_id,
            'num_executions': num_executions,
            'symbol': symbol,
            'start_date': start_date,
            'start_time': start_time,
            'end_date': end_date,
            'end_time': end_time,
            'duration_hours': duration_hours,
            'quantity': quantity,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'exit_type': ''  # Add empty exit_type column
        }
        
        trades_summary.append(trade_summary)
    
    # Convert to DataFrame
    trades_summary_df = pd.DataFrame(trades_summary)
    
    print(f"Identified {len(trades_summary_df)} complete trades from {len(df)} executions")
    if not rejected_trades_df.empty:
        print(f"Rejected {len(rejected_trades_df)} executions that didn't match strategy pattern")
    
    return df, trades_summary_df, rejected_trades_df

def calculate_trade_metrics(trades_summary, trades_df, strategy_params=None):
    """
    Calculate all trade metrics and prepare data for HTML report generation.
    
    Args:
        trades_summary (pd.DataFrame): DataFrame containing trade summary data
        trades_df (pd.DataFrame): DataFrame containing detailed trade data
        strategy_params (dict): Dictionary containing strategy parameters
        
    Returns:
        dict: Dictionary containing all calculated metrics and formatted data
    """
    # Initialize results dictionary
    results = {
        'trades_summary_display': None,
        'trades_df_display': None,
        'weekly_metrics_df': pd.DataFrame(),
        'monthly_metrics_df': pd.DataFrame(),
        'yearly_metrics_df': pd.DataFrame(),  # Add yearly metrics
        'strategy_metrics': {
            'side': None,
            'stop_loss': None,
            'risk_reward': None,
            'risk_per_trade': None
        }
    }
    
    # Format trades_summary for better display
    trades_summary_display = trades_summary.copy()
    
    # Strategy parameters details for display
    strategy_side = None
    strategy_stop_loss = None
    strategy_risk_reward = None
    strategy_risk_per_trade = None
    
    # Add stop price calculation if stop_loss parameter is available
    if strategy_params and 'stop_loss' in strategy_params and 'side' in strategy_params:
        stop_loss = strategy_params['stop_loss']
        side = strategy_params['side']
        
        # Save for display in header
        strategy_side = side
        strategy_stop_loss = stop_loss
        results['strategy_metrics']['side'] = side
        results['strategy_metrics']['stop_loss'] = stop_loss
        
        # Get risk_reward if available
        if 'risk_reward' in strategy_params:
            strategy_risk_reward = strategy_params['risk_reward']
            results['strategy_metrics']['risk_reward'] = strategy_risk_reward
            
        # Get risk_per_trade if available
        if 'risk_per_trade' in strategy_params:
            strategy_risk_per_trade = strategy_params['risk_per_trade']
            risk_per_trade = strategy_params['risk_per_trade']
            results['strategy_metrics']['risk_per_trade'] = risk_per_trade
        else:
            # perc_return calculation requires risk_per_trade
            print("Warning: risk_per_trade not provided - percentage return calculation will be skipped")
            risk_per_trade = None
        
        # Calculate stop price based on side and entry price
        if side == 'buy':
            trades_summary_display['stop_price'] = trades_summary_display['entry_price'] - stop_loss
        else:  # sell
            trades_summary_display['stop_price'] = trades_summary_display['entry_price'] + stop_loss
            
        # Round stop price to 2 decimal places
        trades_summary_display['stop_price'] = trades_summary_display['stop_price'].round(2)
        
        # Calculate take profit price if risk_reward is available
        if strategy_risk_reward:
            risk_reward = strategy_params['risk_reward']
            if side == 'buy':
                # For buy trades: entry_price + (entry_price - stop_price) * risk_reward
                trades_summary_display['take_profit_price'] = trades_summary_display['entry_price'] + \
                    (trades_summary_display['entry_price'] - trades_summary_display['stop_price']) * risk_reward
            else:  # sell
                # For sell trades: entry_price - (stop_price - entry_price) * risk_reward
                trades_summary_display['take_profit_price'] = trades_summary_display['entry_price'] - \
                    (trades_summary_display['stop_price'] - trades_summary_display['entry_price']) * risk_reward
            
            # Round take profit price to 2 decimal places
            trades_summary_display['take_profit_price'] = trades_summary_display['take_profit_price'].round(2)
            
            print(f"Added take profit price calculation (risk_reward: {risk_reward})")
            
        # Calculate capital required and place it after stop_price
        # We'll do this by creating a new DataFrame with the columns in the desired order
        trades_summary_display['capital_required'] = (trades_summary_display['entry_price'] * trades_summary_display['quantity']).round(2)
        
        # Determine exit type for each trade
        # Use a small tolerance for price comparisons (1% of entry price)
        price_tolerance = 0.01
        
        # First, ensure end_time is a proper time object for all trades
        try:
            # Check if at least the first non-null value is a time object
            is_time_already = isinstance(trades_summary_display['end_time'].dropna().iloc[0], time)
        except (IndexError, AttributeError):
            is_time_already = False
            
        if not is_time_already:
            # Convert end_time to time objects if they aren't already
            trades_summary_display['end_time'] = pd.to_datetime(trades_summary_display['end_time'], format='%H:%M:%S', errors='coerce').dt.time
        
        # Define market close window time range
        market_close_start = time(15, 50, 0)  # 15:50:00
        market_close_end = time(16, 0, 0)  # 16:00:00
        
        # Create mask for trades that ended during market close window
        end_of_day_mask = trades_summary_display['end_time'].apply(
            lambda x: market_close_start <= x <= market_close_end if pd.notna(x) else False
        )
        
        # Initialize all exit types based on end of day check
        trades_summary_display['exit_type'] = "end of day"
        trades_summary_display.loc[~end_of_day_mask, 'exit_type'] = "unclassified"
        
        # Check for stop losses for all trades
        if side == 'buy':
            # For buy trades: exit_price <= stop_price + tolerance = stop loss
            stop_mask = trades_summary_display['exit_price'] <= (trades_summary_display['stop_price'] + price_tolerance)
            trades_summary_display.loc[stop_mask, 'exit_type'] = "stop"
            
            # For buy trades: If risk_reward exists, check for take profits
            if strategy_risk_reward:
                # exit_price >= take_profit_price - tolerance = take profit
                tp_mask = trades_summary_display['exit_price'] >= (trades_summary_display['take_profit_price'] - price_tolerance)
                trades_summary_display.loc[tp_mask, 'exit_type'] = "take profit"
        else:  # sell
            # For sell trades: exit_price >= stop_price - tolerance = stop loss
            stop_mask = trades_summary_display['exit_price'] >= (trades_summary_display['stop_price'] - price_tolerance)
            trades_summary_display.loc[stop_mask, 'exit_type'] = "stop"
            
            # For sell trades: If risk_reward exists, check for take profits
            if strategy_risk_reward:
                # exit_price <= take_profit_price + tolerance = take profit
                tp_mask = trades_summary_display['exit_price'] <= (trades_summary_display['take_profit_price'] + price_tolerance)
                trades_summary_display.loc[tp_mask, 'exit_type'] = "take profit"
        
        # Convert any remaining unclassified trades to end of day
        trades_summary_display.loc[trades_summary_display['exit_type'] == "unclassified", 'exit_type'] = "end of day"
        
        print("Classified trade exits as: stop, take profit, or end of day")
        
        # Using original exit prices from the data, no adjustment
        
        # Recalculate actual risk/reward ratio with the adjusted exit prices
        if side == 'buy':
            # For buy trades: (exit_price - entry_price) / (entry_price - stop_price)
            trades_summary_display['actual_risk_reward'] = (
                (trades_summary_display['exit_price'] - trades_summary_display['entry_price']) / 
                (trades_summary_display['entry_price'] - trades_summary_display['stop_price'])
            )
        else:  # sell
            # For sell trades: (entry_price - exit_price) / (stop_price - entry_price)
            trades_summary_display['actual_risk_reward'] = (
                (trades_summary_display['entry_price'] - trades_summary_display['exit_price']) / 
                (trades_summary_display['stop_price'] - trades_summary_display['entry_price'])
            )
        
        # Handle possible infinity or NaN values in risk_reward
        trades_summary_display['actual_risk_reward'] = trades_summary_display['actual_risk_reward'].replace([np.inf, -np.inf], np.nan)
        
        # Round actual_risk_reward to 2 decimal places
        trades_summary_display['actual_risk_reward'] = trades_summary_display['actual_risk_reward'].round(2)
        
        print("Recalculated risk/reward ratios with adjusted exit prices")
        
        # Add winning trade column based on adjusted exit price comparison
        # For floating point comparison, use a small tolerance
        tolerance = 0.01  # 1 cent tolerance for price comparison
        
        if side == 'buy':
            # For buy trades: 
            # - Winning trade (1): exit_price > entry_price (sold higher than bought)
            # - Losing trade (0): exit_price <= entry_price (sold lower than or same as bought)
            trades_summary_display['winning_trade'] = (
                trades_summary_display['exit_price'] > (trades_summary_display['entry_price'] + tolerance)
            ).astype(int)
        else:  # sell
            # For sell trades:
            # - Winning trade (1): exit_price < entry_price (bought back lower than sold)
            # - Losing trade (0): exit_price >= entry_price (bought back higher than or same as sold)
            trades_summary_display['winning_trade'] = (
                trades_summary_display['exit_price'] < (trades_summary_display['entry_price'] - tolerance)
            ).astype(int)
            
            # Calculate percentage return only if risk_per_trade is available
        if risk_per_trade is not None:
            # Calculate percentage return using risk_per_trade * actual_risk_reward for ALL trades
            trades_summary_display['perc_return'] = risk_per_trade * trades_summary_display['actual_risk_reward']
            
            # Convert to percentage format and round
            trades_summary_display['perc_return'] = (trades_summary_display['perc_return'] * 100).round(2)
            
            # Add week, month, and year columns for time-based grouping
            trades_summary_calc = trades_summary_display.copy()
            
            # Convert start_date to datetime if it's not already
            if not pd.api.types.is_datetime64_any_dtype(trades_summary_calc['start_date']):
                trades_summary_calc['start_date'] = pd.to_datetime(trades_summary_calc['start_date'])
            
            # Extract week, month, and year
            trades_summary_calc['week'] = trades_summary_calc['start_date'].dt.to_period('W').astype(str)
            trades_summary_calc['month'] = trades_summary_calc['start_date'].dt.to_period('M').astype(str)
            trades_summary_calc['year'] = trades_summary_calc['start_date'].dt.year
            
            # Add the time period columns to the display DataFrame
            trades_summary_display['week'] = trades_summary_calc['week']
            trades_summary_display['month'] = trades_summary_calc['month']
            trades_summary_display['year'] = trades_summary_calc['year']
            
            # Create weekly metrics
            weekly_metrics = []
            for week, week_df in trades_summary_calc.groupby('week'):
                # Extract week number and year from the period string
                # Period format example: '2023-01-02/2023-01-08'
                week_date = pd.to_datetime(week.split('/')[0])
                week_num = week_date.isocalendar()[1]  # ISO week number
                year = week_date.year
                
                # Use the actual dates in the dataframe to determine the year
                # This handles edge cases where ISO week might be from previous/next year
                actual_year = week_df['start_date'].dt.year.iloc[0]
                
                total_trades = len(week_df)
                winning_trades = week_df['winning_trade'].sum()
                accuracy = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # Get average risk reward for winning and losing trades
                winning_mask = week_df['winning_trade'] == 1
                losing_mask = week_df['winning_trade'] == 0
                avg_win = week_df.loc[winning_mask, 'actual_risk_reward'].mean() if winning_mask.any() else 0
                avg_loss = week_df.loc[losing_mask, 'actual_risk_reward'].mean() if losing_mask.any() else 0
                avg_risk_reward = week_df['actual_risk_reward'].mean()
                
                # Calculate total return
                total_return = week_df['perc_return'].sum()
                
                weekly_metrics.append({
                    'Period': f"Week {week_num}, {actual_year}",
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{week_df['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                })
            
            # Create monthly metrics
            monthly_metrics = []
            for month, month_df in trades_summary_calc.groupby('month'):
                # Extract month name and year from period string
                # Period format example: '2023-01'
                month_date = pd.to_datetime(month)
                month_name = month_date.strftime('%B')  # Full month name
                year = month_date.year
                
                total_trades = len(month_df)
                winning_trades = month_df['winning_trade'].sum()
                accuracy = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # Get average risk reward for winning and losing trades
                winning_mask = month_df['winning_trade'] == 1
                losing_mask = month_df['winning_trade'] == 0
                avg_win = month_df.loc[winning_mask, 'actual_risk_reward'].mean() if winning_mask.any() else 0
                avg_loss = month_df.loc[losing_mask, 'actual_risk_reward'].mean() if losing_mask.any() else 0
                avg_risk_reward = month_df['actual_risk_reward'].mean()
                
                # Calculate total return
                total_return = month_df['perc_return'].sum()
                
                monthly_metrics.append({
                    'Period': f"{month_name} {year}",
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{month_df['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                })
            
            # Create yearly metrics
            yearly_metrics = []
            for year, year_df in trades_summary_calc.groupby('year'):
                total_trades = len(year_df)
                winning_trades = year_df['winning_trade'].sum()
                accuracy = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # Get average risk reward for winning and losing trades
                winning_mask = year_df['winning_trade'] == 1
                losing_mask = year_df['winning_trade'] == 0
                avg_win = year_df.loc[winning_mask, 'actual_risk_reward'].mean() if winning_mask.any() else 0
                avg_loss = year_df.loc[losing_mask, 'actual_risk_reward'].mean() if losing_mask.any() else 0
                avg_risk_reward = year_df['actual_risk_reward'].mean()
                
                # Calculate total return
                total_return = year_df['perc_return'].sum()
                
                yearly_metrics.append({
                    'Period': str(year),
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{year_df['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                })
            
            # Convert to DataFrames
            weekly_metrics_df = pd.DataFrame(weekly_metrics)
            monthly_metrics_df = pd.DataFrame(monthly_metrics)
            yearly_metrics_df = pd.DataFrame(yearly_metrics)
            
            # Add totals row to weekly metrics
            if not weekly_metrics_df.empty:
                total_trades = len(trades_summary_calc)
                winning_trades = trades_summary_calc['winning_trade'].sum()
                accuracy = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # Get average risk reward for winning and losing trades for all weeks
                winning_mask = trades_summary_calc['winning_trade'] == 1
                losing_mask = trades_summary_calc['winning_trade'] == 0
                avg_win = trades_summary_calc.loc[winning_mask, 'actual_risk_reward'].mean() if winning_mask.any() else 0
                avg_loss = trades_summary_calc.loc[losing_mask, 'actual_risk_reward'].mean() if losing_mask.any() else 0
                avg_risk_reward = trades_summary_calc['actual_risk_reward'].mean()
                
                # Calculate total return for all weeks
                total_return = trades_summary_calc['perc_return'].sum()
                
                # Create a total row
                total_row = pd.DataFrame([{
                    'Period': 'TOTAL',
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{trades_summary_calc['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                }])
                
                # Append the total row to the weekly metrics
                weekly_metrics_df = pd.concat([weekly_metrics_df, total_row], ignore_index=True)
            
            # Add totals row to monthly metrics
            if not monthly_metrics_df.empty:
                # Reuse the same total values calculated above
                total_row = pd.DataFrame([{
                    'Period': 'TOTAL',
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{trades_summary_calc['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                }])
                
                # Append the total row to the monthly metrics
                monthly_metrics_df = pd.concat([monthly_metrics_df, total_row], ignore_index=True)
            
            # Add totals row to yearly metrics
            if not yearly_metrics_df.empty:
                # Reuse the same total values calculated above
                total_row = pd.DataFrame([{
                    'Period': 'TOTAL',
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{trades_summary_calc['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                }])
                
                # Append the total row to the yearly metrics
                yearly_metrics_df = pd.concat([yearly_metrics_df, total_row], ignore_index=True)
            
            results['weekly_metrics_df'] = weekly_metrics_df
            results['monthly_metrics_df'] = monthly_metrics_df
            results['yearly_metrics_df'] = yearly_metrics_df
            
            print(f"Added stop price calculation (side: {side}, stop_loss: {stop_loss})")
            print(f"Added winning trade column based on entry vs exit price comparison")
            print(f"Added capital required and actual risk/reward columns")
            print(f"Added percentage return column (risk_per_trade × actual_risk_reward)")
            print(f"Added weekly, monthly, and yearly performance metrics")
        else:
            # If no strategy parameters, still calculate capital required
            trades_summary_display['capital_required'] = (trades_summary_display['entry_price'] * trades_summary_display['quantity']).round(2)
            
            results['weekly_metrics_df'] = pd.DataFrame()
            results['monthly_metrics_df'] = pd.DataFrame()
            results['yearly_metrics_df'] = pd.DataFrame()
    else:
        # If no strategy parameters, still calculate capital required
        trades_summary_display['capital_required'] = (trades_summary_display['entry_price'] * trades_summary_display['quantity']).round(2)
        
        results['weekly_metrics_df'] = pd.DataFrame()
        results['monthly_metrics_df'] = pd.DataFrame()
        results['yearly_metrics_df'] = pd.DataFrame()
    
    # Format numeric columns
    if 'duration_hours' in trades_summary_display.columns:
        trades_summary_display['duration_hours'] = trades_summary_display['duration_hours'].round(2)
    if 'entry_price' in trades_summary_display.columns:
        trades_summary_display['entry_price'] = trades_summary_display['entry_price'].round(2)
    if 'exit_price' in trades_summary_display.columns:
        trades_summary_display['exit_price'] = trades_summary_display['exit_price'].round(2)
    
    # Ensure trade_id is displayed as integer without decimals
    trades_summary_display['trade_id'] = trades_summary_display['trade_id'].astype(int)
    
    # Reorder columns to put capital_required after stop_price
    if 'stop_price' in trades_summary_display.columns:
        # Get all column names
        all_cols = trades_summary_display.columns.tolist()
        
        # Remove columns we want to reorder
        if 'capital_required' in all_cols:
            all_cols.remove('capital_required')
        if 'stop_price' in all_cols:
            all_cols.remove('stop_price')
        if 'entry_price' in all_cols:
            all_cols.remove('entry_price')
        if 'exit_price' in all_cols:
            all_cols.remove('exit_price')
            
        # Find the position of entry_price (where it would be)
        try:
            # If there's a quantity column, insert entry_price after it
            quantity_pos = all_cols.index('quantity')
            
            # Insert columns in the desired order: entry_price, stop_price, exit_price, capital_required
            all_cols.insert(quantity_pos + 1, 'entry_price')
            all_cols.insert(quantity_pos + 2, 'stop_price')
            all_cols.insert(quantity_pos + 3, 'exit_price')
            all_cols.insert(quantity_pos + 4, 'capital_required')
        except ValueError:
            # If no quantity column, append them at the end in the right order
            all_cols.extend(['entry_price', 'stop_price', 'exit_price', 'capital_required'])
        
        # Reorder the DataFrame
        trades_summary_display = trades_summary_display[all_cols]
    
    # Format capital_required with commas
    if 'capital_required' in trades_summary_display.columns:
        trades_summary_display['capital_required'] = trades_summary_display['capital_required'].apply(lambda x: f"{x:,.2f}")
        
    # Format perc_return as percentage with % symbol
    if 'perc_return' in trades_summary_display.columns:
        trades_summary_display['perc_return'] = trades_summary_display['perc_return'].apply(lambda x: f"{x:+.2f}%" if not pd.isna(x) else "")
    
    # Create a copy of trades_df for display and convert trade_id to int
    trades_df_display = trades_df.copy()
    trades_df_display['trade_id'] = trades_df_display['trade_id'].astype(int)
    
    # Store formatted DataFrames in results
    results['trades_summary_display'] = trades_summary_display
    results['trades_df_display'] = trades_df_display
    
    return results

def generate_html_report(trades_df, trades_summary, output_file='trade_report.html', auto_open=True, original_file=None, strategy_params=None, rejected_trades=None):
    """
    Generate an HTML report from the trade execution data and summary
    
    Args:
        trades_df: DataFrame with detailed trade executions
        trades_summary: DataFrame with trade summaries
        output_file: Path to save the HTML report
        auto_open: Whether to automatically open the report in a browser
        original_file: Path to the original trades CSV file
        strategy_params: Dictionary of strategy parameters
        rejected_trades: DataFrame containing trades that were rejected during processing
        
    Returns:
        str: Path to the generated HTML file
    """
    # Create output directory if needed
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Calculate all metrics using the new function
    metrics = calculate_trade_metrics(trades_summary, trades_df, strategy_params)
    
    # Extract data from metrics dictionary
    trades_summary_display = metrics['trades_summary_display']
    trades_df_display = metrics['trades_df_display']
    weekly_metrics_df = metrics['weekly_metrics_df']
    monthly_metrics_df = metrics['monthly_metrics_df']
    yearly_metrics_df = metrics['yearly_metrics_df']
    strategy_metrics = metrics['strategy_metrics']
    
    # Strategy parameters display
    strategy_side = strategy_metrics['side']
    strategy_stop_loss = strategy_metrics['stop_loss']
    strategy_risk_reward = strategy_metrics['risk_reward']
    strategy_risk_per_trade = strategy_metrics['risk_per_trade']
    
    # Load original CSV data if provided
    original_data_html = ""
    if original_file and os.path.exists(original_file):
        try:
            # Read the original CSV file
            original_df = pd.read_csv(original_file)
            # Add the original data section
            original_data_html = f"""
            <h2>Original CSV Data</h2>
            <p>This is the raw data from the CSV file before processing.</p>
            {original_df.head(30).to_html(index=False)}
            <p><em>Note: Showing first 30 rows only. Total rows: {len(original_df)}</em></p>
            """
        except Exception as e:
            original_data_html = f"""
            <h2>Original CSV Data</h2>
            <p>Error loading original CSV file: {str(e)}</p>
            """
    
    # Generate Rejected Trades HTML
    rejected_trades_html = ""
    if rejected_trades is not None and not rejected_trades.empty:
        rejected_trades_html = f"""
        <div class="section">
            <h2>Rejected Trades</h2>
            <p>These trades were filtered out during processing because they didn't meet the criteria for this strategy.</p>
            {rejected_trades.to_html(index=False)}
            <p><em>Total rejected trades: {len(rejected_trades)}</em></p>
        </div>
        """
    
    # Generate Weekly Metrics HTML
    weekly_metrics_html = ""
    if not weekly_metrics_df.empty:
        # Add CSS class to the total row
        weekly_metrics_html_table = weekly_metrics_df.to_html(index=False)
        weekly_metrics_html_table = weekly_metrics_html_table.replace('<tr>', '<tr class="row">')
        weekly_metrics_html_table = weekly_metrics_html_table.replace('<tr class="row">\n      <td>TOTAL</td>', '<tr class="total-row">\n      <td>TOTAL</td>')
        
        weekly_metrics_html = f"""
        <div class="section">
            <h2>Weekly Performance Metrics</h2>
            {weekly_metrics_html_table}
        </div>
        """
    
    # Generate Monthly Metrics HTML
    monthly_metrics_html = ""
    if not monthly_metrics_df.empty:
        # Add CSS class to the total row
        monthly_metrics_html_table = monthly_metrics_df.to_html(index=False)
        monthly_metrics_html_table = monthly_metrics_html_table.replace('<tr>', '<tr class="row">')
        monthly_metrics_html_table = monthly_metrics_html_table.replace('<tr class="row">\n      <td>TOTAL</td>', '<tr class="total-row">\n      <td>TOTAL</td>')
        
        monthly_metrics_html = f"""
        <div class="section">
            <h2>Monthly Performance Metrics</h2>
            {monthly_metrics_html_table}
        </div>
        """
    
    # Generate Yearly Metrics HTML
    yearly_metrics_html = ""
    if not yearly_metrics_df.empty:
        # Add CSS class to the total row
        yearly_metrics_html_table = yearly_metrics_df.to_html(index=False)
        yearly_metrics_html_table = yearly_metrics_html_table.replace('<tr>', '<tr class="row">')
        yearly_metrics_html_table = yearly_metrics_html_table.replace('<tr class="row">\n      <td>TOTAL</td>', '<tr class="total-row">\n      <td>TOTAL</td>')
        
        yearly_metrics_html = f"""
        <div class="section">
            <h2>Yearly Performance Metrics</h2>
            {yearly_metrics_html_table}
        </div>
        """
    
    # Strategy parameters HTML
    strategy_params_html = ""
    source_file_html = f"""<p><span class="highlight">Source File:</span> {os.path.basename(original_file) if original_file else "Unknown"}</p>"""
    
    if strategy_side is not None:
        direction_html = f"""<p><span class="highlight">Direction:</span> {strategy_side.upper()}</p>"""
        strategy_params_html += direction_html
        
        if strategy_stop_loss is not None:
            stop_loss_html = f"""<p><span class="highlight">Stop Loss:</span> ${strategy_stop_loss:.2f}</p>"""
            strategy_params_html += stop_loss_html
            
        if strategy_risk_reward is not None:
            risk_reward_html = f"""<p><span class="highlight">Risk Reward:</span> {strategy_risk_reward:.1f}</p>"""
            strategy_params_html += risk_reward_html
            
        if strategy_risk_per_trade is not None:
            # Format risk_per_trade as a percentage (multiply by 100)
            risk_per_trade_pct = strategy_risk_per_trade * 100
            risk_per_trade_html = f"""<p><span class="highlight">Risk Per Trade:</span> {risk_per_trade_pct:.2f}%</p>"""
            strategy_params_html += risk_per_trade_html
            
            # Add source file after strategy parameters
            strategy_params_html += source_file_html
    else:
        # If no strategy parameters, just show source file
        strategy_params_html = source_file_html
    
    # HTML template
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Trade Analysis Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2 {{ color: #2c3e50; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f2f2f2; }}
            tr:hover {{ background-color: #f5f5f5; }}
            .summary {{ background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .highlight {{ font-weight: bold; }}
            .section {{ margin-bottom: 30px; border-bottom: 1px solid #eee; padding-bottom: 20px; }}
            .total-row {{ background-color: #f8f9fa; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1>Trade Analysis Report</h1>
        <div class="summary">
            <p><span class="highlight">Report Generated:</span> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><span class="highlight">Total Trades:</span> {len(trades_summary)}</p>
            <p><span class="highlight">Total Executions:</span> {len(trades_df)}</p>
            <p><span class="highlight">Symbols Traded:</span> {', '.join(trades_df['symbol'].unique())}</p>
            {strategy_params_html}
        </div>
        
        {yearly_metrics_html}
        
        {monthly_metrics_html}
        
        {weekly_metrics_html}
        
        <div class="section">
            <h2>Trade Summary</h2>
            {trades_summary_display.to_html(index=False)}
        </div>
        
        <div class="section">
            <h2>Processed Executions</h2>
            {trades_df_display[['execution_timestamp', 'date', 'time_of_day', 'identifier', 'symbol', 'side', 'filled_quantity', 'price', 'trade_id', 'open_volume']].head(20).to_html(index=False)}
            <p><em>Note: Showing first 20 rows only. Total rows: {len(trades_df)}</em></p>
        </div>
        
        {rejected_trades_html}
        
        <div class="section">
            {original_data_html}
        </div>
    </body>
    </html>
    """
    
    # Write the HTML file
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"HTML report generated: {output_file}")
    
    # Automatically open the HTML file in the default browser if requested
    if auto_open:
        try:
            # Convert to absolute path for browser
            abs_path = os.path.abspath(output_file)
            print(f"Opening report in browser: {abs_path}")
            # Use file:// protocol for local files
            webbrowser.open(f"file://{abs_path}")
        except Exception as e:
            print(f"Warning: Could not open browser automatically: {str(e)}")
    
    return output_file

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Process trades from CSV files")
    parser.add_argument("--stop_loss", type=float, help="Stop loss parameter from the strategy")
    parser.add_argument("--side", type=str, help="Trade side (buy/sell) from the strategy")
    parser.add_argument("--risk_reward", type=float, help="Risk reward ratio from the strategy")
    parser.add_argument("--risk_per_trade", type=float, help="Risk per trade as a percentage of capital")
    parser.add_argument("--file_path", type=str, help="Path to the trades CSV file to process")
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    # Create strategy parameters dictionary if args are provided
    strategy_params = {}
    if args.stop_loss is not None:
        strategy_params['stop_loss'] = args.stop_loss
    if args.side:
        strategy_params['side'] = args.side
    if args.risk_reward is not None:
        strategy_params['risk_reward'] = args.risk_reward
    if args.risk_per_trade is not None:
        strategy_params['risk_per_trade'] = args.risk_per_trade
        
    if strategy_params:
        print(f"Received strategy parameters: {strategy_params}")
    
    # Check if file_path is provided as an argument, otherwise use get_latest_trades_file
    if args.file_path:
        latest_file = Path(args.file_path)
        print(f"Using provided file path: {latest_file}")
    else:
        # Fallback to automatic discovery
        latest_file = get_latest_trades_file()
        
    if latest_file:
        print(f"File path: {latest_file.absolute()}")
        
        # Clean the trades data
        cleaned_data, rejected_qty_trades = clean_trades_file(latest_file)
        print(f"Processed {len(cleaned_data)} trade records")
        
        try:
            # Identify complete trades
            if strategy_params and 'side' in strategy_params:
                strategy_side = strategy_params['side']
            else:
                strategy_side = 'buy'
                print("No strategy side provided, defaulting to 'buy' for position tracking.")
                
            trades_df, trades_summary, rejected_strategy_trades = identify_trades(cleaned_data, strategy_side)
            
            # Display preview of the trades
            print("\nPreview of identified trades:")
            print(trades_df[['execution_timestamp', 'date', 'time_of_day', 'identifier', 'symbol', 'side', 'filled_quantity', 'price', 'trade_id', 'open_volume']].head(10))
            
            print("\nTrade summary:")
            print(trades_summary.head())
            
            # Combine rejected trades from both steps
            all_rejected_trades = pd.concat([rejected_qty_trades, rejected_strategy_trades]) if not rejected_qty_trades.empty or not rejected_strategy_trades.empty else pd.DataFrame()
            
            # Display rejected trades summary if any
            if not all_rejected_trades.empty:
                print(f"\nRejected {len(all_rejected_trades)} incompatible trades")
                print(all_rejected_trades.groupby('rejection_reason').size())
            
            # Generate HTML report and open it in browser
            # Get timestamp from the trades file name if possible, otherwise use current time
            timestamp = ""
            if latest_file:
                # Try to extract timestamp from file name (format like "strategy_YYYY-MM-DD_HH-MM_ID_trades.csv")
                filename = os.path.basename(latest_file)
                parts = filename.split('_')
                if len(parts) >= 3 and parts[-1] == "trades.csv":
                    # Use the timestamp from the filename
                    timestamp = f"_{parts[1]}_{parts[2]}"
                else:
                    # Use current timestamp
                    timestamp = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Save to logs directory instead of reports
            report_file = os.path.join('logs', f"trade_report{timestamp}.html")
            generate_html_report(trades_df, trades_summary, report_file, auto_open=True, original_file=latest_file, strategy_params=strategy_params, rejected_trades=all_rejected_trades)
            
        except ValueError as e:
            print(f"\nERROR: {str(e)}")
            print("HTML report was not generated due to errors in trade identification.")
            sys.exit(1)