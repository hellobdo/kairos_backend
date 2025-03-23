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
    # Imported and used as module
    from analytics.backtest_executions import process_backtest_executions, clean_backtest_executions, identify_trades
    from utils.html_generator import generate_html_report
    
    # Process directly from a strategy class
    process_backtest_executions(StrategyClass, trades_file_path)
    
    # Or process specific file
    file_path = get_latest_trade_report("csv")  # From utils.get_latest_trade_report
    cleaned_data, rejected_trades = clean_backtest_executions(file_path)
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
import calendar
from utils.get_latest_trade_report import get_latest_trade_report
from utils.html_generator import generate_html_report
from utils.trade_metrics import create_trades_summary, calculate_trade_metrics


def process_backtest_executions(strategy, file_path):
    """
    Process trades from a backtest strategy directly.
    
    Args:
        strategy_class: The strategy class containing the parameters to use
        file_path: The path to the trades CSV file to process
    Returns:
        bool: True if processing was successful, False otherwise
    """
    try:
        print("\nGenerating trade reports...")
            
        # Extract parameters from the strategy class
        strategy_params = {}
        # Add all parameters from the strategy class parameters dictionary
        for key, value in strategy.parameters.items():
            strategy_params[key] = value
            
        print(f"Using strategy parameters: {strategy_params}")

        if 'side' in strategy_params:
            strategy_side = strategy_params['side']
        else:
            raise ValueError("Strategy side (buy/sell) is required but not provided in strategy parameters")
            
        # Process the trades file
        try:
            # Clean the trades data
            cleaned_data, rejected_qty_trades = clean_backtest_executions(file_path)
            print(f"Processed {len(cleaned_data)} trade records")
                
            trades_df, rejected_strategy_trades = identify_trades(cleaned_data, strategy_side)
            
            # Create trade summary after identifying trades
            trades_summary = create_trades_summary(trades_df)
            
            # Combine rejected trades from both steps
            all_rejected_trades = pd.concat([rejected_qty_trades, rejected_strategy_trades]) if not rejected_qty_trades.empty or not rejected_strategy_trades.empty else pd.DataFrame()

            # Calculate all metrics using the new function
            metrics = calculate_trade_metrics(trades_summary, trades_df, strategy_params)
            
            # Generate HTML report and open it in browser
            # Get timestamp from the trades file name if possible, otherwise use current time
            timestamp = ""
            if file_path:
                # Try to extract timestamp from file name (format like "strategy_YYYY-MM-DD_HH-MM_ID_trades.csv")
                filename = os.path.basename(file_path)
                parts = filename.split('_')
                if len(parts) >= 3 and parts[-1] == "trades.csv":
                    # Use the timestamp from the filename
                    timestamp = f"_{parts[1]}_{parts[2]}"
                else:
                    # Use current timestamp
                    timestamp = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Save to logs directory
            report_file = os.path.join('logs', f"trade_report{timestamp}.html")
            generate_html_report(trades_df, trades_summary, metrics,report_file, auto_open=True, 
                                original_file=file_path, strategy_params=strategy_params, 
                                rejected_trades=all_rejected_trades)
            
            print("Trade reports generated successfully")
            return True
            
        except ValueError as e:
            print(f"\nERROR: {str(e)}")
            print("HTML report was not generated due to errors in trade identification.")
            return False
            
    except Exception as e:
        print(f"ERROR: Failed to process trades: {str(e)}")
        return False

def clean_backtest_executions(file_path):
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
    
    # Store rejected zero quantity executions
    if any(zero_quantity_mask):
        rejected_zero_qty = df[zero_quantity_mask].copy()
        rejected_zero_qty['rejection_reason'] = "Quantity not greater than zero"
        rejected_trades.append(rejected_zero_qty)
    
    # Filter out zero quantity executions
    df = df[~zero_quantity_mask]
    filtered_count = initial_count - len(df)
    print(f"Filtered out {filtered_count} rows where filled_quantity is not greater than zero")
    
    # Combine all rejected executions into a single DataFrame
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
        tuple: (trades_df, rejected_trades_df)
            - trades_df: Original DataFrame with trade_id and open_volume columns
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
    
    # Return the DataFrame with trade_id and open_volume columns, and rejected trades
    return df, rejected_trades_df

