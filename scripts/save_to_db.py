#!/usr/bin/env python3
"""
Save Backtest Results to Database

This script processes trade execution data and saves it to the SQLite database.
It can either use the most recent trades CSV file or a specific file provided as an argument.

Usage:
    python save_to_db.py                      # Uses latest trades file
    python save_to_db.py --file path/to/file  # Uses specific file
"""
import argparse
from pathlib import Path
import os
from datetime import datetime
import sys
import sqlite3
from contextlib import contextmanager
import pandas as pd

from process_trades import (
    clean_trades_file,
    identify_trades,
    calculate_trade_metrics
)

# Database configuration
DB_PATH = Path("data/trades.db")

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

def process_trades_file(file_path: Path, strategy_name: str, strategy_params: dict = None) -> tuple:
    """
    Process a trades file using functions from process_trades.py
    
    Args:
        file_path: Path to the trades CSV file
        strategy_name: Name of the strategy
        strategy_params: Optional dictionary of strategy parameters
        
    Returns:
        tuple: (trades_df, trades_summary_display, metrics)
            - trades_df: DataFrame with all executions and their trade IDs
            - trades_summary_display: DataFrame with trade summaries and calculated metrics
            - metrics: Dictionary with calculated metrics
    """
    print("\nProcessing trades file...")
    
    # Step 1: Clean the trades data
    print("Cleaning trades data...")
    cleaned_data, rejected_qty_trades = clean_trades_file(file_path)
    print(f"Found {len(cleaned_data)} valid executions")
    
    # Step 2: Identify trades
    print("\nIdentifying trades...")
    strategy_side = strategy_params.get('side', 'buy') if strategy_params else 'buy'
    trades_df, trades_summary, rejected_strategy_trades = identify_trades(cleaned_data, strategy_side)
    print(f"Identified {len(trades_summary)} complete trades")
    
    # Step 3: Calculate metrics
    print("\nCalculating trade metrics...")
    metrics = calculate_trade_metrics(trades_summary, trades_df, strategy_params)
    
    # Use trades_summary_display which has all calculated fields
    trades_summary_display = metrics['trades_summary_display']
    
    return trades_df, trades_summary_display, metrics

def save_executions(trades_df: pd.DataFrame, run_id: int) -> dict:
    """
    Save executions to the database and return a mapping of their IDs.
    
    Args:
        trades_df: DataFrame containing execution data with trade_ids
        run_id: ID of the backtest run
        
    Returns:
        dict: Mapping of (timestamp, symbol, side) to execution_id
    """
    execution_ids = {}
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Prepare executions data
        for _, row in trades_df.iterrows():
            # Convert datetime fields to strings
            execution_timestamp = row['execution_timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            date = row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], pd.Timestamp) else str(row['date'])
            time_of_day = str(row['time_of_day'])
            
            cursor.execute('''
                INSERT INTO backtest_executions (
                    execution_timestamp,
                    date,
                    time_of_day,
                    identifier,
                    symbol,
                    side,
                    filled_quantity,
                    price,
                    trade_id,
                    open_volume,
                    run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                execution_timestamp,
                date,
                time_of_day,
                row.get('identifier'),
                row['symbol'],
                row['side'],
                row['filled_quantity'],
                row['price'],
                row['trade_id'],
                row['open_volume'],
                run_id
            ))
            
            # Store the execution ID with a unique key
            exec_id = cursor.lastrowid
            key = (
                execution_timestamp,
                row['symbol'],
                row['side']
            )
            execution_ids[key] = exec_id
        
        conn.commit()
    
    print(f"Saved {len(execution_ids)} executions to database")
    return execution_ids

def save_trades(trades_summary: pd.DataFrame, run_id: int):
    """
    Save trades to the database, linking them to their executions.
    
    Args:
        trades_summary: DataFrame containing trade summary data
        run_id: ID of the backtest run
    """
    # Debug: Print DataFrame info
    print("\nAvailable columns:", trades_summary.columns.tolist())
    print("\nSample row:")
    print(trades_summary.iloc[0])
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        trades_saved = 0
        
        for _, row in trades_summary.iterrows():
            # Convert datetime fields to strings
            start_date = row['start_date'].strftime('%Y-%m-%d') if isinstance(row['start_date'], pd.Timestamp) else str(row['start_date'])
            start_time = str(row['start_time'])
            end_date = row['end_date'].strftime('%Y-%m-%d') if isinstance(row['end_date'], pd.Timestamp) else str(row['end_date'])
            end_time = str(row['end_time'])
            
            cursor.execute('''
                INSERT INTO backtest_trades (
                    trade_id,
                    num_executions,
                    symbol,
                    start_date,
                    start_time,
                    end_date,
                    end_time,
                    duration_hours,
                    quantity,
                    entry_price,
                    stop_price,
                    exit_price,
                    capital_required,
                    exit_type,
                    take_profit_price,
                    risk_reward,
                    winning_trade,
                    perc_return,
                    week,
                    month,
                    year,
                    run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['trade_id'],
                row.get('num_executions', 2),  # Default to 2 (entry + exit)
                row['symbol'],
                start_date,
                start_time,
                end_date,
                end_time,
                row['duration_hours'],
                row['quantity'],
                row['entry_price'],
                row.get('stop_price'),
                row['exit_price'],
                row['capital_required'],
                row.get('exit_type', 'unknown'),
                row.get('take_profit_price'),
                row.get('actual_risk_reward', 0),  # Default to 0 if actual_risk_reward is None
                bool(row.get('winning_trade', 0)),
                row.get('perc_return', 0),
                row.get('week', ''),
                row.get('month', ''),
                row.get('year', 0),
                run_id
            ))
            trades_saved += 1
        
        conn.commit()
    
    print(f"Saved {trades_saved} trades to database")

def create_backtest_run(strategy_name: str, strategy_params: dict, trades_df: pd.DataFrame) -> int:
    """
    Create a new backtest run entry and return its ID.
    
    Args:
        strategy_name: Name of the strategy
        strategy_params: Dictionary of strategy parameters
        trades_df: DataFrame containing trade data (used to determine date range)
        
    Returns:
        int: ID of the created backtest run
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get unique symbols from trades
        symbols = sorted(trades_df['symbol'].unique())
        symbols_str = ','.join(symbols)
        
        # Get date range from trades
        start_date = trades_df['date'].min()
        end_date = trades_df['date'].max()
        
        # Insert the run
        cursor.execute('''
            INSERT INTO backtest_runs (
                timestamp,
                strategy_name,
                symbols_traded,
                direction,
                stop_loss,
                risk_reward,
                risk_per_trade,
                backtest_start_date,
                backtest_end_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now(),
            strategy_name,
            symbols_str,
            strategy_params.get('side', 'buy'),
            strategy_params.get('stop_loss'),
            strategy_params.get('risk_reward'),
            strategy_params.get('risk_per_trade'),
            start_date,
            end_date
        ))
        
        run_id = cursor.lastrowid
        conn.commit()
        
        print(f"Created backtest run with ID: {run_id}")
        return run_id

def get_latest_trades_file(logs_dir: Path = None) -> Path:
    """
    Find the most recent trades CSV file in the logs directory.
    
    Args:
        logs_dir: Optional path to logs directory. Defaults to 'logs' in current directory.
        
    Returns:
        Path to the latest trades file
        
    Raises:
        FileNotFoundError: If no trades files are found
    """
    if logs_dir is None:
        logs_dir = Path("logs")
    
    if not logs_dir.exists():
        raise FileNotFoundError(f"Logs directory not found: {logs_dir}")
    
    # Find all CSV files with 'trades' in the name
    trade_files = list(logs_dir.glob("*trades*.csv"))
    
    if not trade_files:
        raise FileNotFoundError(f"No trade CSV files found in {logs_dir}")
    
    # Get the latest file based on creation time
    latest_file = max(trade_files, key=os.path.getctime)
    
    print(f"Latest trades file found: {latest_file}")
    print(f"Created at: {datetime.fromtimestamp(os.path.getctime(latest_file))}")
    
    return latest_file

def main():
    parser = argparse.ArgumentParser(description="Save backtest results to database")
    parser.add_argument("--file", type=str, help="Path to trades CSV file")
    parser.add_argument("--strategy", type=str, default="unknown", 
                       help="Strategy name for this backtest")
    parser.add_argument("--side", type=str, choices=['buy', 'sell'],
                       help="Strategy side (buy/sell)")
    parser.add_argument("--stop-loss", type=float,
                       help="Stop loss value")
    parser.add_argument("--risk-reward", type=float,
                       help="Risk/reward ratio")
    parser.add_argument("--risk-per-trade", type=float,
                       help="Risk per trade as percentage (e.g., 0.01 for 1%%)")
    
    args = parser.parse_args()
    
    try:
        # Get the trades file
        if args.file:
            file_path = Path(args.file)
            if not file_path.exists():
                print(f"Error: File not found: {file_path}")
                return 1
        else:
            # Try to find latest file
            try:
                file_path = get_latest_trades_file()
            except FileNotFoundError as e:
                print(f"Error: {str(e)}")
                return 1
        
        print(f"\nUsing trades file: {file_path}")
        print(f"Strategy name: {args.strategy}")
        
        # Collect strategy parameters
        strategy_params = {}
        if args.side:
            strategy_params['side'] = args.side
        if args.stop_loss:
            strategy_params['stop_loss'] = args.stop_loss
        if args.risk_reward:
            strategy_params['risk_reward'] = args.risk_reward
        if args.risk_per_trade:
            strategy_params['risk_per_trade'] = args.risk_per_trade
            
        if strategy_params:
            print("\nStrategy parameters:")
            for key, value in strategy_params.items():
                print(f"  {key}: {value}")
        
        # Process the trades file
        trades_df, trades_summary_display, metrics = process_trades_file(
            file_path, 
            args.strategy,
            strategy_params
        )
        
        # Save to database
        print("\nSaving to database...")
        
        # First create a backtest run
        run_id = create_backtest_run(args.strategy, strategy_params, trades_df)
        
        # Then save executions
        execution_ids = save_executions(trades_df, run_id)
        
        # Finally save trades using trades_summary_display
        save_trades(trades_summary_display, run_id)
        
        print("\nDone!")
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 