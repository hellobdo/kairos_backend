import pandas as pd
from utils.db_utils import DatabaseManager
import json
import os
from pathlib import Path

# Initialize database manager
db_manager = DatabaseManager()

def insert_trades(df):
    """
    Insert processed backtest trades into the backtest_trades table.
    
    Args:
        df (pandas.DataFrame): Processed DataFrame with trade_id assignments
        
    Returns:
        int: Number of records inserted
    """
    if df.empty:
        return 0
    
    try:
        # Create a copy of the DataFrame and prepare for database insertion
        backtest_trades_df = pd.DataFrame({
            'trade_id': df['trade_id'],
            'num_executions': df['num_executions'],
            'symbol': df['symbol'],
            'start_date': df['start_date'],
            'start_time': df['start_time'],
            'end_date': df['end_date'],
            'end_time': df['end_time'],
            'duration_hours': df['duration_hours'],
            'quantity': df['quantity'],
            'entry_price': df['entry_price'],
            'stop_price': df['stop_price'],
            'exit_price': df['exit_price'],
            'capital_required': df['capital_required'],
            'exit_type': df['exit_type'],
            'take_profit_price': df['take_profit_price'],
            'risk_reward': df['risk_reward'],
            'is_winner': df['is_winner'],
            'perc_return': df['perc_return'],
            'week': df['week'],
            'month': df['month'],
            'year': df['year'],
            'run_id': df['run_id'],
            'risk_per_trade_perc': df['risk_per_trade_perc'],
            'day': df['day'],
            'commission': df['commission'],
            'direction': df['direction'],
            'status': df['status']
        })
        
        # Insert the DataFrame into the database
        records_inserted = db_manager.insert_dataframe(backtest_trades_df, 'backtest_trades')
        
        print(f"Successfully inserted {records_inserted} records into backtest_trades table")
        return records_inserted
        
    except Exception as e:
        print(f"Error inserting backtest trades into database: {e}")
        raise

def insert_executions(df):
    """
    Insert processed backtest executions into the backtest_executions table.
    
    Args:
        df (pandas.DataFrame): Processed DataFrame with trade_id assignments
        
    Returns:
        int: Number of records inserted
    """
    if df.empty:
        return 0
    
    try:
        # Create a copy of the DataFrame and prepare for database insertion
        commission = 0  # Define commission once
        backtest_executions_df = pd.DataFrame({
            'execution_timestamp': df['execution_timestamp'],
            'date': df['date'],
            'time_of_day': df['time_of_day'],
            'order_id': df['order_id'],
            'symbol': df['symbol'],
            'side': df['side'],
            'quantity': df['quantity'],
            'price': df['price'],
            'trade_id': df['trade_id'],
            'run_id': df['run_id'],
            'is_entry': df['is_entry'].astype(int),
            'is_exit': df['is_exit'].astype(int),
            'commission': commission,
            'order_type': df['type'],
            'net_cash_with_billable': df['quantity'] * df['price'] + commission
        })
        
        # Insert the DataFrame into the database
        records_inserted = db_manager.insert_dataframe(backtest_executions_df, 'backtest_executions')
        
        print(f"Successfully inserted {records_inserted} records into backtest_executions table")
        return records_inserted
        
    except Exception as e:
        print(f"Error inserting backtest executions into database: {e}")
        raise

def get_backtest_info():
    """
    Create backtest info from a JSON settings file.
    
    Args:
        json_path (str): Path to the JSON settings file (format: Strategy_YYYY-MM-DD_HH-MM_XXXXX_settings.json)
        
    Returns:
        dict: Dictionary containing the required fields from parameters
              - backtesting_start
              - backtesting_end
              - indicators
              - symbols
              - side
              - stop_loss_rules
              - risk_reward
              - risk_per_trade
              - out_before_end_of_day
              - backtest_name (extracted from filename)
              - margin
              - day_trading
              - sleeptime
    """
    settings_file = get_latest_settings_file()
    if not settings_file:
        print("Error: Could not find settings file")
        return False
    
    try:
        # Extract backtest name from the json path
        # From: path/to/Strategy_2025-03-24_15-41_E6mMj9_settings.json
        # Get: Strategy_2025-03-24_15-41_E6mMj9
        filename = os.path.basename(settings_file)  # Get just the filename
        source_file = filename.replace('_settings.json', '')
        
        with open(settings_file, 'r') as f:
            settings = json.load(f)
            
        # Extract parameters
        params = settings.get('parameters', {})
        
        # Get required fields
        df = pd.DataFrame([{
            'backtesting_start': params.get('backtesting_start'),
            'backtesting_end': params.get('backtesting_end'),
            'indicators': json.dumps(params.get('indicators', [])),  # Convert list to JSON string
            'symbols_traded': json.dumps(params.get('symbols', [])),  # Convert list to JSON string
            'direction': params.get('side'),
            'stop_loss': json.dumps(params.get('stop_loss_rules', [])),  # Convert list to JSON string
            'risk_reward': params.get('risk_reward'),
            'risk_per_trade': params.get('risk_per_trade'),
            'source_file': source_file,
            'bar_signals_length': params.get('bar_signals_length'),
            'margin': params.get('margin'),
            'sleeptime': params.get('sleeptime'),
        }])

        return df
        
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None

def insert_backtest_info(df):
    """
    Insert backtest info into the backtest_runs table.
    
    Args:
        df (pd.DataFrame): DataFrame containing the backtest info
        
    Returns:
        int: run_id of the inserted backtest info
    """

    try:
        inserted = db_manager.insert_dataframe(df, 'backtest_runs')
        if inserted > 0:  # Check if any records were actually inserted
            run_id = db_manager.get_max_id("backtest_runs","run_id")
            return run_id
    except Exception as e:
        print(f"Error saving backtest info: {e}")
        return None
    
def insert_to_db(executions_df, trades_df):
    """
    Insert the processed DataFrame into the database.
    
    Args:
        df (pd.DataFrame): DataFrame containing the backtest executions
        json_path (str): Path to the JSON settings file
        
    Returns:
        bool: True if successful, False otherwise
    """
    
    try:
        # First save backtest info and get run_id
        settings_df = get_backtest_info()
        run_id = insert_backtest_info(settings_df)
        if run_id is None:
            print("Error: Could not save backtest info")
            return False
            
        # Add run_id to DataFrame
        executions_df['run_id'] = run_id
        trades_df['run_id'] = run_id
        
        # Insert executions using the dedicated function
        executions_inserted = insert_executions(executions_df)
        trades_inserted = insert_trades(trades_df)
        
        return executions_inserted > 0 and trades_inserted > 0
        
    except Exception as e:
        print(f"Error inserting data into database: {str(e)}")
        return False
    
def get_latest_settings_file():
    """
    Find the most recently created settings.json file in the logs directory.
    
    Returns:
        str: settings_file path. None if not found.
    """
    try:
        logs_dir = Path('logs')
        if not logs_dir.exists():
            print("Logs directory not found")
            return None
            
        # Find all matching files and their creation times
        settings_files = [(f, f.stat().st_ctime) for f in logs_dir.glob('*_settings.json')]
        latest_settings = max(settings_files, key=lambda x: x[1])[0] if settings_files else None

        return latest_settings
    
    except Exception as e:
        print(f"Error finding latest settings file: {str(e)}")
        return None