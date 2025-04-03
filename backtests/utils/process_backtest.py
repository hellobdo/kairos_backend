import pandas as pd
from utils.db_utils import DatabaseManager
import json
import os

# Initialize database manager
db_manager = DatabaseManager()

def insert_executions_to_db(df):
    """
    Insert processed backtest data into the backtest_executions table.
    
    Args:
        df (pandas.DataFrame): Processed DataFrame with trade_id assignments
        
    Returns:
        int: Number of records inserted
    """
    if df.empty:
        return 0
    
    try:
        # Create a copy of the DataFrame and prepare for database insertion
        backtest_executions_df = pd.DataFrame({
            'execution_timestamp': df['execution_timestamp'],
            'identifier': df['order_id'],
            'symbol': df['symbol'],
            'side': df['side'],
            'type': df['order_type'],
            'price': df['price'],
            'quantity': df['quantity'],
            'trade_cost': df['commission'],
            'date': df['date'],
            'time_of_day': df['time_of_day'],
            'trade_id': df['trade_id'],
            'is_entry': df['is_entry'].astype(int),
            'is_exit': df['is_exit'].astype(int),
            'net_cash_with_billable': df['quantity'] * df['price'] + df['commission'],
            'run_id': df['run_id']
        })
        
        # Insert the DataFrame into the database
        records_inserted = db_manager.insert_dataframe(backtest_executions_df, 'backtest_executions')
        
        print(f"Successfully inserted {records_inserted} records into backtest_executions table")
        return records_inserted
        
    except Exception as e:
        print(f"Error inserting backtest executions into database: {e}")
        raise

def create_backtest_info(json_path):
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
              - backtest_name (extracted from filename)
    """
    try:
        # Extract backtest name from the json path
        # From: path/to/Strategy_2025-03-24_15-41_E6mMj9_settings.json
        # Get: Strategy_2025-03-24_15-41_E6mMj9
        filename = os.path.basename(json_path)  # Get just the filename
        backtest_name = filename.replace('_settings.json', '')
        
        with open(json_path, 'r') as f:
            settings = json.load(f)
            
        # Extract parameters
        params = settings.get('parameters', {})
        
        # Get required fields
        backtest_info = {
            'backtesting_start': params.get('backtesting_start'),
            'backtesting_end': params.get('backtesting_end'),
            'indicators': json.dumps(params.get('indicators', [])),  # Convert list to JSON string
            'symbols_traded': json.dumps(params.get('symbols', [])),  # Convert list to JSON string
            'direction': params.get('side'),
            'stop_loss': json.dumps(params.get('stop_loss_rules', [])),  # Convert list to JSON string
            'risk_reward': params.get('risk_reward'),
            'risk_per_trade': params.get('risk_per_trade'),
            'backtest_name': backtest_name
        }
        
        return backtest_info
        
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None
    
def save_backtest_info(json_path):
    """
    Save backtest information to the database.
    
    Args:
        json_path (str): Path to the JSON settings file
        
    Returns:
        int: The ID of the inserted backtest run, or None if there was an error
    """
    try:
        # First get the data from the JSON file
        backtest_info = create_backtest_info(json_path)
        if not backtest_info:
            print(f"Error: Could not extract backtest info from {json_path}")
            return None
        
        # Save to database and get run_id
        run_id = db_manager.save_to_backtest_runs(backtest_info)
        
        return run_id
        
    except Exception as e:
        print(f"Error saving backtest info: {str(e)}")
        return None

def insert_to_db(df, json_path):
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
        run_id = save_backtest_info(json_path)
        if run_id is None:
            print("Error: Could not save backtest info")
            return False
            
        # Add run_id to DataFrame
        df['run_id'] = run_id
        
        # Insert executions using the dedicated function
        records_inserted = insert_executions_to_db(df)
        
        return records_inserted > 0
        
    except Exception as e:
        print(f"Error inserting data into database: {str(e)}")
        return False
