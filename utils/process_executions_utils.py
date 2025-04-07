import pandas as pd
import re
from utils.db_utils import DatabaseManager

# Initialize database manager
db = DatabaseManager()

def process_datetime_fields(df, datetime_column):
    """
    Process date and time fields from a DataFrame column.
    Validates execution_timestamp presence and sorts the DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame containing a datetime column
        datetime_column (str): Name of the datetime column to process
        
    Returns:
        pandas.DataFrame: DataFrame with processed date and time columns, sorted by execution_timestamp.
                         Returns empty DataFrame if execution_timestamp validation fails.
    """
    # Make a copy to avoid modifying the original
    processed_df = df.copy()
    
    # Check if the specified column exists
    if datetime_column in processed_df.columns:
        # Add execution_timestamp (rename of Date/Time) from a slice of the datetime column
        processed_df['execution_timestamp'] = processed_df[datetime_column].astype(str).str[0:19]
        
        # First detect which delimiter is used (semicolon or space)
        # Try to split with semicolon first
        if any(processed_df[datetime_column].astype(str).str.contains(';')):
            delimiter = ';'
        else:
            # If no semicolons found, use space as delimiter
            delimiter = ' '
        
        # Extract date and time components using the detected delimiter
        processed_df[['date', 'time_of_day']] = processed_df['execution_timestamp'].str.split(delimiter, n=1, expand=True)
    
    else:
        print(f"Warning: Column '{datetime_column}' not found in DataFrame")
    
    # Check for execution_timestamp and sort
    if 'execution_timestamp' not in processed_df.columns:
        print("ERROR: 'execution_timestamp' field is missing")
        return pd.DataFrame()
    
    # Sort the data by execution_timestamp and reset the index
    return processed_df.sort_values(by='execution_timestamp').reset_index(drop=True)


def identify_trade_ids(df, db_validation=True):
    """
    Assign trade_id based on open positions per symbol.
    
    Args:
        df (pandas.DataFrame): Processed DataFrame from process_ibkr_data
        db_validation (bool): Whether to validate against the database (default: True)
    Returns:
        pandas.DataFrame: DataFrame with trade_id and position tracking fields added
    """
    # Make a copy to avoid modifying the original
    trades_df = df.copy()
    
    # Initialize new columns
    trades_df['trade_id'] = None
    trades_df['open_volume'] = 0
    trades_df['is_entry'] = False
    trades_df['is_exit'] = False
    
    if db_validation:
        # Get current state from database
        current_trade_id = db.get_max_id("executions","trade_id")
        # Handle None case by defaulting to 0
        if current_trade_id is None:
            current_trade_id = 0
            
        existing_positions = db.get_open_positions()
        # Handle None case by defaulting to empty list
        if existing_positions is None:
            existing_positions = []
        
        # Initialize positions from database state
        open_positions = {pos[0]: pos[1] for pos in existing_positions}  # {symbol: current_volume}
        position_trade_ids = {pos[0]: pos[2] for pos in existing_positions}  # {symbol: current_trade_id}
    else:
        # For backtesting or when db validation is not needed
        current_trade_id = 0
        open_positions = {}
        position_trade_ids = {}
    
    # Process each execution
    for idx, row in trades_df.iterrows():
        symbol = row['symbol']
        quantity = row['quantity']
        
        # Initialize symbol tracking if not exists
        if symbol not in open_positions:
            open_positions[symbol] = 0
            position_trade_ids[symbol] = None
        
        # Get previous position value
        prev_position = open_positions[symbol]
        
        # Update position
        open_positions[symbol] += quantity
        
        # Check if we're starting a new position (entry)
        if prev_position == 0 and open_positions[symbol] != 0:
            current_trade_id += 1
            position_trade_ids[symbol] = current_trade_id
            trades_df.at[idx, 'is_entry'] = True
        
        # Assign trade_id from the current position's trade
        trades_df.at[idx, 'trade_id'] = position_trade_ids[symbol]
        trades_df.at[idx, 'open_volume'] = open_positions[symbol]
        
        # Check if position was closed (exit)
        if prev_position != 0 and open_positions[symbol] == 0:
            trades_df.at[idx, 'is_exit'] = True
            position_trade_ids[symbol] = None
    
    return trades_df