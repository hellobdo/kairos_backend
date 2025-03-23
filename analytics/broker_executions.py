import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from api.ibkr import get_ibkr_report
from utils.db_utils import DatabaseManager

# Initialize database manager
db = DatabaseManager()

def process_ibkr_data(df):
    """
    Process raw IBKR DataFrame to add derived fields, excluding executions already in DB.
    
    Args:
        df (pandas.DataFrame): Raw DataFrame from get_ibkr_flex_data
        
    Returns:
        pandas.DataFrame: Processed DataFrame with derived fields
    """
    # Make a copy to avoid modifying the original
    processed_df = df.copy()
    
    # Filter out executions already in the database
    try:
        existing_executions = db.get_existing_trade_external_ids()
        new_executions = processed_df[~processed_df['trade_external_ID'].isin(existing_executions)]
        
        if len(new_executions) < len(processed_df):
            print(f"Filtered out {len(processed_df) - len(new_executions)} executions already in database")
        processed_df = new_executions
        
    except Exception as e:
        print(f"Warning: Could not check existing executions in database: {e}")
    
    if processed_df.empty:
        print("No new executions to process")
        return processed_df
    
    # Convert numeric fields
    numeric_fields = ['Quantity', 'Price', 'NetCashWithBillable', 'Commission']
    for field in numeric_fields:
        if field in processed_df.columns:
            processed_df[field] = pd.to_numeric(processed_df[field], errors='coerce')
    
    # Process date and time fields
    if 'Date/Time' in processed_df.columns:
        # Add execution_timestamp (rename of Date/Time)
        processed_df['execution_timestamp'] = processed_df['Date/Time']
        
        # Example format for debugging if available
        if not processed_df.empty:
            print(f"Example Date/Time format: {processed_df['Date/Time'].iloc[0]}")
        
        try:
            # Extract date and time components (format: YYYY-MM-DD;HH:MM:SS)
            processed_df[['date', 'time_of_day']] = processed_df['Date/Time'].str.split(';', n=1, expand=True)
            print("Successfully extracted date and time components")
        except Exception as e:
            print(f"Error splitting Date/Time: {e}")
            processed_df['date'] = processed_df['Date/Time']
            processed_df['time_of_day'] = ''
    
    # Determine trade side from quantity
    if 'Quantity' in processed_df.columns:
        processed_df['side'] = processed_df['Quantity'].apply(
            lambda x: 'BUY' if pd.to_numeric(x, errors='coerce') > 0 else 'SELL'
        )
    
    # Check for execution_timestamp and sort
    if 'execution_timestamp' not in processed_df.columns:
        print("ERROR: 'execution_timestamp' field is missing")
        return pd.DataFrame()
    
    # Sort the data by execution_timestamp
    return processed_df.sort_values(by='execution_timestamp')

def identify_trade_ids(df):
    """
    Assign trade_id based on open positions per symbol.
    
    Args:
        df (pandas.DataFrame): Processed DataFrame from process_ibkr_data
        
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
    
    try:
        # Get current state from database
        current_trade_id = db.get_max_trade_id()
        existing_positions = db.get_open_positions()
        
        # Initialize positions from database state
        open_positions = {pos[0]: pos[1] for pos in existing_positions}  # {symbol: current_volume}
        position_trade_ids = {pos[0]: pos[2] for pos in existing_positions}  # {symbol: current_trade_id}
        
        print("\nExisting open positions loaded from database:")
        for symbol, qty in open_positions.items():
            print(f"  {symbol}: {qty} (Trade ID: {position_trade_ids[symbol]})")
            
    except Exception as e:
        print(f"Warning: Could not get state from database: {e}")
        current_trade_id = 0
        open_positions = {}
        position_trade_ids = {}
    
    # Process each execution
    for idx, row in trades_df.iterrows():
        symbol = row['Symbol']
        quantity = row['Quantity']
        
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

def insert_executions_to_db(df):
    """
    Insert processed IBKR data into the executions table.
    
    Args:
        df (pandas.DataFrame): Processed DataFrame with trade_id assignments
        
    Returns:
        int: Number of records inserted
    """
    records_inserted = 0
    
    try:
        for _, row in df.iterrows():
            # Convert boolean fields to integers for database storage
            is_entry_int = 1 if row.get('is_entry', False) else 0
            is_exit_int = 1 if row.get('is_exit', False) else 0
            
            # Prepare execution data as a tuple
            execution_data = (
                row.get('ClientAccountID', ''),
                row.get('TradeID', ''),
                row.get('OrderID', ''),
                row.get('Symbol', ''),
                row.get('Quantity', 0),
                row.get('Price', 0),
                row.get('NetCashWithBillable', 0),
                row.get('execution_timestamp', ''),
                row.get('Commission', 0),
                row.get('date', ''),
                row.get('time_of_day', ''),
                row.get('side', ''),
                row.get('trade_id'),
                is_entry_int,
                is_exit_int
            )
            
            # Insert the execution
            db.insert_execution(execution_data)
            records_inserted += 1
        
        print(f"Successfully inserted {records_inserted} records into executions table")
        
    except Exception as e:
        print(f"Error inserting data into database: {e}")
        raise
    
    return records_inserted

def process_account_data(token, query_id, account_type="paper"):
    """
    Process IBKR data for a specific account and insert into database.
    
    Args:
        token (str): IBKR Flex Web Service token
        query_id (str): IBKR Flex query ID
        account_type (str): Type of account (paper or live)
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\nProcessing {account_type} trading account:")
    
    try:
        # Get trade confirmations using the centralized function
        df_raw = get_ibkr_report(token, query_id, "trade_confirmations")
        
        if df_raw is False:
            print(f"Failed to retrieve trade confirmations for {account_type} account")
            return False
        
        print(f"Trade confirmations retrieved from IBKR for {account_type} account")
        print("\nDataFrame columns:", df_raw.columns.tolist())
        
        # Process the data through our pipeline
        df_processed = process_ibkr_data(df_raw)
        
        if df_processed.empty:
            print(f"No new executions to process for {account_type} account")
            return False
            
        df_executions_with_trade_ids = identify_trade_ids(df_processed)
        
        # Insert into database
        inserted = insert_executions_to_db(df_executions_with_trade_ids)
        
        if inserted:
            print(f"Updated database with {inserted} new executions for {account_type} account")
            return True
        else:
            print(f"No new executions inserted for {account_type} account")
            return False
            
    except Exception as e:
        print(f"Error processing {account_type} account: {e}")
        return False

# If running the script directly, use environment variables
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Process paper trading account
    process_account_data(
        os.getenv("IBKR_TOKEN_PAPER"),
        os.getenv("IBKR_QUERY_ID_TRADE_CONFIRMATION_PAPER"),
        "paper"
    )
    
    # Uncomment to process live account
    # process_account_data(
    #     os.getenv("IBKR_TOKEN_LIVE"),
    #     os.getenv("IBKR_QUERY_ID_TRADE_CONFIRMATION_LIVE"),
    #     "live"
    # )