import os
import pandas as pd
from dotenv import load_dotenv
import sqlite3
from analytics.ibkr_api import get_ibkr_flex_data

def get_trade_confirms(token, query_id):
    """
    Fetch trade confirmations from IBKR Flex report.
    
    Args:
        token (str): IBKR Flex Web Service token
        query_id (str): IBKR Flex query ID
        
    Returns:
        pandas.DataFrame: DataFrame containing trade confirmations
    """
    # Get raw CSV data with trade confirmations path
    df = get_ibkr_flex_data(token, query_id)
    
    if df is False:
        print("No trade confirmations found")
        return False
    
    print(f"Retrieved {len(df)} trade confirmations")
    return df

def process_ibkr_data(df):
    """
    Process raw IBKR DataFrame to add derived fields.
    
    Args:
        df (pandas.DataFrame): Raw DataFrame from get_ibkr_flex_data
        
    Returns:
        pandas.DataFrame: Processed DataFrame with derived fields, excluding executions already in the database
    """
    if df.empty:
        return df
    
    # Make a copy to avoid modifying the original
    processed_df = df.copy()
    
    # Get list of existing trade_external_IDs from database
    try:
        conn = sqlite3.connect('data/kairos.db')
        cursor = conn.cursor()
        cursor.execute("SELECT trade_external_ID FROM executions")
        existing_trades = {row[0] for row in cursor.fetchall()}
        conn.close()
        
        # Filter out executions that are already in the database
        new_trades = processed_df[~processed_df['TradeID'].isin(existing_trades)]
        if len(new_trades) < len(processed_df):
            print(f"Filtered out {len(processed_df) - len(new_trades)} executions that are already in the database")
        processed_df = new_trades
        
    except Exception as e:
        print(f"Warning: Could not check existing trades in database: {e}")
    
    if processed_df.empty:
        print("No new executions to process")
        return processed_df
    
    # Convert numeric fields
    numeric_fields = ['Quantity', 'Price', 'NetCashWithBillable', 'Commission']
    for field in numeric_fields:
        if field in processed_df.columns:
            processed_df[field] = pd.to_numeric(processed_df[field], errors='coerce')
    
    # Handle Date/Time field
    if 'Date/Time' in processed_df.columns:
        # Add execution_timestamp (rename of Date/Time)
        processed_df['execution_timestamp'] = processed_df['Date/Time']
        
        # Print an example for debugging
        if not processed_df.empty:
            print(f"Example Date/Time format: {processed_df['Date/Time'].iloc[0]}")
        
        # Extract date (just the date part) and time_of_day properly
        try:
            # For IBKR format which is typically YYYY-MM-DD;HH:MM:SS
            # Extract just the date part (before the semicolon)
            processed_df['date'] = processed_df['Date/Time'].str.split(';', n=1).str[0]
            
            # Extract the time part (after the semicolon)
            processed_df['time_of_day'] = processed_df['Date/Time'].str.split(';', n=1).str[1]
            
            print("Successfully extracted date and time components")
            if not processed_df.empty:
                print(f"Example extracted date: {processed_df['date'].iloc[0]}")
                print(f"Example extracted time: {processed_df['time_of_day'].iloc[0]}")
        except Exception as e:
            # Fallback if splitting fails
            print(f"Error splitting Date/Time: {e}")
            processed_df['date'] = processed_df['Date/Time']
            processed_df['time_of_day'] = ''
    
    # Determine trade side from quantity
    if 'Quantity' in processed_df.columns:
        processed_df['side'] = processed_df['Quantity'].apply(
            lambda x: 'BUY' if pd.to_numeric(x, errors='coerce') > 0 else 'SELL'
        )
    
    # Check for execution_timestamp and sort the data
    if 'execution_timestamp' not in processed_df.columns:
        print("ERROR: 'execution_timestamp' field is missing from the data")
        print("Available columns:", processed_df.columns.tolist())
        return pd.DataFrame()  # Return empty DataFrame instead of raising exception
    
    # Sort the data by execution_timestamp
    processed_df = processed_df.sort_values(by='execution_timestamp')
    print("Data sorted by execution_timestamp")
    
    return processed_df

def identify_trade_ids(df):
    """
    Assign trade_id based on open positions per symbol.
    IBKR convention: 
    - Buy orders have positive quantity
    - Sell/short orders have negative quantity
    
    Args:
        df (pandas.DataFrame): Processed DataFrame from process_ibkr_data
        
    Returns:
        pandas.DataFrame: DataFrame with trade_id and open_volume assigned
    """
    if df.empty:
        return df
    
    # Make a copy to avoid modifying the original
    trades_df = df.copy()
    
    # Initialize new columns
    trades_df['trade_id'] = None
    trades_df['open_volume'] = 0
    trades_df['is_entry'] = False
    trades_df['is_exit'] = False
    
    try:
        conn = sqlite3.connect('data/kairos.db')
        cursor = conn.cursor()
        
        # Get the latest trade_id from the database
        cursor.execute("SELECT MAX(trade_id) FROM executions")
        result = cursor.fetchone()
        current_trade_id = result[0] if result[0] is not None else 0
        
        # Get current open positions from the database
        # We need to sum quantities for each symbol and get the latest trade_id for open positions
        cursor.execute("""
            WITH position_sums AS (
                SELECT 
                    symbol,
                    SUM(quantity) as total_quantity,
                    MAX(trade_id) as latest_trade_id
                FROM executions
                GROUP BY symbol
                HAVING SUM(quantity) != 0
            )
            SELECT symbol, total_quantity, latest_trade_id
            FROM position_sums
        """)
        existing_positions = cursor.fetchall()
        conn.close()
        
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
        df (pandas.DataFrame): Processed DataFrame from process_ibkr_data
        
    Returns:
        int: Number of records inserted
    """
    if df.empty:
        print("No data to insert")
        return 0
    
    # Connect to the database
    conn = sqlite3.connect('data/kairos.db')
    cursor = conn.cursor()
    
    # Insert each row into the executions table
    records_inserted = 0
    
    try:
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO executions (
                    accountId, trade_external_ID, orderID, symbol, quantity, 
                    price, netCashWithBillable, execution_timestamp, commission,
                    date, time_of_day, side, trade_id, is_entry, is_exit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('ClientAccountID', ''),
                row.get('TradeID', ''),  # Keep using TradeID from IBKR data but map to trade_external_ID
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
                1 if row.get('is_entry', False) else 0,  # Convert boolean to integer
                1 if row.get('is_exit', False) else 0    # Convert boolean to integer
            ))
            records_inserted += 1
        
        # Commit the changes
        conn.commit()
        print(f"Successfully inserted {records_inserted} records into executions table")
        
    except Exception as e:
        print(f"Error inserting data into database: {e}")
        conn.rollback()
    
    finally:
        conn.close()
    
    return records_inserted

def process_ibkr_account(token, query_id):
    """
    Process IBKR data for a specific account and insert into database.
    
    Args:
        token (str): IBKR Flex Web Service token
        query_id (str): IBKR Flex query ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    
    # Get trade confirmations
    df_raw = get_trade_confirms(token, query_id)
    
    # Break the process if df_raw is False
    if df_raw is False:
        print("Breaking process - no data retrieved from IBKR")
        return False
    
    if not df_raw.empty:
        print("Trade confirmations retrieved from IBKR.")
        
        # Print column names for debugging
        print("\nDataFrame columns:")
        print(df_raw.columns.tolist())
        
        # Process the data
        df_processed = process_ibkr_data(df_raw)
        
        # Identify trades
        df_with_trades = identify_trade_ids(df_processed)
        
        # Insert into database
        if insert_executions_to_db(df_with_trades):
            print(f"Data successfully inserted into database")
            return True
        else:
            print(f"Failed to insert data into database")
            return False
    else:
        print(f"No trade confirmations retrieved from IBKR.")
        return False

# If running the script directly, use environment variables
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Process paper trading account
    token_paper = os.getenv("IBKR_TOKEN_PAPER")
    query_id_paper = os.getenv("IBKR_QUERY_ID_TRADE_CONFIRMATION_PAPER")
    token_live = os.getenv("IBKR_TOKEN_LIVE")
    query_id_live = os.getenv("IBKR_QUERY_ID_TRADE_CONFIRMATION_LIVE")
    process_ibkr_account(token_paper, query_id_paper)
    #process_ibkr_account(token_live, query_id_live)