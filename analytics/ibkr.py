import requests
import xml.etree.ElementTree as ET
import os
import time
import pandas as pd
from dotenv import load_dotenv
import sqlite3

def get_ibkr_flex_data(token, query_id, flex_version=3):
    """
    Fetch IBKR Flex report data and return it as a DataFrame.
    
    Args:
        token (str): IBKR Flex Web Service token
        query_id (str): IBKR Flex query ID
        flex_version (int, optional): Flex API version. Defaults to 3.
    
    Returns:
        pandas.DataFrame: DataFrame containing the raw XML data with no transformations
    """
    request_base = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
    
    # Step 1: Request report generation
    send_slug = "/SendRequest"
    send_params = {
        "t": token, 
        "q": query_id, 
        "v": flex_version
    }
    
    print(f"Requesting report generation with query ID: {query_id}...")
    flex_req = requests.get(url=request_base+send_slug, params=send_params)
    
    # Parse the response
    tree = ET.ElementTree(ET.fromstring(flex_req.text))
    root = tree.getroot()
    
    # Check response status and get reference code
    ref_code = None
    for child in root:
        if child.tag == "Status":
            if child.text != "Success":
                print(f"Failed to generate Flex statement: {flex_req.text}")
                return pd.DataFrame()  # Return empty DataFrame on failure
        elif child.tag == "ReferenceCode":
            ref_code = child.text
    
    if not ref_code:
        print("No reference code found in response.")
        return pd.DataFrame()
    
    print(f"Report generation request successful. Reference code: {ref_code}")
    print("Waiting for report to be generated (20 seconds)...")
    time.sleep(20)
    
    # Step 2: Retrieve the report
    receive_slug = "/GetStatement"
    receive_params = {
        "t": token, 
        "q": ref_code, 
        "v": flex_version
    }
    
    print("Retrieving report...")
    receive_req = requests.get(url=request_base+receive_slug, params=receive_params, allow_redirects=True)
    
    print(f"Response status code: {receive_req.status_code}")
    
    # Parse the XML response
    try:
        tree = ET.ElementTree(ET.fromstring(receive_req.text))
        root = tree.getroot()
        
        # Find all TradeConfirm elements in the XML
        trade_confirms = root.findall('.//TradeConfirm')
        print(f"Found {len(trade_confirms)} trade confirmations to process")
        
        # Create a list to store all trade data
        trades_data = []
        
        # Extract all attributes from each trade confirmation without transformations
        for trade in trade_confirms:
            # Get all attributes from the XML element directly
            trade_data = {key: val for key, val in trade.attrib.items()}
            trades_data.append(trade_data)
        
        # Convert to DataFrame
        df = pd.DataFrame(trades_data)
        
        return df
    
    except Exception as e:
        print(f"Error parsing XML response: {e}")
        return pd.DataFrame()

def process_ibkr_data(df):
    """
    Process raw IBKR DataFrame to add derived fields.
    
    Args:
        df (pandas.DataFrame): Raw DataFrame from get_ibkr_flex_data
        
    Returns:
        pandas.DataFrame: Processed DataFrame with derived fields
    """
    if df.empty:
        return df
    
    # Make a copy to avoid modifying the original
    processed_df = df.copy()
    
    # Convert numeric fields
    numeric_fields = ['quantity', 'price', 'netCashWithBillable', 'commission']
    for field in numeric_fields:
        if field in processed_df.columns:
            processed_df[field] = pd.to_numeric(processed_df[field], errors='coerce')
    
    # Handle dateTime field - print a sample first to debug
    if 'dateTime' in processed_df.columns:
        # Print dateTime format for debugging
        if not processed_df.empty:
            print(f"\nSample dateTime format: '{processed_df['dateTime'].iloc[0]}'")
        
        # Add execution_timestamp (rename of dateTime)
        processed_df['execution_timestamp'] = processed_df['dateTime']
        
        # Split dateTime into date and time_of_day at the semicolon
        processed_df[['date', 'time_of_day']] = processed_df['dateTime'].str.split(';', expand=True)
    
    # Determine trade side from quantity
    if 'quantity' in processed_df.columns:
        processed_df['side'] = processed_df['quantity'].apply(
            lambda x: 'BUY' if pd.to_numeric(x, errors='coerce') > 0 else 'SELL'
        )
    
    # Check for execution_timestamp and sort the data
    if 'execution_timestamp' not in processed_df.columns:
        raise ValueError("Critical error: 'execution_timestamp' field is missing from the data")
    
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
    
    # Track positions and trade IDs by symbol
    current_trade_id = 0
    open_positions = {}  # {symbol: current_volume}
    position_trade_ids = {}  # {symbol: current_trade_id}
    
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
                    date, time_of_day, side, trade_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('accountId', ''),
                row.get('tradeID', ''),  # Keep using tradeID from IBKR data but map to trade_external_ID
                row.get('orderID', ''),
                row.get('symbol', ''),
                row.get('quantity', 0),
                row.get('price', 0),
                row.get('netCashWithBillable', 0),
                row.get('execution_timestamp', ''),
                row.get('commission', 0),
                row.get('date', ''),
                row.get('time_of_day', ''),
                row.get('side', ''),
                row.get('trade_id')
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
    
    # Get raw data
    df_raw = get_ibkr_flex_data(token, query_id)
    
    if not df_raw.empty:
        print(f"Raw data retrieved from IBKR.")
        
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
        print(f"No data retrieved from IBKR.")
        return False

# If running the script directly, use environment variables
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Process paper trading account
    token_paper = os.getenv("IBKR_TOKEN_PAPER")
    query_id_paper = os.getenv("IBKR_QUERY_ID_TRADE_CONFIRMATION_PAPER")
    process_ibkr_account(token_paper, query_id_paper)