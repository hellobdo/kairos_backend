import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from api.ibkr import get_ibkr_report
from utils.db_utils import DatabaseManager
from utils.pandas_utils import convert_to_numeric
from utils.process_executions_utils import process_datetime_fields,identify_trade_ids

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
    numeric_fields = ['quantity', 'price', 'netcashwithbillable', 'commission']
    processed_df = convert_to_numeric(processed_df, numeric_fields)
    
    # Process date and time fields using the utility function
    # This also validates execution_timestamp and sorts the DataFrame
    processed_df = process_datetime_fields(processed_df, 'date/time')
    
    # If process_datetime_fields returned an empty DataFrame, return it
    if processed_df.empty:
        return processed_df
    
    # Determine trade side from quantity
    if 'quantity' in processed_df.columns:
        processed_df['side'] = processed_df['quantity'].apply(
            lambda x: 'buy' if pd.to_numeric(x, errors='coerce') > 0 else 'sell'
        )
    
    return processed_df

def insert_executions_to_db(df):
    """
    Insert processed IBKR data into the executions table.
    
    Args:
        df (pandas.DataFrame): Processed DataFrame with trade_id assignments
        
    Returns:
        int: Number of records inserted
    """
    if df.empty:
        return 0
    
    try:
        # Create a copy of the DataFrame and prepare for database insertion
        executions_df = pd.DataFrame({
            'account_id': df['clientaccountid'],
            'execution_external_id': df['tradeid'],
            'order_id': df['orderid'],
            'symbol': df['symbol'],
            'quantity': df['quantity'],
            'price': df['price'],
            'net_cash_with_billable': df['netcashwithbillable'],
            'execution_timestamp': df['execution_timestamp'],
            'commission': df['commission'],
            'date': df['date'],
            'time_of_day': df['time_of_day'],
            'side': df['side'],
            'trade_id': df['trade_id'],
            'is_entry': df['is_entry'].astype(int),
            'is_exit': df['is_exit'].astype(int),
            'order_type': df['ordertype']
        })
        
        # Insert the DataFrame into the database
        records_inserted = db.insert_dataframe(executions_df, 'executions')
        
        print(f"Successfully inserted {records_inserted} records into executions table")
        return records_inserted
        
    except Exception as e:
        print(f"Error inserting executions into database: {e}")
        raise

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