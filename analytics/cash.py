import os
from dotenv import load_dotenv
import pandas as pd
import sqlite3
from datetime import datetime
import re

# Handle import differently when run as script vs module
try:
    # When imported as part of a package
    from .ibkr_api import get_ibkr_flex_data
except ImportError:
    # When run directly as a script
    from analytics.ibkr_api import get_ibkr_flex_data

def update_accounts_balances(df):
    """
    Update accounts_balances table with cash report data.
    Ensures only one entry per account_ID + date combination.
    
    Args:
        df (pandas.DataFrame): Cash report data from IBKR
        
    Raises:
        ValueError: If date is not in YYYY-MM-DD format
    """
    # Skip if DataFrame is empty or missing required columns
    if df.empty or not all(col in df.columns for col in ['ClientAccountID', 'EndingCash', 'ToDate']):
        return
    
    conn = sqlite3.connect('data/kairos.db')
    inserted_count = 0
    
    try:
        # Get the mapping from account_external_ID to ID
        account_map_df = pd.read_sql("SELECT ID, account_external_ID FROM accounts", conn)
        
        # Current timestamp for record_date
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Process each valid row
        cash_data = []
        for _, row in df.iterrows():
            # Skip rows with missing data
            if not (row.get('ClientAccountID') and row.get('EndingCash')):
                continue
                
            # Find matching account ID
            matching_accounts = account_map_df[account_map_df['account_external_ID'] == str(row['ClientAccountID'])]
            if matching_accounts.empty:
                continue
                
            # Extract and validate data
            db_account_id = int(matching_accounts['ID'].iloc[0])
            cash_balance = float(row['EndingCash'])
            date_value = str(row['ToDate'])[:10]  # Get only YYYY-MM-DD part
            
            # Validate date format
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_value):
                raise ValueError(f"Date must be in YYYY-MM-DD format. Got: {row['ToDate']}, extracted: {date_value}")
            
            # Check if record already exists
            existing = pd.read_sql(
                "SELECT 1 FROM accounts_balances WHERE account_ID = ? AND date = ?",
                conn, params=[db_account_id, date_value]
            )
            if not existing.empty:
                print(f"Account ID {db_account_id} with date {date_value} already exists in database - skipping")
                continue
            
            # Add to cash data for batch insert
            cash_data.append((db_account_id, date_value, cash_balance, current_timestamp))
        
        # Batch insert all records
        if cash_data:
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT INTO accounts_balances 
                (account_ID, date, cash_balance, record_date) 
                VALUES (?, ?, ?, ?)
            """, cash_data)
            conn.commit()
            inserted_count = len(cash_data)
            
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()
        
    return inserted_count

def process_ibkr_account(token, query_id):
    """
    Get IBKR cash data for a specific account.
    
    Args:
        token (str): IBKR Flex Web Service token
        query_id (str): IBKR Flex query ID
        
    Returns:
        pandas.DataFrame or False: DataFrame with cash data if successful, False otherwise
    """
    # Get CSV data as DataFrame
    df = get_ibkr_flex_data(token, query_id)
    
    if df is False or df.empty:
        print("No data retrieved from IBKR" if df is False else "Empty DataFrame returned from IBKR")
        return False
        
    print(f"Cash report retrieved from IBKR with {len(df)} rows")
    print(df.head())
    return df

def process_account_data(token, query_id, account_type="paper"):
    """Process account data for a specific account type"""
    print(f"\nProcessing {account_type} trading account:")
    
    # Step 1: Get the cash data from IBKR
    df = process_ibkr_account(token, query_id)
    
    # Step 2: Update the database with the cash data
    if isinstance(df, pd.DataFrame):
        inserted = update_accounts_balances(df)
        if inserted:
            print(f"Updated database with {inserted} new cash entries for {account_type} account")
        else:
            print(f"No new cash entries inserted for {account_type} account")
    else:
        print(f"Failed to retrieve cash data for {account_type} account")

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Process paper trading account
    process_account_data(
        os.getenv("IBKR_TOKEN_PAPER"),
        os.getenv("IBKR_QUERY_ID_CASH_PAPER")
    )
    
    # Uncomment to process live account
    # process_account_data(
    #     os.getenv("IBKR_TOKEN_LIVE"),
    #     os.getenv("IBKR_QUERY_ID_CASH_LIVE")
    # ) 