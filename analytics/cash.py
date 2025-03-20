import os
from dotenv import load_dotenv
from ibkr_connection import get_ibkr_flex_data
import pandas as pd
import sqlite3
from datetime import datetime

def update_accounts_balances(df):
    """
    Update accounts_balances table with cash report data.
    Ensures only one entry per account_ID + date combination.
    
    Args:
        df (pandas.DataFrame): Cash report data from IBKR
    """
    conn = sqlite3.connect('data/kairos.db')
    try:
        # Get the mapping from account_external_ID to ID
        account_map_df = pd.read_sql("""
            SELECT ID, account_external_ID 
            FROM accounts
        """, conn)
        
        # Current timestamp for record_date
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Process each account in the data
        cash_data = []
        for _, row in df.iterrows():
            # Skip header or non-data rows
            if 'ClientAccountID' not in row or not row['EndingCash']:
                continue
                
            account_identifier = str(row['ClientAccountID'])
            
            # Look up account_id from accounts table
            matching_accounts = account_map_df[
                account_map_df['account_external_ID'] == account_identifier
            ]
            
            if matching_accounts.empty:
                continue
                
            db_account_id = int(matching_accounts['ID'].iloc[0])  # Ensure it's an integer
            cash_balance = float(row['EndingCash'])
            date_value = str(row['ToDate'])[:10]  # Get just the date part
            
            # Check if record already exists
            existing = pd.read_sql("""
                SELECT * FROM accounts_balances
                WHERE account_ID = ? AND date = ?
            """, conn, params=[db_account_id, date_value])
            
            if not existing.empty:
                continue
            
            # Add to cash data
            cash_data.append({
                'account_ID': db_account_id,
                'date': date_value,
                'cash_balance': cash_balance,
                'record_date': current_timestamp
            })
        
        if cash_data:
            # Insert records one by one
            cursor = conn.cursor()
            for record in cash_data:
                cursor.execute("""
                    INSERT INTO accounts_balances 
                    (account_ID, date, cash_balance, record_date) 
                    VALUES (?, ?, ?, ?)
                """, (
                    record['account_ID'], 
                    record['date'], 
                    record['cash_balance'], 
                    record['record_date']
                ))
            
            conn.commit()
            
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def process_ibkr_account(token, query_id):
    """
    Process IBKR cash data for a specific account.
    
    Args:
        token (str): IBKR Flex Web Service token
        query_id (str): IBKR Flex query ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Get CSV data as DataFrame
    df = get_ibkr_flex_data(token, query_id)
    
    # Break the process if df is False
    if df is False:
        print("Breaking process - no data retrieved from IBKR")
        return False
    
    if not df.empty:
        print(f"Cash report retrieved from IBKR with {len(df)} rows")
        update_accounts_balances(df)
        return True
    else:
        print("No cash data to process (empty DataFrame)")
        return False

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Process paper trading account
    token_paper = os.getenv("IBKR_TOKEN_PAPER")
    query_id_paper = os.getenv("IBKR_QUERY_ID_CASH_PAPER")
    
    # Process live trading account (if available)
    token_live = os.getenv("IBKR_TOKEN_LIVE")
    query_id_live = os.getenv("IBKR_QUERY_ID_CASH_LIVE")
    
    print("\nProcessing paper trading account:")
    paper_result = process_ibkr_account(token_paper, query_id_paper)
    print(f"Paper account processing {'succeeded' if paper_result else 'failed'}")
    
    # Uncomment to process live account
    # print("\nProcessing live trading account:")
    # live_result = process_ibkr_account(token_live, query_id_live)
    # print(f"Live account processing {'succeeded' if live_result else 'failed'}")