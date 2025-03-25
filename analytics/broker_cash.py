import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import re
from api.ibkr import get_ibkr_report
from utils.db_utils import DatabaseManager

# Initialize database manager
db = DatabaseManager()

def update_accounts_balances(df):
    """
    Update accounts_balances table with cash report data.
    Ensures only one entry per account_ID + date combination.
    
    Args:
        df (pandas.DataFrame): Cash report data from IBKR
        
    Returns:
        int: Number of records inserted
        
    Raises:
        ValueError: If date is not in YYYY-MM-DD format
    """
    # Skip if DataFrame is empty or missing required columns
    if df.empty or not all(col in df.columns for col in ['clientaccountid', 'endingcash', 'todate']):
        return 0
    
    inserted_count = 0
    
    try:
        # Get the mapping from account_external_ID to ID
        account_map_df = db.get_account_map()
        
        # Current timestamp for record_date
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Create empty DataFrame with the required columns
        balances_df = pd.DataFrame(columns=['account_id', 'date', 'cash_balance', 'record_date'])
        
        # Process each valid row
        for _, row in df.iterrows():
            # Skip rows with missing data
            if not (row.get('clientaccountid') and row.get('endingcash')):
                continue
                
            # Find matching account ID
            matching_accounts = account_map_df[account_map_df['account_external_id'] == str(row['clientaccountid'])]
            if matching_accounts.empty:
                continue
                
            # Extract and validate data
            db_account_id = int(matching_accounts['id'].iloc[0])
            cash_balance = float(row['endingcash'])
            date_value = str(row['todate'])[:10]  # Get only YYYY-MM-DD part
            
            # Validate date format
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_value):
                raise ValueError(f"Date must be in YYYY-MM-DD format. Got: {row['todate']}, extracted: {date_value}")
            
            # Check if record already exists
            if db.check_balance_exists(db_account_id, date_value):
                print(f"Account ID {db_account_id} with date {date_value} already exists in database - skipping")
                continue
            
            # Add to DataFrame directly
            new_row = pd.DataFrame({
                'account_id': [db_account_id],
                'date': [date_value],
                'cash_balance': [cash_balance],
                'record_date': [current_timestamp]
            })
            balances_df = pd.concat([balances_df, new_row], ignore_index=True)
        
        # Batch insert all records
        if not balances_df.empty:
            # Insert using the new method
            inserted_count = db.insert_dataframe(balances_df, 'accounts_balances')
            
    except Exception as e:
        raise
        
    return inserted_count

def process_account_data(token, query_id, account_type="paper"):
    """Process account data for a specific account type"""
    print(f"\nProcessing {account_type} trading account:")
    
    try:
        # Get CSV data as DataFrame directly from IBKR using the centralized function
        df = get_ibkr_report(token, query_id, "cash")
        
        if df is False:
            print(f"Failed to retrieve cash data for {account_type} account")
            return
        
        # Update the database with the cash data
        inserted = update_accounts_balances(df)
        if inserted:
            print(f"Updated database with {inserted} new cash entries for {account_type} account")
        else:
            print(f"No new cash entries inserted for {account_type} account")
            
    except Exception as e:
        print(f"Error processing {account_type} account: {e}")

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Process paper trading account
    process_account_data(
        os.getenv("IBKR_TOKEN_PAPER"),
        os.getenv("IBKR_QUERY_ID_CASH_PAPER"),
        "paper"
    )
    
    # Uncomment to process live account
    # process_account_data(
    #     os.getenv("IBKR_TOKEN_LIVE"),
    #     os.getenv("IBKR_QUERY_ID_CASH_LIVE"),
    #     "live"
    # ) 