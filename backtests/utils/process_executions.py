import pandas as pd
from utils.pandas_utils import csv_to_dataframe, clean_empty_rows, convert_to_numeric
from utils.process_executions_utils import process_datetime_fields, identify_trade_ids
from utils.db_utils import DatabaseManager

# Initialize database manager
db = DatabaseManager()

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
            'is_entry': df['is_entry'],
            'is_exit': df['is_exit']
        })
        
        # Add run_id only if it exists in the DataFrame
        if 'run_id' in df.columns:
            backtest_executions_df['run_id'] = df['run_id']
        
        # Insert the DataFrame into the database
        records_inserted = db.insert_dataframe(backtest_executions_df, 'backtest_executions')
        
        print(f"Successfully inserted {records_inserted} records into backtest_executions table")
        return records_inserted
        
    except Exception as e:
        print(f"Error inserting backtest executions into database: {e}")
        raise


def side_follows_qty(df):
    """
    Standardizes the 'side' column to "buy" or "sell" and 
    adjusts filled_quantity based on side:
    - If side contains 'sell', makes filled_quantity negative
    - If side contains 'buy', leaves filled_quantity as is
    """
    # Create a copy to avoid modifying the original DataFrame
    processed_df = df.copy()
    
    # Check if 'side' column exists
    if 'side' in processed_df.columns:
        # First standardize sides to "buy" or "sell"
        # Convert to strings and lowercase for comparison
        processed_df['side'] = processed_df['side'].astype(str).str.lower()
        
        # Set side to "buy" or "sell" based on content
        buy_mask = processed_df['side'].str.contains('buy', case=False)
        sell_mask = processed_df['side'].str.contains('sell', case=False)
        
        # Set standardized sides
        processed_df.loc[buy_mask, 'side'] = 'buy'
        processed_df.loc[sell_mask, 'side'] = 'sell'
        
        # Now apply the quantity adjustment for sell orders
        processed_df.loc[sell_mask, 'filled_quantity'] = processed_df.loc[sell_mask, 'filled_quantity'] * -1
    
    return processed_df

def drop_columns(df):
    """
    Drop specific columns from a DataFrame. Handles non-existent columns gracefully.
    
    Args:
        df (pandas.DataFrame): DataFrame to process
        
    Returns:
        pandas.DataFrame: DataFrame with specified columns removed
    """
    columns_to_drop = [
        'strategy',
        'status',
        'multiplier',
        'time_in_force',
        'asset.strike',
        'asset.multiplier',
        'asset.asset_type'
    ]
    
    # Get list of columns that actually exist in the DataFrame
    existing_columns = [col for col in columns_to_drop if col in df.columns]
    
    # Drop only existing columns
    if existing_columns:
        df = df.drop(columns=existing_columns)
        print(f"Dropped columns: {existing_columns}")
    else:
        print("None of the specified columns exist in the DataFrame")
        
    return df

def process_csv(csv_path, run_id=None):
    """
    Orchestration function that reads a CSV file and processes the resulting DataFrame.
    
    This function:
    1. Reads the CSV file into a DataFrame
    2. Drops unnecessary columns from the DataFrame
    
    Args:
        csv_path (str): Path to the CSV file
        
    Returns:
        pandas.DataFrame: Processed DataFrame, or None if CSV reading failed
    """
    print(f"Processing CSV file: {csv_path}")
    
    # Step 1: Read CSV into DataFrame
    df = csv_to_dataframe(csv_path)
    
    # Check if CSV loading was successful
    if df is None:
        # No need to print another error message as csv_to_dataframe already did
        return None
    
    print(f"CSV loaded successfully. Shape: {df.shape}")
    
    try:
        # Step 2: Drop unnecessary columns
        df = drop_columns(df)
        print("Columns dropped successfully")

        df = clean_empty_rows(df, 'filled_quantity')

        # Step 3: Convert numeric fields
        numeric_fields = ['filled_quantity', 'price', 'trade_cost']
        df = convert_to_numeric(df, numeric_fields)
        print("Numeric conversion successful")
        
        # Step 4: Process date and time fields
        # This also validates execution_timestamp and sorts the DataFrame
        df = process_datetime_fields(df, 'time')
        if df.empty:
            print("WARNING: process_datetime_fields returned an empty DataFrame")
            return pd.DataFrame()
        
        print("Datetime processing successful")


        # Renaming quantity field for compatibility with identify_trade_ids
        df = df.rename(columns={
            'filled_quantity': 'quantity', 
            })

        df_executions_with_trade_ids = identify_trade_ids(df)

        if run_id is not None:
            df['run_id'] = run_id

        inserted = insert_executions_to_db(df_executions_with_trade_ids, run_id)

        if inserted:
            print(f"Updated database with {inserted} new executions for {run_id} backtest")
            return True
        else:
            print(f"No new executions inserted for {run_id} backtest")
            return False
        
    except Exception as e:
        print(f"Error processing CSV: {e}")
        return False