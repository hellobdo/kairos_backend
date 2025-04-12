import pandas as pd
from utils.pandas_utils import csv_to_dataframe, convert_to_numeric
from utils.process_executions_utils import process_datetime_fields, identify_trade_ids
from analytics.process_trades import process_trades
from backtests.utils.backtest_data_to_db import get_backtest_info

def side_follows_qty(df):
    """
    Standardizes the 'side' column to "buy" or "sell" and 
    adjusts quantity based on side:
    - If side contains 'sell', makes quantity negative
    - If side contains 'buy', leaves quantity as is
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
        processed_df.loc[sell_mask, 'quantity'] = processed_df.loc[sell_mask, 'quantity'] * -1
    
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

def process_csv_to_executions(csv_path):
    """
    Process a CSV file containing execution data and return a processed DataFrame.
    
    This function:
    1. Reads the CSV file into a DataFrame
    2. Drops unnecessary columns
    3. Cleans empty rows
    4. Converts numeric fields
    5. Processes datetime fields
    
    Args:
        csv_path (str): Path to the CSV file
        
    Returns:
        pd.DataFrame: Processed DataFrame containing execution data
        False: If any processing step fails
    """
    print(f"Processing CSV file: {csv_path}")
    
    # Step 1: Read CSV into DataFrame
    try:
        df = csv_to_dataframe(csv_path)
        
        # Check if CSV loading was successful
        if df is None:
            # No need to print another error message as csv_to_dataframe already did
            return False
        
        print(f"CSV loaded successfully. Shape: {df.shape}")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return False
    
    try:
        # Step 2: Drop unnecessary columns
        df = drop_columns(df)
        print("Columns dropped successfully")
    except Exception as e:
        print(f"Error dropping columns: {e}")
        return False

    try:
        # Step 3: Convert numeric fields
        numeric_fields = ['quantity', 'price', 'trade_cost']
        df = convert_to_numeric(df, numeric_fields)
        print("Numeric conversion successful")
    except Exception as e:
        print(f"Error converting numeric fields: {e}")
        return False
    
    try:    
        # Step 4: Process date and time fields
        # This also validates execution_timestamp and sorts the DataFrame
        df = process_datetime_fields(df, 'timestamp')
        if df.empty:
            print("WARNING: process_datetime_fields returned an empty DataFrame")
            return False
        
        print("Datetime processing successful")
    except Exception as e:
        print(f"Error processing datetime fields: {e}")
        return False

    # Step 5: Standardize sides and adjust quantities
    df = side_follows_qty(df)

    # Step 6: Identify trade IDs
    df = identify_trade_ids(df, db_validation=False)
    print("Trade IDs identification successful")

    # Convert is_entry and is_exit to boolean
    df['is_entry'] = df['is_entry'].astype(bool)
    df['is_exit'] = df['is_exit'].astype(bool)

    return df

def process_executions_to_trades(df, backtest: bool = True):
    """
    Process a DataFrame of executions into trades.
    
    Args:
        df (pandas.DataFrame): DataFrame containing execution data
            
    Returns:
        pandas.DataFrame: DataFrame with processed trades, or False if processing fails
    """
    try:
        settings_df = get_backtest_info()
        trades_df = process_trades(df, backtest, settings_df)
        if trades_df is None:
            print("Processing trades failed")
            return False
        print("Trades processing successful")
    
        return trades_df
            
    except Exception as e:
        print(f"Error in trade processing: {e}")
        return False
