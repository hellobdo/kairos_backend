#!/usr/bin/env python3
import pandas as pd
import os
import sys
import time

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import DatabaseManager

# Initialize database connection
db = DatabaseManager()

def calculate_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print(f"Starting time is {start_time}")
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Ending time is {end_time}")
        print(f"Total execution time: {end_time - start_time} seconds")
        return result
    return wrapper

@calculate_time
def calculate_indicators(table_name):
    """
    Calculates technical indicators for the OHLCV data in the specified table using SQL
    
    Args:
        table_name (str): Name of the OHLCV table (stocks_ohlcv_daily or indexes_ohlcv_daily)
    """
    print(f"Calculating indicators for {table_name}...")
    
    # Get all unique asset_ids from the table
    df_asset_ids = db.select_distinct(table_name, 'asset_id')
    asset_ids = df_asset_ids['asset_id'].tolist()
    
    # Include only asset_ids 11 and 12
    asset_ids = [asset_id for asset_id in asset_ids if asset_id not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
    
    if not asset_ids:
        print("No asset_ids to process after filtering")
        return
        
    print(f"Processing {len(asset_ids)} unique assets in {table_name}")
    
    try:
        # Calculate 30-day average volume using SQL
        print("Calculating 30-day average volume...")
        vol_rows = db.update_avg_daily_volume(table_name, asset_ids)
        print(f"Updated {vol_rows} rows with avg_daily_vol_30d values")
        
        # Calculate 20-day average daily range using SQL
        print("Calculating 20-day average daily range...")
        adr_rows = db.update_avg_daily_range(table_name, asset_ids)
        print(f"Updated {adr_rows} rows with adr_20d values")
        
        print(f"Successfully updated indicators for assets: {asset_ids}")
    except Exception as e:
        print(f"Error processing indicators: {e}")

def main():
    # Process both stocks and indexes daily tables
    calculate_indicators('stocks_ohlcv_daily')
    # calculate_indicators('indexes_ohlcv_daily')

if __name__ == "__main__":
    main() 