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

def calculate_indicators(table_name):
    """
    Calculates technical indicators for the OHLCV data in the specified table
    
    Args:
        table_name (str): Name of the OHLCV table (stocks_ohlcv_daily or indexes_ohlcv_daily)
    """
    print(f"Calculating indicators for {table_name}...")
    
    # Get all unique asset_ids from the table
    df_asset_ids = db.select_distinct(table_name, 'asset_id')
    asset_ids = df_asset_ids['asset_id'].tolist()
    
    print(f"Found {len(asset_ids)} unique assets in {table_name}")
    
    total_updated = 0
    
    # Process each asset_id separately
    for asset_id in asset_ids:
        try:
            print(f"Processing asset_id: {asset_id}")
            
            # Get all OHLCV data and then filter by asset_id in Python
            # This avoids SQL syntax error with multiple ORDER BY clauses
            all_df = db.get_table_data(table_name, "datetime")
            df = all_df[all_df['asset_id'] == asset_id].copy()  # Create explicit copy
            
            if df.empty:
                print(f"No data found for asset_id {asset_id}")
                continue
            
            # Ensure datetime is in order
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime')
            
            # Calculate indicators
            # 1. Average Daily Volume (30 days)
            df['avg_daily_vol_30d'] = df['volume'].rolling(window=30).mean()
            
            # 2. Average Daily Range (20 days)
            df['adr_20d'] = df['daily_range_perc'].rolling(window=20).mean()
            
            # Create a subset of the dataframe with only the columns to update
            update_df = df[['id', 'avg_daily_vol_30d', 'adr_20d']].copy()
            
            # Use insert_dataframe with update_existing=True to update values
            print(f"Updating indicators for asset_id {asset_id}...")
            rows_updated = db.insert_dataframe(
                update_df,
                table_name,
                update_existing=True,
                id_field='id'
            )
            
            print(f"Updated {rows_updated} rows for asset_id {asset_id}")
            
            total_updated += 1
            if total_updated == 1:
                break
            
        except Exception as e:
            print(f"Error processing asset_id {asset_id}: {e}")
            continue
    
    print(f"Processed indicators for {total_updated} assets in {table_name}")

def main():
    # Process both stocks and indexes daily tables
    calculate_indicators('stocks_ohlcv_daily')
    calculate_indicators('indexes_ohlcv_daily')
    
if __name__ == "__main__":
    start_time = time.time()
    main() 
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time} seconds")