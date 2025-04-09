from api.yf import download_data
from utils.db_utils import DatabaseManager
import pandas as pd
import time

db = DatabaseManager()

def map_dataframe_to_ohlcv_table(df, matching_df):
    """
    Maps DataFrame columns to match the ohlcv table structure
    
    Args:
        df: DataFrame with OHLCV data
        assets_df: DataFrame with asset information to get asset_id
        
    Returns:
        DataFrame with columns matching ohlcv table
    """
    # Create a new DataFrame with only the columns needed for stocks_ohlcv
    ohlcv_df = pd.DataFrame()
    
    # Map date column
    ohlcv_df['datetime'] = df['date']
    
    # Map price and volume columns
    ohlcv_df['open'] = df['open']
    ohlcv_df['high'] = df['high']
    ohlcv_df['low'] = df['low']
    ohlcv_df['close'] = df['close']
    ohlcv_df['volume'] = df['volume']
    ohlcv_df['adjusted_close'] = df['close']
    
    # We need to map ticker to asset_id using the assets table
    # Create a dictionary mapping ticker to asset_id
    ticker_to_id = dict(zip(matching_df['ticker'], matching_df['id']))
    ohlcv_df['asset_id'] = df['ticker'].map(ticker_to_id)
    
    # Filter out rows where asset_id is null (ticker not found in assets table)
    ohlcv_df = ohlcv_df.dropna(subset=['asset_id'])
    
    # Convert asset_id to integer
    ohlcv_df['asset_id'] = ohlcv_df['asset_id'].astype(int)

    # Format the timestamp as yyyy-mm-dd hh:mm:ss
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    ohlcv_df['insert_timestamp'] = current_time
    
    return ohlcv_df

def get_specific_tickers(assets_df, ticker_list):
    """
    Get specific tickers from the database
    
    Returns:
        list: List containing specific ticker symbols
    """
    try:
        # Filter the assets_df to only include the tickers in the ticker_list
        ticker_df = assets_df[assets_df['ticker'].isin(ticker_list)]
        ticker_list = ticker_df['ticker'].tolist()
        return ticker_list
    except Exception as e:
        print(f"Error getting specific tickers: {e}")
        return []

def process_stock_data(table_name, timeframe, ticker_list=None):
    try:
        print(f"Getting list of assets from {table_name} table...")
        assets_df = db.get_table_data(table_name)
        if ticker_list is None:
            ticker_list = assets_df['ticker'].unique().tolist()
        
        else:
            ticker_list = get_specific_tickers(assets_df, ticker_list)
        
        print(f"Found {len(ticker_list)} unique tickers to process")
        print(ticker_list)
    except Exception as e:
        print(f"Error getting stock list: {e}")
        exit()
    

    processed_count = 0
    error_count = 0
    
    # Process each ticker individually instead of all at once
    for ticker in ticker_list:
        try:
            print(f"Processing ticker: {ticker}")
            
            # Download data for single ticker
            print(f"Downloading data for {ticker}...")
            data = download_data([ticker])
            
            if not data or ticker not in data:
                print(f"No data found for {ticker}, skipping")
                continue
                
            # Convert dictionary to DataFrame
            df_data = data[ticker]
            
            # Map to OHLCV table format
            print(f"Mapping {ticker} data to {table_name} table...")
            ohlcv_df = map_dataframe_to_ohlcv_table(df_data, assets_df)
            
            if ohlcv_df.empty:
                print(f"No mappable data for {ticker}, skipping")
                continue
            
            # Insert into database
            if timeframe == 'daily':
                table_name = f'{table_name}_ohlcv_daily'
            else:
                print(f"Timeframe {timeframe} not supported, skipping")
                break
            
            print(f"Inserting {ticker} data into {table_name} table...")
            rows_inserted = db.insert_dataframe(ohlcv_df, f'{table_name}')
            print(f"Successfully inserted {rows_inserted} rows for {ticker}")
            
            processed_count += 1
            print(f"Completed {processed_count}/{len(ticker_list)} tickers")
            
            # Add a small delay between tickers to avoid rate limits
            if processed_count < len(ticker_list):
                delay = 1.5  # 1.5 seconds between requests
                print(f"Waiting {delay} seconds before next ticker...")
                time.sleep(delay)
            
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            error_count += 1
            # Continue with next ticker rather than exiting
            continue
    
    print(f"Processing complete. Successfully processed: {processed_count}, Errors: {error_count}")



if __name__ == "__main__":
    # needs to pass in table name and ticker list
    process_stock_data('indexes', 'daily', ['VIX'])