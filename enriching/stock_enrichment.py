from api.yf import download_data
from utils.db_utils import DatabaseManager
import pandas as pd
import time

db = DatabaseManager()

def map_dataframe_to_ohlcv_table(df, stocks_df):
    """
    Maps DataFrame columns to match the stocks_ohlcv table structure
    
    Args:
        df: DataFrame with OHLCV data
        stocks_df: DataFrame with stock information to get stock_id
        
    Returns:
        DataFrame with columns matching stocks_ohlcv table
    """
    # Create a new DataFrame with only the columns needed for stocks_ohlcv
    ohlcv_df = pd.DataFrame()
    
    # Map date column
    ohlcv_df['date'] = df['date']
    
    # Map price and volume columns
    ohlcv_df['open'] = df['open']
    ohlcv_df['high'] = df['high']
    ohlcv_df['low'] = df['low']
    ohlcv_df['close'] = df['close']
    ohlcv_df['volume'] = df['volume']
    ohlcv_df['adjusted_close'] = df['close']
    
    # We need to map ticker to stock_id using the stocks table
    # Create a dictionary mapping ticker to stock_id
    ticker_to_id = dict(zip(stocks_df['ticker'], stocks_df['id']))
    ohlcv_df['stock_id'] = df['ticker'].map(ticker_to_id)
    
    # Filter out rows where stock_id is null (ticker not found in stocks table)
    ohlcv_df = ohlcv_df.dropna(subset=['stock_id'])
    
    # Convert stock_id to integer
    ohlcv_df['stock_id'] = ohlcv_df['stock_id'].astype(int)

    return ohlcv_df

def process_stock_data():
    try:
        print("Getting list of stocks from database...")
        stocks_df = db.get_table_data('stocks')
        ticker_list = stocks_df['ticker'].unique().tolist()
        print(f"Found {len(ticker_list)} unique tickers to process")
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
            print(f"Mapping {ticker} data to stocks_ohlcv table...")
            ohlcv_df = map_dataframe_to_ohlcv_table(df_data, stocks_df)
            
            if ohlcv_df.empty:
                print(f"No mappable data for {ticker}, skipping")
                continue
            
            # Insert into database
            print(f"Inserting {ticker} data into stocks_ohlcv table...")
            rows_inserted = db.insert_dataframe(ohlcv_df, 'stocks_ohlcv')
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
    process_stock_data()