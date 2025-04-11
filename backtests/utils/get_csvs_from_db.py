import os
import pandas as pd
import sys

# Add the parent directory to the path to import from main code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from utils.db_utils import DatabaseManager

# Use DatabaseManager to get data
db_manager = DatabaseManager()

def save_ohlcv_to_csv(tickers, start_date=None, end_date=None, output_dir="data/csv/"):
    """
    Retrieve OHLCV data for multiple tickers and save to CSV files.
    
    Parameters:
    -----------
    tickers : list
        List of ticker symbols to retrieve data for
    start_date : str, optional
        Start date in YYYY-MM-DD format
    end_date : str, optional
        End date in YYYY-MM-DD format
    output_dir : str, optional
        Directory to save CSV files, default is "data"
    
    Returns:
    --------
    dict
        Dictionary mapping ticker symbols to file paths
    """
    
    # Get data for date range using built-in method
    df = db_manager.get_stock_data_for_date_range(tickers, start_date, end_date)
    
    if df.empty:
        print("No data to save")
        return {}
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Dictionary to store file paths
    file_paths = {}
    
    # Split by ticker and save to separate files
    for ticker in tickers:
        ticker_df = df[df['ticker'] == ticker]
        
        if not ticker_df.empty:
            file_name = f"{ticker}.csv"
            file_path = os.path.join(output_dir, file_name)
            
            # Save to CSV
            ticker_df.to_csv(file_path, index=False)
            file_paths[ticker] = file_path
            print(f"Saved {len(ticker_df)} rows for {ticker} to {file_path}")
        else:
            print(f"No data found for {ticker}")
    
    return file_paths

if __name__ == "__main__":
    # Example usage
    tickers = ["QQQ"]
    start_date = "2025-01-01"
    end_date = "2025-05-01"
    
    # Save data to CSV files
    file_paths = save_ohlcv_to_csv(tickers, start_date, end_date)
    print(f"Files saved: {file_paths}")