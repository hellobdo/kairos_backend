import yfinance as yf
import pandas as pd
import numpy as np

def download_data(tickers=["SPY", "QQQ"], period="5y"):
    """
    Downloads historical data for the specified tickers using yfinance.
    
    Args:
        tickers (list): List of ticker symbols
        period (str): Time period to download (default: 5 years)
    
    Returns:
        dict: Dictionary with ticker symbols as keys and DataFrames as values,
              with standardized date formats and column names
    """
    data = {}
    
    for ticker in tickers:
        # Create Ticker object
        ticker_obj = yf.Ticker(ticker)
        
        # Download historical data
        df = ticker_obj.history(period=period)
        
        # Reset index to make Date a column
        df = df.reset_index()

        # Convert all column names to lowercase
        df.columns = df.columns.str.lower()
        
        # Standardize date handling
        # Convert to datetime object first
        df['date'] = pd.to_datetime(df['date'])
        # Extract date components before converting to string
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['week'] = df['date'].dt.isocalendar().week
        # Convert to string format after extracting components
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df['ticker'] = ticker
                
        # Store in dictionary
        data[ticker] = df
        
        print(f"Downloaded {len(df)} rows of data for {ticker}")
    
    return data

# Example usage
if __name__ == "__main__":
    # Download data for SPY and QQQ
    etf_data = download_data()
    
    # Print the first few rows of each DataFrame
    for ticker, df in etf_data.items():
        print(f"\n{ticker} data sample:")
        print(df.head())
        
        # Print available columns
        print(f"\nColumns in {ticker} data:")
        print(df.columns.tolist())
