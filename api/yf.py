import yfinance as yf
import pandas as pd
import numpy as np

def download_data(tickers: list[str], period="5y", start=None, end=None):
    """
    Downloads historical data for the specified tickers using yfinance.
    
    Args:
        tickers (list): List of ticker symbols
        period (str, optional): Time period to download (default: 5 years).
                              Used if start and end are not specified.
                              Examples: "1d", "1mo", "1y", "5y", "max"
        start (str or datetime, optional): Start date for data download.
                                         Format: 'YYYY-MM-DD' or datetime object.
                                         Takes precedence over period if specified.
        end (str or datetime, optional): End date for data download.
                                       Format: 'YYYY-MM-DD' or datetime object.
                                       If not specified and start is specified, defaults to today.
    
    Returns:
        dict: Dictionary with ticker symbols as keys and DataFrames as values,
              with standardized date formats and column names
    """
    data = {}
    
    for ticker in tickers:
        # Create Ticker object
        if ticker == 'VIX':
            ticker = '^VIX'
        ticker_obj = yf.Ticker(ticker)
        
        # Download historical data
        if start and end:
            df = ticker_obj.history(start=start, end=end)
        else:
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
        # Convert ^VIX back to VIX
        if ticker == '^VIX':
            df['ticker'] = 'VIX'
        else:
            df['ticker'] = ticker
                
        # Store in dictionary
        if ticker == '^VIX':
            data['VIX'] = df
        else:
            data[ticker] = df
        
        print(f"Downloaded {len(df)} rows of data for {ticker}")
    
    return data