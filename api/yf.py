import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pandas_market_calendars as mcal

def get_next_business_day(date_str):
    """Get the next business day after a given date"""
    # Create NYSE calendar
    nyse = mcal.get_calendar('NYSE')
    
    # Convert string to datetime
    if isinstance(date_str, str):
        date = pd.Timestamp(date_str)
    else:
        date = date_str
        
    # Get the next business day
    next_day = date + timedelta(days=1)
    
    # Check if it's a business day, if not get the next one
    calendar = nyse.schedule(start_date=next_day, end_date=next_day + timedelta(days=5))
    if calendar.empty:
        # If no trading days in the next 5 days (unlikely), just use next day
        return next_day.strftime('%Y-%m-%d')
    else:
        # Return the first trading day
        return calendar.index[0].strftime('%Y-%m-%d')

def download_data(tickers: list[str], period="5y", start=None, end=None, specific_date=None):
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
        specific_date (str or datetime, optional): If provided, only returns data for this specific date.
                                                Format: 'YYYY-MM-DD' or datetime object.
    
    Returns:
        DataFrame: Combined DataFrame containing data for all tickers with a 'ticker' column.
    """
    data = {}
    
    # Handle specific_date parameter
    if specific_date:
        # Convert specific_date to string if it's a datetime object
        if hasattr(specific_date, 'strftime'):
            specific_date = specific_date.strftime('%Y-%m-%d')
            
        # Set start to specific_date and end to next business day
        start = specific_date
        end = get_next_business_day(specific_date)
            
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
    
    # Combine all dataframes into a single DataFrame
    if data:
        return pd.concat(data.values(), ignore_index=True)
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data was downloaded
    
if __name__ == "__main__":
    # Example usage - use a date we know has data
    result = download_data(["QQQ"], specific_date="2025-04-10")
    print(result.head())