import requests
import os
import pandas as pd
from dotenv import load_dotenv
from utils.db_utils import DatabaseManager

def get_polygon_tickers_data(**kwargs):
    """
    Get stock data from Polygon API with optional filters.
    
    Args:
        **kwargs: Optional query parameters for the Polygon API
                 e.g., market_cap_gt=1000000000, sector='Technology'

    Returns:
        list: A list of dictionaries containing stock data
    """
    load_dotenv()
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    
    
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "apiKey": polygon_api_key,
        "limit": 1000  # Maximum per page
    }
    
    # Add any additional parameters passed to the function
    params.update(kwargs)

    all_results = []
    page = 1

    while url:
        print(f"Fetching page {page}...")
        # For subsequent pages, include API key in URL
        if page > 1:
            url = f"{url}&apiKey={polygon_api_key}"
            
        response = requests.get(url, params=params if page == 1 else None)
        data = response.json()
        results = data.get("results", [])
        all_results.extend(results)
        url = data.get("next_url")
        page += 1

    return all_results

def clean_up_data(data_source):
    """
    Clean up the data source by removing records with null CIKs and duplicate tickers.
    
    Args:
        data_source (pandas.DataFrame): DataFrame containing stock data from Polygon API
        
    Returns:
        pandas.DataFrame: Cleaned DataFrame with no null CIKs and no duplicate tickers
    """
    if not isinstance(data_source, pd.DataFrame):
        print("Converting data to DataFrame")
        data_source = pd.DataFrame(data_source)
    
    original_count = len(data_source)
    print(f"Original record count: {original_count}")
    
    # Check for null or empty CIKs
    null_cik_mask = data_source['cik'].isna()
    null_cik_count = null_cik_mask.sum()
    
    empty_cik_mask = data_source['cik'] == ''
    empty_cik_count = empty_cik_mask.sum()
    
    # If we have any null or empty CIKs, remove them
    if null_cik_count > 0 or empty_cik_count > 0:
        # Combine the masks
        invalid_cik_mask = null_cik_mask | empty_cik_mask
        invalid_cik_count = invalid_cik_mask.sum()
        
        print(f"Removing {invalid_cik_count} records with null or empty CIKs ({null_cik_count} null, {empty_cik_count} empty)")
        
        if 'ticker' in data_source.columns:
            invalid_cik_tickers = data_source.loc[invalid_cik_mask, 'ticker'].tolist()
            print(f"Tickers with invalid CIKs (first 10): {', '.join(invalid_cik_tickers[:10])}" + 
                  ("..." if len(invalid_cik_tickers) > 10 else ""))
        
        # Remove all rows with invalid CIKs
        data_source = data_source[~invalid_cik_mask]
    
    # Check for and remove duplicate tickers
    duplicate_mask = data_source.duplicated(subset=['ticker'], keep='first')
    duplicate_count = duplicate_mask.sum()
    
    if duplicate_count > 0:
        print(f"Removing {duplicate_count} duplicate ticker records")
        # Display some of the duplicates before removing
        duplicate_tickers = data_source[data_source.duplicated(subset=['ticker'], keep=False)]
        if not duplicate_tickers.empty:
            print("Sample of duplicate tickers (showing first 5):")
            # Group by ticker and show the first 5 groups
            for ticker, group in list(duplicate_tickers.groupby('ticker'))[:5]:
                print(f"  Ticker: {ticker} ({len(group)} records)")
                for i, (_, row) in enumerate(group.iterrows()):
                    if i < 2:  # Show at most 2 examples per ticker
                        print(f"    - {row.get('name', 'N/A')} | CIK: {row.get('cik', 'N/A')} | FIGI: {row.get('composite_figi', 'N/A')}")
                if len(group) > 2:
                    print(f"    - ... and {len(group) - 2} more")
        
        # Keep only the first occurrence of each ticker
        data_source = data_source.drop_duplicates(subset=['ticker'], keep='first')
    
    cleaned_count = len(data_source)
    removed_count = original_count - cleaned_count
    print(f"Cleaned data: {cleaned_count} records ({removed_count} removed, {removed_count/original_count:.2%} of original)")
    
    return data_source

if __name__ == "__main__":
    db = DatabaseManager()

    # Load environment variables from .env file
    load_dotenv()
    polygon_api_key = os.getenv("POLYGON_API_KEY")

    base_params = {
        "market": "stocks", 
        "type": "CS",
        "currency": "USD",
        "sort": "ticker",
        "order": "asc"
    }

    # Initialize polygon_data as empty DataFrame
    polygon_data = pd.DataFrame()

    # Get data from Polygon API
    try:
        data = get_polygon_tickers_data(**base_params)
        if data and len(data) > 0:
            # Print total count
            print(f"Total stocks: {len(data)}")
            
            # Print data structure of first stock for debugging
            print("\nData structure of first stock:")
            first_stock = data[0]
            for key, value in first_stock.items():
                print(f"  {key}: {value}")
            
            # Print first 10 stocks with available fields
            print("\nFirst 10 stocks:")
            for i, stock in enumerate(data[:10]):
                print(f"{i+1}. {stock['ticker']} | {stock['name']} | {stock['market']} | {stock['type']} | {stock['currency_name']} | Active: {stock['active']}")

            # Convert to DataFrame
            polygon_data = pd.DataFrame(data)
            polygon_data = clean_up_data(polygon_data)
    
    except Exception as e:
        print(f"Error getting data from Polygon API: {e}")
    
    try:
        # Insert in db
        if not polygon_data.empty:
            inserted = db.insert_dataframe(polygon_data, "stocks")
            
    except Exception as e:
        raise ValueError(f"Error updating stock history: {e}")