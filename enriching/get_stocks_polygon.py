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

def check_new_stocks(db, data_source):
    """
    Check which stocks from the input DataFrame don't exist in the database.
    
    Args:
        db (pandas.DataFrame): DataFrame containing stock data from database
        data_source (pandas.DataFrame): DataFrame containing stock data from Polygon API
        
    Returns:
        pandas.DataFrame: DataFrame containing only stocks that don't exist in the database
    """
    if 'ticker' not in data_source.columns:
        print("Error: DataFrame must contain 'ticker' column")
        return pd.DataFrame()
    
    if data_source.empty:
        return pd.DataFrame()
    
    try:      
        # Get existing tickers as strings for reliable comparison
        existing_tickers = set(db['ticker'].astype(str))
        
        # Filter the input DataFrame to keep only stocks whose tickers don't exist in the database
        new_stocks = data_source[~data_source['ticker'].astype(str).isin(existing_tickers)]
        
        return new_stocks
        
    except Exception as e:
        print(f"Error checking for non-existent stocks: {e}")
        return pd.DataFrame()

def check_changes(db, data_source):
    """
    Check if stock data from Polygon API has changed compared to what's in the database.
    
    Args:
        db (pandas.DataFrame): DataFrame containing stock data from database
        data_source (pandas.DataFrame): DataFrame containing stock data from Polygon API
        
    Returns:
        tuple: (updated_stocks, unchanged_stocks) DataFrames
    """
    
    # Create sets of tickers for comparison
    db_tickers = set(db['ticker'].astype(str))
    data_source_tickers = set(data_source['ticker'].astype(str))
    
    # Identify stocks that exist in both datasets
    common_tickers = data_source_tickers.intersection(db_tickers)
    
    # Initialize DataFrames for updated and unchanged stocks
    stocks_with_changes = pd.DataFrame()
    unchanged_stocks = pd.DataFrame()
    
    # Determine fields to compare (all fields except ticker and timestamp)
    if len(common_tickers) > 0 and not db.empty:
        # Get all column names from the database table
        db_columns = set(db.columns)
        # Exclude ticker and timestamp
        compare_fields = [field for field in db_columns if field not in ['ticker', 'timestamp']]
    
    # Check for changes in existing stocks
    for ticker in common_tickers:
        db_row = db[db['ticker'].astype(str) == ticker].iloc[0]
        data_source_row = data_source[data_source['ticker'].astype(str) == ticker].iloc[0]
        
        # Check if any fields have changed
        has_changes = False
        for field in compare_fields:
            # Only compare fields that exist in both dataframes
            if field in data_source_row.index and field in db_row.index:
                # Convert values to strings for comparison to handle different types
                if str(data_source_row[field]) != str(db_row[field]):
                    has_changes = True
                    break
        
        # Add to appropriate DataFrame
        if has_changes:
            stocks_with_changes = pd.concat([stocks_with_changes, data_source_row.to_frame().T], ignore_index=True)
        else:
            unchanged_stocks = pd.concat([unchanged_stocks, data_source_row.to_frame().T], ignore_index=True)
    
    return stocks_with_changes, unchanged_stocks

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

def update_stocks_history(db, changes_df, timestamp):
    """
    Track changes to stocks and store them in the stock_history table.
    
    Args:
        db (DatabaseManager): DatabaseManager instance
        changes_df (pandas.DataFrame): DataFrame containing stocks with updated information
        timestamp (str): Timestamp for the history records
        
    Returns:
        int: Number of records inserted into stock_history table
    """
    if changes_df.empty:
        return 0
    
    history_records = []
    
    try:
        # For each updated stock, identify the specific fields that changed
        for _, updated_row in changes_df.iterrows():
            ticker = updated_row['ticker']
            
            # Find the corresponding row in the database
            db_rows = db[db['ticker'].astype(str) == str(ticker)]
            if db_rows.empty:
                continue
                
            db_row = db_rows.iloc[0]
            
            # Determine fields to compare (all fields except ticker and timestamp)
            db_columns = set(db_row.index)
            compare_fields = [field for field in db_columns if field not in ['ticker', 'timestamp']]
            
            # Check each field for changes
            for field in compare_fields:
                if field in updated_row.index and field in db_row.index:
                    old_value = str(db_row[field])
                    new_value = str(updated_row[field])
                    
                    if old_value != new_value:
                        # Create a history record for this change
                        history_records.append({
                            'ticker': ticker,
                            'timestamp': timestamp,
                            'change_type': 'update',
                            'field_changed': field,
                            'old_value': old_value,
                            'new_value': new_value
                        })
        
        # Convert list of records to DataFrame and insert into stock_history table
        if history_records:
            history_df = pd.DataFrame(history_records)
            records_inserted = db.insert_dataframe(history_df, 'stock_history')
            print(f"Added {records_inserted} records to stock_history table")
            return records_inserted
        
        return 0
        
    except Exception as e:
        print(f"Error updating stock history: {e}")
        return 0

def update_stocks(new_stocks, stocks_with_changes, timestamp):
    """
    Add new stocks to the database and update existing stock information.
    
    Args:
        new_stocks (pandas.DataFrame): DataFrame containing new stocks to add
        stocks_with_changes (pandas.DataFrame): DataFrame containing stocks with updated information
        timestamp (str): Timestamp for the timestamp field
        
    Returns:
        tuple: (new_added_count, updated_count) - Number of stocks added and updated
    """
    new_added_count = 0
    stocks_updated_count = 0
    
    try:
        # Add new stocks to the database
        if not new_stocks.empty:
            # Add timestamp to new stocks
            new_stocks_with_timestamp = new_stocks.copy()
            new_stocks_with_timestamp['timestamp'] = timestamp
            
            # Insert new stocks into the stocks table
            new_added_count = db.insert_dataframe(new_stocks_with_timestamp, 'stocks')
            print(f"Added {new_added_count} new stocks to the database")
        
        # Update existing stocks with new information
        if not stocks_with_changes.empty:
            # Add timestamp to updated stocks
            updated_stocks_with_timestamp = stocks_with_changes.copy()
            updated_stocks_with_timestamp['timestamp'] = timestamp
            
            # For updates, use insert_dataframe with update_existing=True
            # This will update records where the ticker matches
            stocks_updated_count = db.insert_dataframe(updated_stocks_with_timestamp, 'stocks', 
                                                      update_existing=True, 
                                                      id_field='ticker')
            print(f"Updated {stocks_updated_count} existing stocks in the database")
        
        return new_added_count, stocks_updated_count
        
    except Exception as e:
        print(f"Error updating stocks in database: {e}")
        return 0, 0
    

def get_specific_tickers(polygon_data):
    """
    Get specific tickers from the Polygon API.
    
    Returns:
        pandas.DataFrame: DataFrame containing specific tickers
    """
    # Filter to only include specific ETFs: QQQ, SPY, DIA, and IWM
    etf_tickers = ['QQQ', 'SPY', 'DIA', 'IWM']
    print(f"\nFiltering data to only include these tickers: {', '.join(etf_tickers)}")
    polygon_data = polygon_data[polygon_data['ticker'].isin(etf_tickers)]
    print(f"Filtered data contains {len(polygon_data)} tickers")

    return polygon_data


if __name__ == "__main__":
    db = DatabaseManager()

    # Load environment variables from .env file
    load_dotenv()
    polygon_api_key = os.getenv("POLYGON_API_KEY")

    base_params = {
        "market": "stocks", 
        # "primary_exchange": "XNAS,XNYS",
        "type": "CS",
        "currency": "USD",
        "active": "true",
        # "is_etf": "false",
        "sort": "ticker",
        "order": "asc"
    }

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

            polygon_data = get_specific_tickers(polygon_data)
            print(f"Filtered data contains {len(polygon_data)} tickers")
            print(polygon_data.head())
    
    except Exception as e:
        raise ValueError(f"Error getting data from Polygon API: {e}")

    # Commenting out database operations temporarily
    """
    try:
        # Use get_table_data to get existing stocks from database
        stocks_in_db = db.get_table_data('stocks')
    
    except Exception as e:
        raise ValueError(f"Error getting stocks in db: {e}")
    
    try:
        # Find new stocks (not in database)
        new_stocks = check_new_stocks(stocks_in_db, polygon_data)
        print(f"Found {len(new_stocks)} new stocks")
    except Exception as e:
        raise ValueError(f"Error checking for new stocks: {e}")
    
    try:
        # Check for changes between existing and new data
        stocks_with_changes, unchanged_stocks = check_changes(stocks_in_db, polygon_data)
        print(f"Found {len(stocks_with_changes)} stocks with changes")
        print(f"Found {len(unchanged_stocks)} stocks without changes")
    except Exception as e:
        raise ValueError(f"Error checking for changes: {e}")
    
    # Generate timestamp once for both operations
    current_timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Update stock history for tracking changes
        if not stocks_with_changes.empty:
            history_count = update_stocks_history(stocks_in_db, stocks_with_changes, current_timestamp)
            print(f"Stock history updated: {history_count} changes recorded")
        
    except Exception as e:
        raise ValueError(f"Error updating stock history: {e}")
    
    try:
        # Update the database with new and updated stocks
        new_count, updated_count = update_stocks(new_stocks, stocks_with_changes, current_timestamp)
        print(f"Database updated: {new_count} new stocks added, {updated_count} stocks updated")
        
    except Exception as e:
        raise ValueError(f"Error updating stocks in database: {e}")
    """