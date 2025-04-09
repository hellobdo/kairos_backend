import pandas as pd
import os
from dotenv import load_dotenv
from filters.get_stocks_polygon import get_polygon_tickers_data

def get_polygon_data():
    """
    Fetch data from Polygon API and return as DataFrame
    """
    # Load environment variables
    load_dotenv()
    
    # Set up parameters for Polygon API
    base_params = {
        "market": "stocks", 
        "primary_exchange": "XNAS,XNYS",
        "type": "CS",
        "currency": "USD",
        "active": "true",
        "is_etf": "false",
        "sort": "ticker",
        "order": "asc"
    }
    
    print("Fetching data from Polygon API...")
    data = get_polygon_tickers_data(**base_params)
    print(f"Retrieved {len(data)} stocks from API")
    
    # Convert to DataFrame
    return pd.DataFrame(data)

def analyze_field(df, field_name, display_name=None):
    """
    Analyze a field in the DataFrame for nulls, empty values, and duplicates
    
    Args:
        df (pandas.DataFrame): DataFrame containing the data
        field_name (str): Name of the field to analyze
        display_name (str, optional): Display name for the field, defaults to field_name
        
    Returns:
        dict: Analysis results
    """
    if display_name is None:
        display_name = field_name.upper()
    
    print(f"\n=== {display_name} ANALYSIS ===")
    
    # Check if field exists
    if field_name not in df.columns:
        print(f"Error: DataFrame missing {field_name} column")
        print("Available columns:", ", ".join(df.columns))
        return None
    
    results = {}
    total_records = len(df)
    results['total_records'] = total_records
    print(f"Total records: {total_records}")
    
    # Check for null values
    null_mask = df[field_name].isna()
    null_count = null_mask.sum()
    results['null_count'] = null_count
    results['null_percentage'] = null_count/total_records if total_records > 0 else 0
    print(f"\nNULL {display_name} VALUES: {null_count} ({results['null_percentage']:.2%} of total)")
    
    if null_count > 0:
        null_records = df[null_mask]
        print(f"Sample null records (first {min(10, len(null_records))}):")
        for _, row in null_records.iloc[:10].iterrows():
            print(f"  {row['ticker']} - {row.get('name', 'N/A')} - CIK: {row.get('cik', 'N/A')}")
    
    # Check for empty strings
    empty_mask = df[field_name] == ''
    empty_count = empty_mask.sum()
    results['empty_count'] = empty_count
    results['empty_percentage'] = empty_count/total_records if total_records > 0 else 0
    print(f"\nEMPTY {display_name} VALUES: {empty_count} ({results['empty_percentage']:.2%} of total)")
    
    if empty_count > 0:
        empty_records = df[empty_mask]
        print(f"Sample empty records (first {min(10, len(empty_records))}):")
        for _, row in empty_records.iloc[:10].iterrows():
            print(f"  {row['ticker']} - {row.get('name', 'N/A')} - CIK: {row.get('cik', 'N/A')}")
    
    # Check for duplicate values (excluding nulls and empty strings)
    valid_df = df[~null_mask & ~empty_mask]
    value_counts = valid_df[field_name].value_counts()
    duplicates = value_counts[value_counts > 1]
    results['duplicate_unique_values'] = len(duplicates)
    
    print(f"\nDUPLICATE {display_name} VALUES: {len(duplicates)} unique values with duplicates")
    
    if not duplicates.empty:
        total_dupes = sum(duplicates) - len(duplicates)
        results['duplicate_records'] = total_dupes
        results['duplicate_percentage'] = total_dupes/total_records if total_records > 0 else 0
        print(f"Total duplicate records: {total_dupes} ({results['duplicate_percentage']:.2%} of total)")
        
        print(f"\nTop duplicate {display_name} values:")
        # Convert items to list before slicing
        top_duplicates = list(duplicates.items())[:10]
        for i, (value, count) in enumerate(top_duplicates):
            dupe_records = df[df[field_name] == value]
            tickers = dupe_records['ticker'].tolist()
            print(f"{i+1}. {display_name} '{value}' appears {count} times:")
            for j, ticker in enumerate(tickers[:5]):  # Limit to 5 examples
                row = dupe_records[dupe_records['ticker'] == ticker].iloc[0]
                print(f"   {j+1}. {ticker} - {row.get('name', 'N/A')} - CIK: {row.get('cik', 'N/A')}")
            if len(tickers) > 5:
                print(f"   ... and {len(tickers) - 5} more")
    
    # Analyze what percentage would be valid for database insertion
    valid_records = valid_df.drop_duplicates(subset=[field_name])
    results['valid_unique_count'] = len(valid_records)
    results['valid_percentage'] = len(valid_records)/total_records if total_records > 0 else 0
    print(f"\nValid records for database using {field_name}: {len(valid_records)} ({results['valid_percentage']:.2%} of total)")
    
    return results

def analyze_composite_field(df, field1, field2, separator=':'):
    """
    Analyze a composite field created from two existing fields
    
    Args:
        df (pandas.DataFrame): DataFrame containing the data
        field1 (str): Name of the first field
        field2 (str): Name of the second field
        separator (str): Separator to use between fields
        
    Returns:
        dict: Analysis results
    """
    composite_name = f"{field1}{separator}{field2}"
    display_name = f"{field1.upper()}+{field2.upper()}"
    
    # Check if fields exist
    if field1 not in df.columns:
        print(f"Error: DataFrame missing {field1} column")
        return None
    
    if field2 not in df.columns:
        print(f"Error: DataFrame missing {field2} column")
        return None
    
    # Create composite field
    df[composite_name] = df[field1].astype(str) + separator + df[field2].astype(str)
    
    # Analyze the composite field
    return analyze_field(df, composite_name, display_name)

def analyze_all_fields(df):
    """
    Run analysis on multiple fields and combinations
    
    Args:
        df (pandas.DataFrame): DataFrame containing the data
    """
    # Analyze individual fields
    analyze_field(df, 'composite_figi', 'COMPOSITE FIGI')
    analyze_field(df, 'ticker', 'TICKER')
    analyze_field(df, 'cik', 'CIK')
    analyze_field(df, 'share_class_figi', 'SHARE CLASS FIGI')
    
    # Analyze composite fields
    print("\n=== COMPOSITE FIELD ANALYSIS ===")
    analyze_composite_field(df, 'ticker', 'cik')
    analyze_composite_field(df, 'ticker', 'primary_exchange')

if __name__ == "__main__":
    df = get_polygon_data()
    
    # Run analysis for a specific field
    field_to_analyze = input("Enter field to analyze (or 'all' for all fields): ").strip()
    
    if field_to_analyze.lower() == 'all':
        analyze_all_fields(df)
    elif field_to_analyze.lower() == 'composite_figi' or field_to_analyze.lower() == 'figi':
        analyze_field(df, 'composite_figi', 'COMPOSITE FIGI')
    elif '+' in field_to_analyze:
        # Handle composite fields like "ticker+cik"
        parts = field_to_analyze.split('+')
        if len(parts) == 2:
            analyze_composite_field(df, parts[0].strip(), parts[1].strip())
        else:
            print("Invalid composite field format. Use: field1+field2")
    else:
        # Try to analyze the specified field
        analyze_field(df, field_to_analyze) 