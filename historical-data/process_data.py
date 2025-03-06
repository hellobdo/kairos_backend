import pandas as pd
import sqlite3
from datetime import datetime
import os

def process_historical_data():
    # Read the CSV file
    print("Reading CSV file...")
    df = pd.read_csv('historical-data/files/30min_charts_historical_data - 30min_charts.csv')
    
    # Print column names to verify
    print("\nColumns in CSV:", df.columns.tolist())
    
    # Convert timestamp to datetime
    print("\nProcessing timestamps...")
    df['date_and_time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Get list of unique symbols
    symbols = df['symbol'].unique()
    print(f"Found {len(symbols)} symbols: {', '.join(symbols)}")
    
    # Find the earliest common date for all symbols
    print("Finding common date range...")
    min_dates = df.groupby('symbol')['date_and_time'].min()
    max_dates = df.groupby('symbol')['date_and_time'].max()
    
    print("\nDate ranges by symbol:")
    for symbol in symbols:
        print(f"{symbol}: {min_dates[symbol]} to {max_dates[symbol]}")
    
    latest_start = min_dates.max()
    earliest_end = max_dates.min()
    
    print(f"\nCommon date range for all symbols:")
    print(f"Start: {latest_start}")
    print(f"End: {earliest_end}")
    
    # Filter data to common date range
    print("\nFiltering to common date range...")
    df = df[
        (df['date_and_time'] >= latest_start) & 
        (df['date_and_time'] <= earliest_end)
    ]
    
    # Check for missing intervals
    print("\nChecking for missing intervals...")
    pivot = df.pivot(index='date_and_time', columns='symbol', values='close')
    missing_counts = pivot.isnull().sum()
    
    if missing_counts.sum() > 0:
        print("\nFound missing data points:")
        for symbol in symbols:
            if missing_counts[symbol] > 0:
                print(f"{symbol}: {missing_counts[symbol]} missing intervals")
        
        # Remove rows with any missing data
        print("\nRemoving rows with missing data...")
        pivot = pivot.dropna()
        df = df[df['date_and_time'].isin(pivot.index)]
    
    # Prepare data for database
    print("\nPreparing data for database...")
    df = df[['date_and_time', 'symbol', 'open', 'high', 'low', 'close', 'Volume']]  # Changed 'volume' to 'Volume'
    
    # Rename Volume column to lowercase
    df = df.rename(columns={'Volume': 'volume'})
    
    # Connect to database
    print("\nConnecting to database...")
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'kairos.db')
    conn = sqlite3.connect(db_path)
    
    # Store in database
    print("Storing data in database...")
    df.to_sql('historical_data_30mins', conn, if_exists='replace', index=False)
    
    # Create indexes
    print("Creating indexes...")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_symbol_date 
        ON historical_data_30mins(symbol, date_and_time)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_date 
        ON historical_data_30mins(date_and_time)
    """)
    
    # Verify data
    print("\nVerifying data...")
    cursor.execute("SELECT COUNT(*) FROM historical_data_30mins")
    total_rows = cursor.fetchone()[0]
    
    cursor.execute("SELECT symbol, COUNT(*) FROM historical_data_30mins GROUP BY symbol")
    symbol_counts = cursor.fetchall()
    
    print(f"\nTotal rows in database: {total_rows}")
    print("\nRows per symbol:")
    for symbol, count in symbol_counts:
        print(f"{symbol}: {count}")
    
    # Close connection
    conn.commit()
    conn.close()
    
    print("\nProcessing complete!")

if __name__ == "__main__":
    process_historical_data() 