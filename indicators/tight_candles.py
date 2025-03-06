import sqlite3
import pandas as pd
import os
import numpy as np

def add_tight_candle_indicator():
    print("Connecting to database...")
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'kairos.db')
    conn = sqlite3.connect(db_path)
    
    # Query existing data
    query = """
    SELECT *
    FROM historical_data_30mins
    WHERE market_session = 'regular'
    ORDER BY date_and_time, symbol
    """
    
    print("Reading data from database...")
    df = pd.read_sql_query(query, conn)
    
    # Calculate absolute percentage difference between open and close
    print("\nCalculating price differences...")
    df['diff_pct_open_close'] = abs((df['close'] - df['open']) / df['open'] * 100)
    
    # Define tightness categories based on percentage difference
    def categorize_tightness(pct_diff):
        if pct_diff <= 0.1:  # Ultra tight: <= 0.1%
            return 'Ultra Tight'
        elif pct_diff <= 0.2:  # Very tight: 0.1% - 0.2%
            return 'Very Tight'
        elif pct_diff <= 0.3:  # Tight: 0.2% - 0.3%
            return 'Tight'
        elif pct_diff <= 0.5:  # Moderate: 0.3% - 0.5%
            return 'Moderate'
        else:  # Wide: > 0.5%
            return 'Wide'
    
    df['tightness'] = df['diff_pct_open_close'].apply(categorize_tightness)
    
    # Update the existing table with new columns
    print("\nUpdating database with new indicators...")
    cursor = conn.cursor()
    
    # Add new columns if they don't exist
    cursor.execute("PRAGMA table_info(historical_data_30mins)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'diff_pct_open_close' not in columns:
        cursor.execute("ALTER TABLE historical_data_30mins ADD COLUMN diff_pct_open_close REAL")
    if 'tightness' not in columns:
        cursor.execute("ALTER TABLE historical_data_30mins ADD COLUMN tightness TEXT")
    
    # Update the values
    print("Updating records...")
    for _, row in df.iterrows():
        cursor.execute("""
            UPDATE historical_data_30mins 
            SET diff_pct_open_close = ?, tightness = ?
            WHERE date_and_time = ? AND symbol = ?
        """, (row['diff_pct_open_close'], row['tightness'], row['date_and_time'], row['symbol']))
    
    # Create index for tightness if it doesn't exist
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_tightness 
        ON historical_data_30mins(tightness)
    """)
    
    # Print distribution of categories
    print("\nDistribution of tight candles per symbol:")
    category_counts = df.groupby(['symbol', 'tightness']).size().unstack(fill_value=0)
    print(category_counts)
    
    # Print percentage distribution
    print("\nPercentage distribution of tight candles per symbol:")
    percentage_dist = (category_counts.div(category_counts.sum(axis=1), axis=0) * 100).round(2)
    print(percentage_dist)
    
    # Print some examples of ultra tight candles
    print("\nExample of ultra tight candles:")
    ultra_tight = df[df['tightness'] == 'Ultra Tight'].groupby('symbol').head(3)
    print(ultra_tight[['date_and_time', 'symbol', 'open', 'close', 'diff_pct_open_close', 'tightness']].round(3))
    
    conn.commit()
    conn.close()
    print("\nProcessing complete!")

if __name__ == "__main__":
    add_tight_candle_indicator() 