import pandas as pd
import sqlite3
from datetime import datetime
import os

def classify_market_session(time_str):
    """Classify the market session based on the time.
    Pre-market: 05:00-09:29
    Regular Hours: 09:30-16:00
    After Hours: 16:01-21:30
    """
    time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').time()
    if time >= datetime.strptime('09:30', '%H:%M').time() and time <= datetime.strptime('16:00', '%H:%M').time():
        return 'regular'
    elif time < datetime.strptime('09:30', '%H:%M').time():
        return 'pre'
    else:
        return 'post'

def add_market_sessions():
    print("Connecting to database...")
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'kairos.db')
    conn = sqlite3.connect(db_path)
    
    # Read existing data
    print("Reading existing data...")
    df = pd.read_sql_query("SELECT * FROM historical_data_30mins", conn)
    
    # Add market session classification
    print("\nClassifying market sessions...")
    df['market_session'] = df['date_and_time'].apply(classify_market_session)
    
    # Create new table with market session
    print("Creating new table with market sessions...")
    df.to_sql('historical_data_30mins_with_sessions', conn, if_exists='replace', index=False)
    
    # Create indexes
    print("Creating indexes...")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_symbol_date_session 
        ON historical_data_30mins_with_sessions(symbol, date_and_time, market_session)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_date_session 
        ON historical_data_30mins_with_sessions(date_and_time, market_session)
    """)
    
    # Verify data distribution
    print("\nVerifying data distribution across market sessions...")
    cursor.execute("""
        SELECT market_session, COUNT(*) as count 
        FROM historical_data_30mins_with_sessions 
        GROUP BY market_session
    """)
    session_counts = cursor.fetchall()
    
    print("\nData distribution by market session:")
    for session, count in session_counts:
        print(f"{session}: {count:,} rows")
    
    # Close connection
    conn.commit()
    conn.close()
    
    print("\nProcessing complete!")

if __name__ == "__main__":
    add_market_sessions() 