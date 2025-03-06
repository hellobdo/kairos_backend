import sqlite3
import os

def create_historical_data_table():
    # Get the path to the database file
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'kairos.db')
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create the historical_data_30mins table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS historical_data_30mins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        symbol TEXT NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(timestamp, symbol)
    )
    ''')
    
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    
    print("Successfully created historical_data_30mins table and indexes")

if __name__ == "__main__":
    create_historical_data_table() 