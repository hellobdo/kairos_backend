import sqlite3
import os

def remove_tight_candles_table():
    print("Connecting to database...")
    db_path = os.path.join(os.path.dirname(__file__), 'kairos.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tight_candles'")
    if cursor.fetchone():
        print("Found 'tight_candles' table. Removing...")
        cursor.execute("DROP TABLE tight_candles")
        print("Table removed successfully!")
    else:
        print("Table 'tight_candles' not found in database.")
    
    conn.commit()
    conn.close()
    print("\nProcessing complete!")

if __name__ == "__main__":
    remove_tight_candles_table() 