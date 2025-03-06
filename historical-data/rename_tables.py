import sqlite3
import os

def rename_tables():
    print("Connecting to database...")
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'kairos.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # First verify that both tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('historical_data_30mins', 'historical_data_30mins_with_sessions')")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]
        
        if 'historical_data_30mins_with_sessions' not in table_names:
            raise Exception("New table 'historical_data_30mins_with_sessions' not found!")
            
        print("\nVerifying row counts...")
        if 'historical_data_30mins' in table_names:
            cursor.execute("SELECT COUNT(*) FROM historical_data_30mins")
            old_count = cursor.fetchone()[0]
            print(f"Old table row count: {old_count:,}")
        
        cursor.execute("SELECT COUNT(*) FROM historical_data_30mins_with_sessions")
        new_count = cursor.fetchone()[0]
        print(f"New table row count: {new_count:,}")
        
        # Drop the old table if it exists
        print("\nDropping old table...")
        cursor.execute("DROP TABLE IF EXISTS historical_data_30mins")
        
        # Rename the new table
        print("Renaming new table...")
        cursor.execute("""
            ALTER TABLE historical_data_30mins_with_sessions 
            RENAME TO historical_data_30mins
        """)
        
        # Verify the rename
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='historical_data_30mins'")
        if cursor.fetchone():
            print("\nTable successfully renamed!")
        else:
            raise Exception("Failed to verify renamed table!")
        
        conn.commit()
        print("\nAll operations completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"\nError occurred: {str(e)}")
        print("Rolling back changes...")
    
    finally:
        conn.close()

if __name__ == "__main__":
    rename_tables() 