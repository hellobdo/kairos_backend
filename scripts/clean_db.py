import sqlite3
import os

def clean_database():
    """
    Cleans all tables from kairos.db except the accounts table.
    """
    print("Connecting to database...")
    conn = sqlite3.connect('data/kairos.db')
    cursor = conn.cursor()
    
    try:
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"Found {len(tables)} tables: {', '.join(tables)}")
        
        # Filter out the accounts table
        tables_to_clean = [table for table in tables if table != 'accounts' and table != 'sqlite_sequence']
        
        print(f"Tables to clean: {', '.join(tables_to_clean)}")
        
        # Delete data from each table
        for table in tables_to_clean:
            print(f"Cleaning table: {table}")
            cursor.execute(f"DELETE FROM {table};")
            
            # Reset auto-increment if sqlite_sequence exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence';")
            if cursor.fetchone():
                cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}';")
        
        # Commit the changes
        conn.commit()
        print("Database cleaned successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"Error cleaning database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Check if database file exists
    if not os.path.exists('data/kairos.db'):
        print("Error: Database file 'data/kairos.db' not found!")
    else:
        print("Starting database cleanup...")
        clean_database() 