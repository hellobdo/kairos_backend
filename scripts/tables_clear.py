#!/usr/bin/env python3
"""
Clear all tables in the trades database.
This script removes all data from the tables while preserving the table structure.
"""
import sqlite3
from pathlib import Path

# Database configuration
DB_PATH = Path("data/kairos.db")

def clear_tables():
    """Clear all tables in the database."""
    print("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Start a transaction
        cursor.execute("BEGIN TRANSACTION")
        
        # Get list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        # Delete data from each table
        for (table_name,) in tables:
            if table_name != 'sqlite_sequence':  # Skip SQLite internal table
                print(f"Clearing table: {table_name}")
                cursor.execute(f"DELETE FROM {table_name}")
                cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table_name}'")
        
        # Commit the transaction
        conn.commit()
        print("\nAll tables cleared successfully!")
        
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        print(f"\nError: {str(e)}")
        raise
    
    finally:
        # Close connection
        conn.close()

if __name__ == "__main__":
    try:
        clear_tables()
    except Exception as e:
        print(f"Failed to clear tables: {str(e)}")
        exit(1) 