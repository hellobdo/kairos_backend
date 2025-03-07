import sqlite3
import argparse
from datetime import datetime

def get_execution_dates(db_path: str) -> list:
    """Get list of unique execution dates from algo_trades table"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT execution_date, COUNT(*) as trade_count
            FROM algo_trades
            GROUP BY execution_date
            ORDER BY execution_date DESC
        """)
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Error fetching execution dates: {e}")
        return []
    finally:
        conn.close()

def clean_trades(db_path: str, execution_date: str):
    """Clean trades from algo_trades table for a specific execution date"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # First get count of trades to be deleted
        cursor.execute("""
            SELECT COUNT(*) 
            FROM algo_trades 
            WHERE execution_date = ?
        """, (execution_date,))
        count = cursor.fetchone()[0]
        
        if count == 0:
            print(f"No trades found for execution date: {execution_date}")
            return
        
        # Ask for confirmation
        print(f"\nWARNING: You are about to delete {count} trades from execution date: {execution_date}")
        confirmation = input("\nAre you sure? This will permanently delete these trades (y/n): ")
        
        if confirmation.lower() != 'y':
            print("Operation cancelled.")
            return
        
        # Delete trades
        cursor.execute("""
            DELETE FROM algo_trades 
            WHERE execution_date = ?
        """, (execution_date,))
        
        conn.commit()
        print(f"\nSuccessfully deleted {count} trades from execution date: {execution_date}")
        
    except sqlite3.Error as e:
        print(f"Error cleaning trades: {e}")
        conn.rollback()
    finally:
        conn.close()

def main():
    # Database path
    db_path = 'data.db'
    
    # Get available execution dates
    execution_dates = get_execution_dates(db_path)
    
    if not execution_dates:
        print("No trades found in the database.")
        return
    
    # Show available dates
    print("\nAvailable execution dates:")
    print("---------------------------")
    for i, (date, count) in enumerate(execution_dates, 1):
        print(f"{i}. {date} ({count} trades)")
    
    # Get user selection
    while True:
        try:
            selection = input("\nEnter the number of the execution date to delete (or 'q' to quit): ")
            
            if selection.lower() == 'q':
                print("Operation cancelled.")
                return
            
            idx = int(selection) - 1
            if 0 <= idx < len(execution_dates):
                selected_date = execution_dates[idx][0]
                break
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number or 'q' to quit.")
    
    # Clean trades for selected date
    clean_trades(db_path, selected_date)

if __name__ == "__main__":
    main() 