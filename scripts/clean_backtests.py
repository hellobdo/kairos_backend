import sqlite3
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_table(conn, table_name):
    """Clean a table by deleting all records."""
    cursor = conn.cursor()
    
    # Get current count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    initial_count = cursor.fetchone()[0]
    logger.info(f"Current number of records in {table_name}: {initial_count}")
    
    # Delete all records
    logger.info(f"Deleting all records from {table_name}...")
    cursor.execute(f"DELETE FROM {table_name}")
    
    # Get new count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    final_count = cursor.fetchone()[0]
    
    logger.info(f"Successfully deleted {initial_count - final_count} records from {table_name}")
    logger.info(f"Records remaining in {table_name}: {final_count}")

def clean_tables():
    """Clean the algo_trades and backtest_runs tables by deleting all records."""
    try:
        # Get the path to the database relative to the workspace root
        db_path = Path('data/algos.db')
        
        # Connect to the database
        logger.info(f"Connecting to database at {db_path}")
        conn = sqlite3.connect(db_path)
        
        # Clean tables
        tables_to_clean = ['algo_trades', 'backtest_runs']
        for table in tables_to_clean:
            clean_table(conn, table)
        
        # Commit the changes
        conn.commit()
        logger.info("All changes committed successfully")
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    clean_tables() 