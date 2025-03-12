import sqlite3
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_algo_trades():
    """Clean the algo_trades table by deleting all records."""
    try:
        # Get the path to the database relative to the workspace root
        db_path = Path('data/kairos.db')
        
        # Connect to the database
        logger.info(f"Connecting to database at {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get current count
        cursor.execute("SELECT COUNT(*) FROM algo_trades")
        initial_count = cursor.fetchone()[0]
        logger.info(f"Current number of records in algo_trades: {initial_count}")
        
        # Delete all records
        logger.info("Deleting all records from algo_trades...")
        cursor.execute("DELETE FROM algo_trades")
        
        # Get new count
        cursor.execute("SELECT COUNT(*) FROM algo_trades")
        final_count = cursor.fetchone()[0]
        
        # Commit the changes
        conn.commit()
        
        logger.info(f"Successfully deleted {initial_count - final_count} records")
        logger.info(f"Records remaining in algo_trades: {final_count}")
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    clean_algo_trades() 