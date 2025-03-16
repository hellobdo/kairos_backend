import sqlite3
import logging
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_entry_config(config_id: int, db_path: str = "data/algos.db") -> Optional[Dict[str, Any]]:
    """
    Load entry configuration from the database based on the config ID.
    
    Args:
        config_id: The ID of the entry configuration to retrieve
        db_path: Path to the SQLite database
        
    Returns:
        Dictionary containing entry configuration or None if not found
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Query the manager_entry table for the specified config ID
        cursor.execute("SELECT id, field, signal, direction FROM manager_entry WHERE id = ?", (config_id,))
        result = cursor.fetchone()
        
        if not result:
            logger.warning(f"No entry configuration found with ID {config_id}")
            return None
        
        # Format the result as a dictionary
        entry_config = {
            "id": result[0],
            "field": result[1],
            "signal": result[2],
            "direction": result[3],
        }
        
        logger.info(f"Loaded entry configuration: {entry_config}")
        return entry_config
        
    except Exception as e:
        logger.error(f"Error loading entry configuration: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

# Example usage
if __name__ == "__main__":
    # Example: Get a specific entry configuration
    config = get_entry_config(1)
    print(f"Entry config #1: {config}")