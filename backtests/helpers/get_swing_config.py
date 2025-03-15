import sqlite3
import logging

# Configure logging
logger = logging.getLogger(__name__)

def get_swing_config(swing_config_id):
    """
    Get swing configuration from the database.
    
    Args:
        swing_config_id (int): The ID of the swing configuration to retrieve
        
    Returns:
        dict: A dictionary containing the swing configuration, or None if not found
    """
    conn = sqlite3.connect('data/algos.db')
    cursor = conn.cursor()
    
    try:
        # Query the manager_swings table for the specified ID
        cursor.execute("""
            SELECT id, swings_allowed, description
            FROM manager_swings
            WHERE id = ?
        """, (swing_config_id,))
        
        row = cursor.fetchone()
        
        if row is None:
            logger.error(f"No swing configuration found with ID {swing_config_id}")
            return None
        
        # Create a dictionary with the swing configuration
        swing_config = {
            'id': row[0],
            'swings_allowed': row[1],
            'description': row[2]
        }
        
        logger.info(f"Retrieved swing configuration: {swing_config}")
        return swing_config
        
    except Exception as e:
        logger.error(f"Error retrieving swing configuration: {e}")
        return None
    finally:
        conn.close() 


if __name__ == "__main__":
    # Example usage with swing_config_id = 1
    config = get_swing_config(1)
    if config:
        print("\nRetrieved swing configuration:")
        print(f"ID: {config['id']}")
        print(f"Swings Allowed: {config['swings_allowed']} ({'Yes' if config['swings_allowed'] == 1 else 'No'})")
        print(f"Description: {config['description']}")
    else:
        print("Failed to retrieve swing configuration") 