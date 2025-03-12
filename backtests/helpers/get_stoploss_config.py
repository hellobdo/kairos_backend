import sqlite3
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_stoploss_config(stoploss_id: int) -> dict:
    """
    Get stoploss configuration from the database.
    
    Args:
        stoploss_id (int): ID of the stoploss configuration to retrieve
        
    Returns:
        dict: Dictionary containing stoploss configuration with keys:
            - stop_type: 'perc', 'abs', or 'variable'
            - stop_value: float value for fixed stops
            - stop_func: function for variable stops (if applicable)
            - name: name of the stoploss configuration
            - description: description of the stoploss configuration
    """
    try:
        conn = sqlite3.connect('data/algos.db')
        cursor = conn.cursor()
        
        # Get stoploss configuration
        cursor.execute("""
            SELECT type, delta_abs, delta_perc, name, description
            FROM manager_stoploss
            WHERE id = ?
        """, (stoploss_id,))
        
        row = cursor.fetchone()
        if not row:
            logger.error(f"No stoploss configuration found for ID {stoploss_id}")
            return None
            
        stop_type, delta_abs, delta_perc, name, description = row
        
        # Convert database type to internal type
        if stop_type == 'fix_abs':
            return {
                'stop_type': 'abs',
                'stop_value': delta_abs,
                'name': name,
                'description': description
            }
        elif stop_type == 'fix_perc':
            return {
                'stop_type': 'perc',
                'stop_value': delta_perc / 100.0,  # Convert percentage to decimal
                'name': name,
                'description': description
            }
        elif stop_type == 'variable':
            # For variable stoploss, we need to get the ranges
            cursor.execute("""
                SELECT price_min, price_max, stop_perc
                FROM manager_stoploss_ranges
                WHERE stoploss_id = ?
                ORDER BY price_min
            """, (stoploss_id,))
            
            ranges = cursor.fetchall()
            if not ranges:
                logger.error(f"No ranges found for variable stoploss ID {stoploss_id}")
                return None
                
            # Create a function that returns the appropriate stop percentage based on price
            def get_stop_perc(price):
                for price_min, price_max, stop_perc in ranges:
                    if (price_min is None or price >= price_min) and (price_max is None or price < price_max):
                        return stop_perc / 100.0  # Convert percentage to decimal
                return ranges[-1][2] / 100.0  # Use last range's percentage as default
            
            return {
                'stop_type': 'custom',
                'stop_func': get_stop_perc,
                'name': name,
                'description': description
            }
            
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return None
    finally:
        conn.close()
        
    return None

if __name__ == "__main__":
    # Example usage with stoploss_config_id = 3
    config = get_stoploss_config(3)
    if config:
        logger.info("Retrieved stoploss configuration:")
        logger.info(config) 