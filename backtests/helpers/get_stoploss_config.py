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
                SELECT min_price, max_price, delta_perc, delta_abs
                FROM manager_stoploss_price_ranges
                WHERE style_id = ?
                ORDER BY min_price
            """, (stoploss_id,))
            
            ranges = cursor.fetchall()
            if not ranges:
                logger.error(f"No ranges found for variable stoploss ID {stoploss_id}")
                return None
                
            # Create a function that returns the appropriate stop percentage based on price
            def get_stop_perc(price):
                for min_price, max_price, delta_perc, delta_abs in ranges:
                    if (min_price is None or price >= min_price) and (max_price is None or price < max_price):
                        # Use delta_perc if available, otherwise calculate from delta_abs
                        if delta_perc is not None:
                            return delta_perc / 100.0  # Convert percentage to decimal
                        elif delta_abs is not None:
                            # If we have an absolute delta, convert to percentage
                            return delta_abs / price if price != 0 else 0.01
                        else:
                            # Neither available, use default
                            return 0.01  # Default to 1%
                
                # Default to using the last range's values
                last_range = ranges[-1]
                last_delta_perc = last_range[2]  # delta_perc
                last_delta_abs = last_range[3]  # delta_abs
                
                if last_delta_perc is not None:
                    return last_delta_perc / 100.0
                elif last_delta_abs is not None and price != 0:
                    return last_delta_abs / price
                else:
                    return 0.01  # Default to 1%
            
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