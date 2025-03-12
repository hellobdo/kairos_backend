import sqlite3
import logging
from pathlib import Path
import json
from typing import Dict, Union, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_stoploss_config(stoploss_id: int) -> Optional[Dict]:
    """
    Retrieve stoploss configuration from manager_stoploss table.
    
    Args:
        stoploss_id: The ID of the stoploss configuration to retrieve
        
    Returns:
        Dictionary containing the stoploss configuration formatted for VectorBT,
        or None if not found
    """
    try:
        # Connect to database
        db_path = Path('data/algos.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get stoploss configuration
        cursor.execute("""
            SELECT 
                ms.id,
                ms.type,
                ms.delta_perc,
                ms.delta_abs,
                ms.variable_style_id,
                msvs.name as style_name
            FROM manager_stoploss ms
            LEFT JOIN manager_stoploss_variable_styles msvs 
                ON ms.variable_style_id = msvs.id
            WHERE ms.id = ?
        """, (stoploss_id,))
        
        row = cursor.fetchone()
        if not row:
            logger.error(f"No stoploss configuration found for ID {stoploss_id}")
            return None
            
        stoploss_id, stop_type, delta_perc, delta_abs, variable_style_id, style_name = row
        
        # Format basic config
        config = {
            'id': stoploss_id,
            'type': stop_type,
            'delta_perc': delta_perc,
            'delta_abs': delta_abs
        }
        
        # If it's a variable style stoploss, get the price ranges
        if variable_style_id:
            cursor.execute("""
                SELECT 
                    min_price,
                    max_price,
                    delta_perc,
                    delta_abs
                FROM manager_stoploss_price_ranges
                WHERE style_id = ?
                ORDER BY min_price
            """, (variable_style_id,))
            
            price_ranges = []
            for range_row in cursor.fetchall():
                min_price, max_price, range_delta_perc, range_delta_abs = range_row
                price_ranges.append({
                    'min_price': min_price,
                    'max_price': max_price,
                    'delta_perc': range_delta_perc,
                    'delta_abs': range_delta_abs
                })
            
            config['variable_style'] = {
                'name': style_name,
                'price_ranges': price_ranges
            }
        
        # Convert to VectorBT format
        vectorbt_config = format_for_vectorbt(config)
        
        return vectorbt_config
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()

def format_for_vectorbt(config: Dict) -> Dict:
    """
    Format the stoploss configuration for VectorBT usage.
    
    Args:
        config: Raw stoploss configuration from database
        
    Returns:
        Dictionary formatted for VectorBT
    """
    stop_type = config['type']
    
    if stop_type == 'fix_perc':
        return {
            'stop_type': 'perc',
            'stop_value': config['delta_perc'] / 100  # Convert percentage to decimal
        }
    elif stop_type == 'fix_abs':
        return {
            'stop_type': 'abs',
            'stop_value': config['delta_abs']
        }
    elif stop_type == 'variable':
        # For variable stoploss, we'll need to implement a custom stoploss function
        # that uses the price ranges
        price_ranges = config['variable_style']['price_ranges']
        
        def get_stop_value(price):
            for range_config in price_ranges:
                if range_config['min_price'] <= price <= range_config['max_price']:
                    if range_config['delta_perc'] is not None:
                        return range_config['delta_perc'] / 100
                    return range_config['delta_abs']
            # Default to the first range if price is below min
            if price < price_ranges[0]['min_price']:
                return price_ranges[0]['delta_perc'] / 100 if price_ranges[0]['delta_perc'] is not None else price_ranges[0]['delta_abs']
            # Default to the last range if price is above max
            return price_ranges[-1]['delta_perc'] / 100 if price_ranges[-1]['delta_perc'] is not None else price_ranges[-1]['delta_abs']
        
        return {
            'stop_type': 'custom',
            'stop_func': get_stop_value
        }
    else:
        raise ValueError(f"Unsupported stop type: {stop_type}")

if __name__ == "__main__":
    # Example usage with stoploss_config_id = 3
    config = get_stoploss_config(3)
    if config:
        logger.info("Retrieved stoploss configuration:")
        logger.info(json.dumps(config, indent=2, default=str))  # default=str to handle custom functions 