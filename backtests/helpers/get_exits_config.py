import sqlite3
import logging
from pathlib import Path
from typing import Dict, Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_exits_config(exit_id: int) -> Optional[Dict]:
    """
    Retrieve exit configuration from manager_exits and manager_exits_ranges tables.
    
    Args:
        exit_id: The ID of the exit configuration to retrieve
        
    Returns:
        Dictionary containing the exit configuration:
        For fixed exits:
        {
            'id': int,
            'type': 'fixed',
            'name': str,
            'description': str,
            'risk_reward': float,  # The fixed risk/reward ratio
            'size_exit': float,    # Will always be 1.0 for fixed exits
        }
        
        For variable exits:
        {
            'id': int,
            'type': 'variable',
            'name': str,
            'description': str,
            'ranges': List[Dict],  # List of ranges with their risk/reward ratios
            # Each range has:
            # {
            #    'size_exit': float,   # Portion of position to exit (0-1)
            #    'risk_reward': float  # Risk/reward ratio for this portion
            # }
        }
        or None if not found
    """
    try:
        # Connect to database
        db_path = Path('data/algos.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get exit configuration
        cursor.execute("""
            SELECT 
                id,
                type,
                name,
                description,
                risk_reward
            FROM manager_exits
            WHERE id = ?
        """, (exit_id,))
        
        row = cursor.fetchone()
        if not row:
            logger.error(f"No exit configuration found for ID {exit_id}")
            return None
            
        exit_id, exit_type, name, description, risk_reward = row
        
        # Get the ranges configuration
        cursor.execute("""
            SELECT size_exit, risk_reward
            FROM manager_exits_ranges
            WHERE exit_id = ?
            ORDER BY size_exit DESC
        """, (exit_id,))
        
        ranges = cursor.fetchall()
        if not ranges:
            logger.error(f"No ranges found for exit ID {exit_id}")
            return None
        
        # Format configuration based on type
        if exit_type == 'fixed':
            # Fixed exits have exactly one range with size_exit = 1
            if len(ranges) != 1 or ranges[0][0] != 1.0:
                logger.error(f"Invalid ranges for fixed exit ID {exit_id}")
                return None
                
            return {
                'id': exit_id,
                'type': 'fixed',
                'name': name,
                'description': description,
                'risk_reward': risk_reward,
                'size_exit': 1.0
            }
            
        elif exit_type == 'variable':
            # Variable exits have multiple ranges that sum to 1
            exit_ranges = [
                {
                    'size_exit': size_exit,
                    'risk_reward': rr
                }
                for size_exit, rr in ranges
            ]
            
            # Verify that size_exits sum to 1
            total_size = sum(r['size_exit'] for r in exit_ranges)
            if round(total_size, 10) != 1.0:
                logger.error(f"Invalid ranges for variable exit ID {exit_id}: sum of size_exit = {total_size}")
                return None
            
            return {
                'id': exit_id,
                'type': 'variable',
                'name': name,
                'description': description,
                'ranges': exit_ranges
            }
        
        return None
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # Example usage with exit_id = 1
    config = get_exits_config(1)
    if config:
        logger.info("Retrieved exit configuration:")
        logger.info(f"Name: {config['name']}")
        logger.info(f"Type: {config['type']}")
        
        if config['type'] == 'fixed':
            logger.info(f"Risk/Reward ratio: {config['risk_reward']}")
            logger.info(f"Size exit: {config['size_exit']}")
        else:
            logger.info("Variable exit configuration:")
            for i, r in enumerate(config['ranges'], 1):
                logger.info(f"Range {i}:")
                logger.info(f"  Size exit: {r['size_exit']}")
                logger.info(f"  Risk/Reward: {r['risk_reward']}") 