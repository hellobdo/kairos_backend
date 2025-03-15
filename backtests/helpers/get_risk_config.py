import sqlite3
import logging
from pathlib import Path
from typing import Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_risk_config(risk_id: int) -> Optional[Dict]:
    """
    Retrieve risk configuration from manager_risk table.
    
    Args:
        risk_id: The ID of the risk configuration to retrieve
        
    Returns:
        Dictionary containing the risk configuration:
        {
            'id': int,
            'risk_per_trade': float,  # percentage as decimal (e.g., 0.01 for 1%)
            'max_daily_risk': float,   # percentage as decimal (e.g., 0.05 for 5%)
            'outside_regular_hours_allowed': int # 1 if outside regular hours are allowed, 0 otherwise
        }
        or None if not found
    """
    try:
        # Connect to database
        db_path = Path('data/algos.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get risk configuration
        cursor.execute("""
            SELECT 
                id,
                risk_per_trade,
                max_daily_risk,
                outside_regular_hours_allowed
            FROM manager_risk
            WHERE id = ?
        """, (risk_id,))
        
        row = cursor.fetchone()
        if not row:
            logger.error(f"No risk configuration found for ID {risk_id}")
            return None
            
        risk_id, risk_per_trade, max_daily_risk, outside_regular_hours_allowed = row
        
        # Format configuration
        config = {
            'id': risk_id,
            'risk_per_trade': risk_per_trade / 100,  # Convert percentage to decimal
            'max_daily_risk': max_daily_risk / 100,   # Convert percentage to decimal
            'outside_regular_hours_allowed': outside_regular_hours_allowed  # Keep as integer (1 or 0)
        }
        
        return config
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # Example usage with risk_config_id = 1
    config = get_risk_config(1)
    if config:
        logger.info("Retrieved risk configuration:")
        logger.info(f"Risk per trade: {config['risk_per_trade']*100}%")
        logger.info(f"Max daily risk: {config['max_daily_risk']*100}%") 
        logger.info(f"Outside regular hours allowed: {'Yes' if config['outside_regular_hours_allowed'] == 1 else 'No'}") 