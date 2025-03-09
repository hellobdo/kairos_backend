import json
import os
import sqlite3
from typing import Dict, Optional
from dataclasses import dataclass
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class RiskManager:
    """Risk manager configuration."""
    risk_per_trade: float
    max_daily_risk: float

def load_risk_manager(db_path: str, config: Dict) -> int:
    """Load a risk manager configuration."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO risk_manager (
                risk_per_trade,
                max_daily_risk
            ) VALUES (?, ?)
            RETURNING id
        """, (
            config['risk_per_trade'],
            config['max_daily_risk']
        ))
        
        risk_id = cursor.fetchone()[0]
        conn.commit()
        print(f"Loaded risk manager config (ID: {risk_id})")
        return risk_id
        
    finally:
        conn.close()

def main():
    """Load all risk configurations into database."""
    # Get database path
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'kairos.db')
    
    # Load risk manager configs
    risk_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', 'risk')
    for file in os.listdir(risk_dir):
        if file.endswith('.json'):
            config_file = os.path.join(risk_dir, file)
            with open(config_file, 'r') as f:
                config = json.load(f)
                load_risk_manager(db_path, config)

if __name__ == "__main__":
    main() 