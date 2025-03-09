import json
import os
import sqlite3
from typing import Dict
from dataclasses import dataclass

@dataclass
class StrategyConfig:
    """Strategy configuration from JSON file."""
    strategy_name: str
    version: str
    variant: str
    indicator_name: str
    parameters: Dict

def load_strategy_to_db(db_path: str, config_file: str) -> int:
    """
    Load a strategy configuration from JSON into algo_strategies table.
    
    Args:
        db_path: Path to SQLite database
        config_file: Path to strategy JSON config file
        
    Returns:
        strategy_id: ID of the created/updated strategy
    """
    # Read strategy config
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Extract parameters
    params = config['parameters'].copy()  # Make a copy to avoid modifying original
    symbols = config.get('symbols', [])  # Get symbols from root level
    
    # Connect to DB
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Insert or update strategy
        cursor.execute("""
            INSERT INTO algo_strategies (
                strategy_name,
                strategy_version,
                variant,
                indicator_name,
                indicator_parameters,
                symbols
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(strategy_name, strategy_version, variant)
            DO UPDATE SET
                indicator_name = excluded.indicator_name,
                indicator_parameters = excluded.indicator_parameters,
                symbols = excluded.symbols
            RETURNING strategy_id
        """, (
            config['strategy_name'],
            config['version'],
            config['variant'],
            config['class_name'].replace('Strategy', ''),  # TightCandleStrategy -> TightCandle
            json.dumps(params),  # Only indicator-specific parameters
            json.dumps(symbols)  # Symbols as JSON array
        ))
        
        strategy_id = cursor.fetchone()[0]
        conn.commit()
        print(f"Loaded strategy {config['strategy_name']} (ID: {strategy_id})")
        return strategy_id
        
    except Exception as e:
        print(f"Error loading strategy {config_file}: {e}")
        conn.rollback()
        raise
        
    finally:
        conn.close()

def main():
    """Load all strategy configurations into database."""
    # Get database path
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'kairos.db')
    
    # Load all strategy configs
    configs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', 'strategies')
    for file in os.listdir(configs_dir):
        if file.endswith('.json'):
            config_file = os.path.join(configs_dir, file)
            load_strategy_to_db(db_path, config_file)

if __name__ == "__main__":
    main() 