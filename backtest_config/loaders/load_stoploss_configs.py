import json
import os
import sqlite3
from typing import Dict, List

def load_stoploss_variable_style(db_path: str, config: Dict) -> int:
    """Load a variable stop loss style configuration with price ranges."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Insert the variable style first
        cursor.execute("""
            INSERT INTO stoploss_variable_styles (
                name,
                description
            ) VALUES (?, ?)
            RETURNING id
        """, (
            config['name'],
            config.get('description')
        ))
        
        style_id = cursor.fetchone()[0]
        
        # Insert all price ranges for this style
        for price_range in config['price_ranges']:
            cursor.execute("""
                INSERT INTO stoploss_price_ranges (
                    style_id,
                    min_price,
                    max_price,
                    delta_abs,
                    delta_perc
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                style_id,
                price_range['min_price'],
                price_range['max_price'],
                price_range.get('delta_abs'),
                price_range.get('delta_perc')
            ))
        
        conn.commit()
        return style_id
        
    finally:
        conn.close()

def load_stoploss(db_path: str, config: Dict) -> int:
    """Load a stop loss configuration."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # If it's a variable stop loss, load the style first
        variable_style_id = None
        if config['type'] == 'variable':
            variable_style_id = load_stoploss_variable_style(db_path, config['variable_style'])
        
        cursor.execute("""
            INSERT INTO stoploss (
                type,
                delta_abs,
                delta_perc,
                variable_style_id
            ) VALUES (?, ?, ?, ?)
            RETURNING id
        """, (
            config['type'],
            config.get('delta_abs'),
            config.get('delta_perc'),
            variable_style_id
        ))
        
        stoploss_id = cursor.fetchone()[0]
        conn.commit()
        print(f"Loaded stop loss config (ID: {stoploss_id})")
        return stoploss_id
        
    finally:
        conn.close()

def main():
    """Load all stop loss configurations into database."""
    # Get database path
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'kairos.db')
    
    # Load stop loss configs
    stoploss_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', 'stoploss')
    for file in os.listdir(stoploss_dir):
        if file.endswith('.json'):
            config_file = os.path.join(stoploss_dir, file)
            with open(config_file, 'r') as f:
                config = json.load(f)
                load_stoploss(db_path, config)

if __name__ == "__main__":
    main() 