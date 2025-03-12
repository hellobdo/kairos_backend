import json
import os
import sqlite3

def load_portfolio_to_db(db_path: str, config_file: str) -> int:
    """
    Load a portfolio configuration from JSON into backtest_portfolios table.
    
    Args:
        db_path: Path to SQLite database
        config_file: Path to portfolio JSON config file
        
    Returns:
        portfolio_id: ID of the created portfolio
    """
    # Read portfolio config
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Connect to DB
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # For each strategy in portfolio
        for strategy in config['strategies']:
            strategy_id = strategy['strategy_id']
            
            # Verify strategy exists
            cursor.execute("""
                SELECT strategy_id 
                FROM algo_strategies 
                WHERE strategy_id = ?
            """, (strategy_id,))
            
            if not cursor.fetchone():
                raise ValueError(f"Strategy ID {strategy_id} not found in algo_strategies")
            
            # Verify account exists
            cursor.execute("""
                SELECT account_id 
                FROM accounts 
                WHERE account_id = ?
            """, (config['account_id'],))
            
            if not cursor.fetchone():
                raise ValueError(f"Account ID {config['account_id']} not found in accounts")
            
            # Save portfolio configuration
            cursor.execute("""
                INSERT INTO backtest_portfolios (
                    portfolio_name,
                    total_capital,
                    account_id,
                    strategy_id,
                    allocation_percentage
                ) VALUES (?, ?, ?, ?, ?)
                RETURNING portfolio_id
            """, (
                config['portfolio_name'],
                config['total_capital'],
                config['account_id'],
                strategy_id,
                strategy['capital_allocation'] * 100  # Convert to percentage
            ))
            
            portfolio_id = cursor.fetchone()[0]
        
        conn.commit()
        print(f"Loaded portfolio {config['portfolio_name']} (ID: {portfolio_id})")
        return portfolio_id
        
    except Exception as e:
        print(f"Error loading portfolio {config_file}: {e}")
        conn.rollback()
        raise
        
    finally:
        conn.close()

def main():
    """Load all portfolio configurations into database."""
    # Get database path
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'kairos.db')
    
    # Load all portfolio configs
    portfolios_dir = os.path.join(os.path.dirname(__file__), 'portfolios')
    for file in os.listdir(portfolios_dir):
        if file.endswith('.json'):
            config_file = os.path.join(portfolios_dir, file)
            load_portfolio_to_db(db_path, config_file)

if __name__ == "__main__":
    main() 