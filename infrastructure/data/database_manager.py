import sqlite3
from typing import List
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def verify_algo_trades_table(self) -> bool:
        """Verify that the algo_trades table exists and has the required columns"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='algo_trades'")
        if not cursor.fetchone():
            print("Error: The 'algo_trades' table does not exist. Please run setup_database.py first.")
            conn.close()
            return False
        
        # Check if the execution_date column exists
        cursor.execute("PRAGMA table_info(algo_trades)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'execution_date' not in columns:
            print("Warning: The 'execution_date' column does not exist in the 'algo_trades' table.")
            print("Please run update_algo_trades.py to add this column.")
            conn.close()
            return False
        
        conn.close()
        return True

    def save_trades(self, trades: List, strategy_name: str, strategy_version: str, variant: str, execution_date: str):
        """Save trades to the algo_trades table"""
        if not trades:
            print("No trades to save")
            return
        
        # First, verify that the algo_trades table exists
        if not self.verify_algo_trades_table():
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for trade in trades:
            cursor.execute("""
                INSERT INTO algo_trades (
                    account_id, symbol, strategy, strategy_version, variant, direction, 
                    entry_date, entry_timestamp, instrument_type, quantity, 
                    entry_price, stop_price, exit_price, exit_date, exit_timestamp, 
                    capital_required, trade_duration, winning_trade, risk_reward, 
                    risk_per_trade, perc_return, risk_size, execution_date, exit_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                'ZZZ',  # account_id (using the backtesting account)
                trade.symbol,
                strategy_name,
                strategy_version,
                trade.variant,
                trade.direction,
                trade.entry_date,
                trade.entry_timestamp,
                'STOCK',  # instrument_type
                trade.quantity,
                trade.entry_price,
                trade.stop_price,
                trade.exit_price,
                trade.exit_date,
                trade.exit_timestamp,
                trade.capital_required,
                trade.trade_duration,
                trade.winning_trade,
                trade.risk_reward,
                trade.risk_per_trade,
                trade.perc_return,
                trade.risk_size,
                execution_date,
                trade.exit_reason
            ))
        
        conn.commit()
        conn.close()
        print(f"Saved {len(trades)} trades to the 'algo_trades' table")
        print(f"Strategy: {strategy_name}, Version: {strategy_version}, Variant: {variant}")
        print(f"Execution date: {execution_date}") 