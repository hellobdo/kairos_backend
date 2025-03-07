import sqlite3
from datetime import datetime
from typing import Optional

class RiskManager:
    def __init__(self, 
                 db_path: str,
                 initial_capital: float,
                 max_daily_risk_pct: float = 1.0):
        self.db_path = db_path
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.max_daily_risk_pct = max_daily_risk_pct
    
    def get_daily_return(self, date: str) -> float:
        """Get total percentage return for the day from closed trades"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get sum of percentage returns for the day
        query = """
        SELECT SUM(perc_return)
        FROM algo_trades
        WHERE exit_date = ?
        """
        
        cursor.execute(query, (date,))
        total_return = cursor.fetchone()[0] or 0  # Use 0 if no trades found
        conn.close()
        
        return total_return

    def get_open_trades_risk(self, date: str) -> float:
        """Get total risk percentage from open trades"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get risk per trade for open trades (trades with entry date but no exit date)
        query = """
        SELECT SUM(risk_per_trade)
        FROM algo_trades
        WHERE entry_date = ? AND exit_date IS NULL
        """
        
        cursor.execute(query, (date,))
        total_risk = cursor.fetchone()[0] or 0  # Use 0 if no open trades
        conn.close()
        
        return total_risk
    
    def get_total_potential_risk(self, date: str) -> float:
        """Get total potential risk including realized losses and open trade risks"""
        realized_return = self.get_daily_return(date)
        open_trades_risk = self.get_open_trades_risk(date)
        
        # Sum realized returns (positive or negative) with open trade risks
        return realized_return + open_trades_risk
    
    def can_take_trade(self, date: str) -> bool:
        """Check if a new trade is allowed based on risk rules"""
        # Get total potential risk
        total_risk = self.get_total_potential_risk(date)
        
        # No more trades if total potential risk is <= -1%
        return total_risk > -self.max_daily_risk_pct 