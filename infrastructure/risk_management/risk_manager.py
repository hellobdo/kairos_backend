import sqlite3
import pandas as pd
from typing import Dict, Optional

class RiskManager:
    # Default risk settings
    DEFAULT_RISK_PER_TRADE = 0.5  # 0.5% risk per trade
    DEFAULT_MAX_DAILY_RISK = -1.0  # -1% max daily risk

    def __init__(self, 
                 db_path: str, 
                 risk_per_trade: float = DEFAULT_RISK_PER_TRADE,
                 max_daily_risk: float = DEFAULT_MAX_DAILY_RISK):
        """
        Initialize the RiskManager.
        
        Args:
            db_path: Path to the SQLite database
            risk_per_trade: Risk per trade as percentage (default 0.5%)
            max_daily_risk: Maximum allowed daily risk percentage (default -1%)
        """
        self.db_path = db_path
        self.risk_per_trade = risk_per_trade
        self.max_daily_risk = max_daily_risk

    def calculate_risk_size(self, account_size: float) -> float:
        """
        Calculate the risk size in dollars.
        
        Args:
            account_size: Current account size in dollars
            
        Returns:
            Risk size in dollars
        """
        return (self.risk_per_trade / 100) * account_size

    def get_daily_realized_return(self, date: str) -> float:
        """
        Get the total realized return for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            Total realized return as a percentage
        """
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT SUM(perc_return) as total_perc_return
        FROM algo_trades
        WHERE DATE(entry_time) = DATE(?)
        AND exit_time IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn, params=[date])
        conn.close()
        
        return float(df['total_perc_return'].iloc[0] or 0)

    def get_open_trades_risk(self, date: str) -> float:
        """
        Calculate total risk from open trades for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            Total potential risk as a percentage
        """
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT SUM(risk_per_trade) as total_risk
        FROM algo_trades
        WHERE DATE(entry_time) = DATE(?)
        AND exit_time IS NULL
        """
        
        df = pd.read_sql_query(query, conn, params=[date])
        conn.close()
        
        return float(df['total_risk'].iloc[0] or 0)

    def get_total_potential_risk(self, date: str) -> float:
        """
        Calculate total potential risk including realized losses and open trade risks.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            Total potential risk as a percentage
        """
        realized_return = self.get_daily_realized_return(date)
        open_trades_risk = self.get_open_trades_risk(date)
        
        return realized_return + open_trades_risk

    def can_take_trade(self, date: str) -> bool:
        """
        Check if a new trade can be taken based on daily risk limits.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            Boolean indicating whether the trade can be taken
        """
        current_risk = self.get_total_potential_risk(date)
        potential_total_risk = current_risk + self.risk_per_trade
        
        return potential_total_risk > self.max_daily_risk

    def validate_vectorbt_entries(self, entries: pd.DataFrame, account_size: float) -> pd.DataFrame:
        """
        Validate entry signals from VectorBT based on daily risk limits.
        
        Args:
            entries: DataFrame of entry signals from VectorBT
            account_size: Current account size in dollars
            
        Returns:
            Modified entry signals DataFrame
        """
        validated_entries = entries.copy()
        
        for date in entries.index:
            date_str = date.strftime('%Y-%m-%d')
            if not self.can_take_trade(date_str):
                validated_entries.loc[date, :] = False
        
        return validated_entries 