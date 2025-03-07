import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Union
import vectorbt as vbt
from datetime import datetime
from ..risk_management.risk_manager import RiskManager

class TradeMetrics:
    @staticmethod
    def is_valid_trade_setup(entry_price: float, stop_price: float) -> bool:
        """
        Validate if the trade setup is valid based on entry and stop prices.
        
        Args:
            entry_price: Entry price of the trade
            stop_price: Stop loss price
            
        Returns:
            bool: True if trade setup is valid, False otherwise
        """
        return abs(entry_price - stop_price) > 0

    @staticmethod
    def calculate_risk_size(risk_manager: RiskManager, account_size: float) -> float:
        """
        Calculate the risk size in dollars.
        
        Args:
            risk_manager: RiskManager instance for risk calculations
            account_size: Total account size
            
        Returns:
            Risk size in dollars
        """
        return risk_manager.calculate_risk_size(account_size)

    @staticmethod
    def calculate_position_size(entry_price: float, stop_price: float,
                              risk_manager: RiskManager, account_size: float) -> int:
        """
        Calculate the position size in number of shares/contracts.
        
        Args:
            entry_price: Entry price of the trade
            stop_price: Stop loss price
            risk_manager: RiskManager instance for risk calculations
            account_size: Total account size
            
        Returns:
            Position size in number of shares/contracts (rounded to nearest integer)
        """
        risk_size_dollars = TradeMetrics.calculate_risk_size(risk_manager, account_size)
        price_risk = abs(entry_price - stop_price)
        return int(round(risk_size_dollars / price_risk))

    @staticmethod
    def calculate_capital_required(entry_price: float, position_size: int) -> float:
        """
        Calculate the capital required for the trade.
        
        Args:
            entry_price: Entry price of the trade
            position_size: Number of shares/contracts
            
        Returns:
            Capital required in dollars
        """
        return float(position_size * entry_price)

    @staticmethod
    def calculate_risk_reward(entry_price: float, exit_price: float, stop_price: float, direction: str) -> float:
        """
        Calculate the realized risk/reward ratio.
        
        Args:
            entry_price: Entry price of the trade
            exit_price: Exit price of the trade
            stop_price: Initial stop loss price
            direction: Trade direction ('long' or 'short')
            
        Returns:
            Realized risk/reward ratio (positive for profitable trades, negative for losing trades)
        """
        if direction == 'long':
            reward = exit_price - entry_price
            risk = entry_price - stop_price
        else:  # short
            reward = entry_price - exit_price
            risk = stop_price - entry_price
            
        return reward / risk

    @staticmethod
    def calculate_percentage_return(risk_per_trade: float, risk_reward: float) -> float:
        """
        Calculate the percentage return of a trade.
        
        Args:
            risk_per_trade: Risk per trade as a percentage
            risk_reward: Realized risk/reward ratio
            
        Returns:
            Percentage return
        """
        return risk_per_trade * risk_reward

    @staticmethod
    def is_winning_trade(risk_reward: float) -> int:
        """
        Determine if the trade is profitable.
        
        Args:
            risk_reward: Realized risk/reward ratio
            
        Returns:
            1 if profitable, 0 if not
        """
        return 1 if risk_reward > 0 else 0

    @staticmethod
    def calculate_trade_duration(entry_time: datetime, exit_time: datetime) -> int:
        """
        Calculate the duration of a trade in minutes.
        
        Args:
            entry_time: Entry timestamp
            exit_time: Exit timestamp
            
        Returns:
            Trade duration in minutes
        """
        duration = exit_time - entry_time
        return int(duration.total_seconds() / 60)

    @staticmethod
    def process_single_trade(trade: pd.Series, risk_manager: RiskManager, account_size: float) -> Optional[Dict]:
        """
        Process a single trade and calculate all metrics.
        
        Args:
            trade: Series containing trade data from VectorBT
            risk_manager: RiskManager instance for risk calculations
            account_size: Account size for position sizing
            
        Returns:
            Dictionary with processed trade metrics or None if trade is invalid
        """
        # Validate trade setup
        if not TradeMetrics.is_valid_trade_setup(trade['Entry Price'], trade['Stop Price']):
            return None
            
        # Determine direction (VectorBT uses negative Size for short trades)
        direction = 'long' if trade['Size'] > 0 else 'short'
        
        # Calculate risk/reward first as other metrics depend on it
        risk_reward = TradeMetrics.calculate_risk_reward(
            trade['Entry Price'],
            trade['Exit Price'],
            trade['Stop Price'],
            direction
        )
        
        # Calculate position size and related metrics
        position_size = TradeMetrics.calculate_position_size(
            trade['Entry Price'],
            trade['Stop Price'],
            risk_manager,
            account_size
        )
        
        # Calculate all required metrics
        risk_size = TradeMetrics.calculate_risk_size(risk_manager, account_size)
        capital_required = TradeMetrics.calculate_capital_required(trade['Entry Price'], position_size)
        perc_return = TradeMetrics.calculate_percentage_return(risk_manager.risk_per_trade, risk_reward)
        winning_trade = TradeMetrics.is_winning_trade(risk_reward)
        duration = TradeMetrics.calculate_trade_duration(
            pd.to_datetime(trade['Entry Timestamp']),
            pd.to_datetime(trade['Exit Timestamp'])
        )
        
        return {
            'symbol': trade['Column'],
            'entry_time': pd.to_datetime(trade['Entry Timestamp']),
            'exit_time': pd.to_datetime(trade['Exit Timestamp']),
            'entry_price': trade['Entry Price'],
            'exit_price': trade['Exit Price'],
            'stop_price': trade['Stop Price'],
            'position_size': position_size,
            'risk_size': risk_size,
            'risk_per_trade': risk_manager.risk_per_trade,
            'risk_reward': risk_reward,
            'perc_return': perc_return,
            'winning_trade': winning_trade,
            'trade_duration': duration,
            'capital_required': capital_required,
            'direction': direction
        }

    @staticmethod
    def process_vectorbt_trades(portfolio: vbt.Portfolio, risk_manager: RiskManager) -> pd.DataFrame:
        """
        Process VectorBT trade records into our format.
        
        Args:
            portfolio: VectorBT Portfolio object
            risk_manager: RiskManager instance for risk calculations
            
        Returns:
            DataFrame with processed trade metrics
        """
        trades = portfolio.trades.records_readable
        account_size = portfolio.init_cash[0]
        
        processed_trades = []
        for _, trade in trades.iterrows():
            trade_metrics = TradeMetrics.process_single_trade(trade, risk_manager, account_size)
            if trade_metrics is not None:
                processed_trades.append(trade_metrics)
            
        return pd.DataFrame(processed_trades)

    @staticmethod
    def save_trades_to_db(trades_df: pd.DataFrame, db_path: str) -> None:
        """
        Save processed trades to the database.
        
        Args:
            trades_df: DataFrame of processed trades
            db_path: Path to the SQLite database
        """
        import sqlite3
        
        conn = sqlite3.connect(db_path)
        trades_df.to_sql('algo_trades', conn, if_exists='append', index=False)
        conn.close() 