import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Union
import vectorbt as vbt
from datetime import datetime

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
            
        return reward / risk if risk != 0 else 0.0

    @staticmethod
    def calculate_percentage_return(entry_price: float, exit_price: float) -> float:
        """
        Calculate the percentage return of a trade.
        
        Args:
            entry_price: Entry price of the trade
            exit_price: Exit price of the trade
            
        Returns:
            Percentage return
        """
        return ((exit_price - entry_price) / entry_price) * 100

    @staticmethod
    def is_winning_trade(entry_price: float, exit_price: float, direction: str) -> int:
        """
        Determine if the trade is profitable.
        
        Args:
            entry_price: Entry price of the trade
            exit_price: Exit price of the trade
            direction: Trade direction ('long' or 'short')
            
        Returns:
            1 if profitable, 0 if not
        """
        if direction == 'long':
            return 1 if exit_price > entry_price else 0
        else:  # short
            return 1 if exit_price < entry_price else 0

    @staticmethod
    def calculate_trade_duration(entry_timestamp: datetime, exit_timestamp: datetime) -> int:
        """
        Calculate the duration of a trade in minutes.
        
        Args:
            entry_timestamp: Entry timestamp
            exit_timestamp: Exit timestamp
            
        Returns:
            Trade duration in minutes
        """
        duration = exit_timestamp - entry_timestamp
        return int(duration.total_seconds() / 60)

    @staticmethod
    def process_single_trade(trade: pd.Series, risk_config: Dict, account_size: float) -> Optional[Dict]:
        """
        Process a single trade and calculate all metrics.
        
        Args:
            trade: Series containing trade data from VectorBT
            risk_config: Dictionary containing risk management configuration
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
        
        # Calculate metrics
        perc_return = TradeMetrics.calculate_percentage_return(trade['Entry Price'], trade['Exit Price'])
        winning_trade = TradeMetrics.is_winning_trade(trade['Entry Price'], trade['Exit Price'], direction)
        duration = TradeMetrics.calculate_trade_duration(
            pd.to_datetime(trade['Entry Timestamp']),
            pd.to_datetime(trade['Exit Timestamp'])
        )
        
        return {
            'symbol': trade['Column'],
            'entry_timestamp': pd.to_datetime(trade['Entry Timestamp']),
            'exit_timestamp': pd.to_datetime(trade['Exit Timestamp']),
            'entry_price': trade['Entry Price'],
            'exit_price': trade['Exit Price'],
            'stop_price': trade['Stop Price'],
            'position_size': abs(trade['Size']),
            'risk_per_trade': risk_per_trade,
            'risk_reward': risk_reward,
            'perc_return': perc_return,
            'winning_trade': winning_trade,
            'trade_duration': duration,
            'capital_required': trade['Capital Required'],
            'direction': direction
        }

    @staticmethod
    def process_vectorbt_trades(portfolio: vbt.Portfolio, risk_config: Dict) -> pd.DataFrame:
        """
        Process VectorBT trade records into our format.
        
        Args:
            portfolio: VectorBT Portfolio object
            risk_config: Dictionary containing risk management configuration
            
        Returns:
            DataFrame with processed trade metrics
        """
        trades = portfolio.trades.records_readable
        account_size = portfolio.init_cash[0]
        
        # Calculate stop prices based on risk per trade
        risk_per_trade_pct = risk_config['risk_per_trade'] / 100  # Convert to decimal
        risk_per_trade_dollars = account_size * risk_per_trade_pct
        
        # Add stop prices to trades
        trades['Stop Price'] = trades.apply(
            lambda row: row['Entry Price'] - risk_per_trade_dollars/abs(row['Size'])
            if row['Size'] > 0 else
            row['Entry Price'] + risk_per_trade_dollars/abs(row['Size']),
            axis=1
        )
        
        processed_trades = []
        for _, trade in trades.iterrows():
            trade_metrics = TradeMetrics.process_single_trade(trade, risk_config, account_size)
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
        
        # Ensure all required columns are present
        required_columns = [
            'symbol', 'entry_timestamp', 'exit_timestamp', 'entry_price',
            'exit_price', 'stop_price', 'position_size',
            'risk_per_trade', 'risk_reward', 'perc_return', 'winning_trade',
            'trade_duration', 'capital_required', 'direction', 'strategy_id',
            'run_id', 'instrument_type'
        ]
        
        missing_columns = set(required_columns) - set(trades_df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        conn = sqlite3.connect(db_path)
        trades_df.to_sql('algo_trades', conn, if_exists='append', index=False)
        conn.close()

    @staticmethod
    def calculate_closing_metrics(trade: Dict) -> Dict:
        """
        Calculate all metrics for a closed trade.
        
        Args:
            trade: Dictionary containing trade data with:
                - entry_price: Entry price
                - exit_price: Exit price
                - entry_timestamp: Entry timestamp
                - exit_timestamp: Exit timestamp
                - stop_price: Initial stop price
                - direction: Trade direction ('long' or 'short')
            
        Returns:
            Dictionary with closing metrics:
                - winning_trade: 1 if profitable, 0 if not
                - trade_duration: Duration in minutes
                - perc_return: Percentage return
                - risk_reward: Realized risk/reward ratio
        """
        # Convert timestamps if they're strings
        entry_timestamp = pd.to_datetime(trade['entry_timestamp'])
        exit_timestamp = pd.to_datetime(trade['exit_timestamp'])
        
        # Calculate all closing metrics
        risk_reward = TradeMetrics.calculate_risk_reward(
            trade['entry_price'],
            trade['exit_price'],
            trade['stop_price'],
            trade['direction']
        )
        
        perc_return = TradeMetrics.calculate_percentage_return(
            trade['entry_price'],
            trade['exit_price']
        )
        
        winning_trade = TradeMetrics.is_winning_trade(
            trade['entry_price'],
            trade['exit_price'],
            trade['direction']
        )
        
        trade_duration = TradeMetrics.calculate_trade_duration(
            entry_timestamp,
            exit_timestamp
        )
        
        return {
            'winning_trade': winning_trade,
            'trade_duration': trade_duration,
            'perc_return': perc_return,
            'risk_reward': risk_reward
        } 