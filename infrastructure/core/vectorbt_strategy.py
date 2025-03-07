import vectorbt as vbt
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from abc import ABC, abstractmethod
import sqlite3
from ..risk_management.risk_manager import RiskManager

class VectorBTStrategy(ABC):
    def __init__(self,
                 db_path: str,
                 symbols: List[str],
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 initial_capital: float = 100000):
        """
        Base class for VectorBT strategies.
        
        Args:
            db_path: Path to SQLite database
            symbols: List of symbols to trade
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            initial_capital: Initial capital for backtesting
        """
        self.db_path = db_path
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        
        # Initialize risk manager with default settings
        self.risk_manager = RiskManager(db_path)
        
        # Load market data
        self.data = self._load_market_data()
        
    def _load_market_data(self) -> Dict[str, pd.DataFrame]:
        """Load market data for all symbols."""
        conn = sqlite3.connect(self.db_path)
        
        data = {}
        date_filter = ""
        params = []
        
        if self.start_date and self.end_date:
            date_filter = "AND date_and_time BETWEEN ? AND ?"
            params.extend([self.start_date, self.end_date])
        
        for symbol in self.symbols:
            query = f"""
            SELECT date_and_time, open, high, low, close, volume
            FROM historical_data_30mins
            WHERE symbol = ?
            {date_filter}
            AND market_session = 'regular'
            ORDER BY date_and_time
            """
            
            symbol_params = [symbol] + params
            df = pd.read_sql_query(query, conn, params=symbol_params)
            df['date_and_time'] = pd.to_datetime(df['date_and_time'])
            df.set_index('date_and_time', inplace=True)
            
            data[symbol] = df
            
        conn.close()
        return data
    
    @abstractmethod
    def generate_signals(self, symbol: str) -> Tuple[pd.Series, pd.Series]:
        """
        Generate entry and exit signals for a symbol.
        
        Args:
            symbol: Symbol to generate signals for
            
        Returns:
            Tuple of (entries, exits) as boolean Series
        """
        pass
    
    def calculate_position_sizes(self, 
                               symbol: str, 
                               entries: pd.Series,
                               price: pd.Series,
                               stop_prices: pd.Series) -> pd.Series:
        """
        Calculate position sizes based on risk per trade.
        
        Args:
            symbol: Symbol being traded
            entries: Boolean series of entry signals
            price: Series of entry prices
            stop_prices: Series of stop prices
            
        Returns:
            Series of position sizes
        """
        sizes = pd.Series(0.0, index=entries.index)
        
        # Only calculate for entry points
        mask = entries & (stop_prices != 0)
        
        # Calculate risk per share
        risk_per_share = abs(price[mask] - stop_prices[mask])
        
        # Calculate position size based on risk
        # Use risk manager to get risk per trade
        risk_per_trade_pct = self.risk_manager.risk_per_trade / 100  # Convert percentage to decimal
        capital_at_risk = self.initial_capital * risk_per_trade_pct
        sizes[mask] = capital_at_risk / risk_per_share
        
        return sizes
    
    def backtest(self, symbol: str) -> vbt.Portfolio:
        """
        Run backtest for a single symbol.
        
        Args:
            symbol: Symbol to backtest
            
        Returns:
            VectorBT Portfolio object with backtest results
        """
        # Get market data
        ohlcv = self.data[symbol]
        
        # Generate signals
        entries, exits = self.generate_signals(symbol)
        
        # Run backtest
        pf = vbt.Portfolio.from_signals(
            close=ohlcv['close'],
            entries=entries,
            exits=exits,
            init_cash=self.initial_capital,
            freq='30min'
        )
        
        return pf
    
    def run_all(self) -> Dict[str, vbt.Portfolio]:
        """
        Run backtest for all symbols.
        
        Returns:
            Dictionary of symbol -> Portfolio results
        """
        results = {}
        
        for symbol in self.symbols:
            print(f"\nBacktesting {symbol}...")
            pf = self.backtest(symbol)
            
            # Get trades data
            trades = pf.trades
            
            # Calculate new metrics
            total_trades = len(trades)
            winning_trades = len(trades[trades.return_ > 0])
            accuracy = winning_trades / total_trades if total_trades > 0 else 0
            
            # Risk metrics
            avg_risk_per_trade = self.risk_manager.risk_per_trade  # This is a percentage
            
            # Return metrics
            avg_win = trades[trades.return_ > 0].return_.mean() if len(trades[trades.return_ > 0]) > 0 else 0
            avg_loss = trades[trades.return_ < 0].return_.mean() if len(trades[trades.return_ < 0]) > 0 else 0
            avg_return = trades.return_.mean() if len(trades) > 0 else 0
            total_return = trades.return_.sum() if len(trades) > 0 else 0
            
            # Time-based metrics
            # Determine the timeframe from the data
            if len(self.data[symbol]) > 1:
                timeframe = pd.Timedelta(self.data[symbol].index[1] - self.data[symbol].index[0])
                if timeframe.days >= 1:
                    timeframe_str = "day"
                    periods_factor = 1
                elif timeframe.seconds // 3600 >= 1:
                    timeframe_str = "hour"
                    periods_factor = 24
                elif timeframe.seconds // 60 >= 1:
                    timeframe_str = "minute"
                    periods_factor = 24 * 60
                else:
                    timeframe_str = "second"
                    periods_factor = 24 * 60 * 60
            else:
                timeframe_str = "unknown"
                periods_factor = 1
            
            # Calculate trading frequency
            if len(self.data[symbol]) > 0:
                total_periods = len(self.data[symbol])
                avg_trades_per_period = total_trades / (total_periods / periods_factor) if total_periods > 0 else 0
            else:
                avg_trades_per_period = 0
            
            # Print refined metrics
            print(f"Total Trades: {total_trades}")
            print(f"Accuracy: {accuracy:.2%}")
            print(f"Avg Risk Per Trade: {avg_risk_per_trade:.2%}")
            print(f"Avg Win: {avg_win:.2%}")
            print(f"Avg Loss: {avg_loss:.2%}")
            print(f"Avg Return: {avg_return:.2%}")
            print(f"Total Return: {total_return:.2%}")
            print(f"Avg Trades per {timeframe_str}: {avg_trades_per_period:.2f}")
            print("-" * 50)
            
            results[symbol] = pf
            
        return results 