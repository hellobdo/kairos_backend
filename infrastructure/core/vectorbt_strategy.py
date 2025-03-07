import vectorbt as vbt
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from abc import ABC, abstractmethod
import sqlite3

class VectorBTStrategy(ABC):
    def __init__(self,
                 db_path: str,
                 symbols: List[str],
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 initial_capital: float = 100000,
                 risk_per_trade_pct: float = 0.01):
        """
        Base class for VectorBT strategies.
        
        Args:
            db_path: Path to SQLite database
            symbols: List of symbols to trade
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            initial_capital: Initial capital for backtesting
            risk_per_trade_pct: Risk per trade as percentage of capital
        """
        self.db_path = db_path
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.risk_per_trade_pct = risk_per_trade_pct
        
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
        capital_at_risk = self.initial_capital * self.risk_per_trade_pct
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
            
            # Print summary statistics
            print(f"Total Return: {pf.total_return:.2%}")
            print(f"Sharpe Ratio: {pf.sharpe_ratio:.2f}")
            print(f"Max Drawdown: {pf.max_drawdown:.2%}")
            print(f"# Trades: {len(pf.trades)}")
            print("-" * 50)
            
            results[symbol] = pf
            
        return results 