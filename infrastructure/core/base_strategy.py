import sqlite3
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from abc import ABC, abstractmethod
from ..capital_management.risk_manager import RiskManager

class BaseStrategy(ABC):
    def __init__(self, 
                 db_path: str,
                 symbols: List[str],
                 start_date: str,
                 end_date: str,
                 initial_capital: float = 100000.0,
                 risk_per_trade_pct: float = 0.5,
                 max_daily_risk_pct: float = 1.0,
                 variant: str = "default"):
        self.db_path = db_path
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.risk_per_trade_pct = risk_per_trade_pct
        self.variant = variant
        
        # Initialize risk manager
        self.risk_manager = RiskManager(
            db_path=db_path,
            initial_capital=initial_capital,
            max_daily_risk_pct=max_daily_risk_pct
        )
        
        # Strategy metadata
        self.strategy_name = self.get_strategy_name()
        self.strategy_version = self.get_strategy_version()
        
        # Performance tracking
        self.trades = []
        
        # Execution date
        self.execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Initialize active trades
        self.active_trades: Dict[str, List] = {symbol: [] for symbol in symbols}

    @abstractmethod
    def get_strategy_name(self) -> str:
        """Return the name of the strategy"""
        pass

    @abstractmethod
    def get_strategy_version(self) -> str:
        """Return the version of the strategy"""
        pass

    def load_data(self, table_name: str, additional_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Load and prepare data from database"""
        conn = sqlite3.connect(self.db_path)
        
        # Base columns that are always needed
        base_columns = ["date_and_time", "symbol", "open", "high", "low", "close", "volume"]
        
        # Add any additional columns if specified
        if additional_columns:
            columns = base_columns + additional_columns
        else:
            columns = base_columns
            
        query = f"""
        SELECT {', '.join(columns)}
        FROM {table_name}
        WHERE symbol IN ({','.join(['?'] * len(self.symbols))})
        AND date_and_time BETWEEN ? AND ?
        AND market_session = 'regular'
        ORDER BY date_and_time, symbol
        """
        
        params = self.symbols + [self.start_date, self.end_date]
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Convert date_and_time to datetime
        df['date_and_time'] = pd.to_datetime(df['date_and_time'])
        return df

    @abstractmethod
    def calculate_entry_signals(self, df: pd.DataFrame, idx: int, row: pd.Series) -> bool:
        """Calculate entry signals for the strategy"""
        pass

    @abstractmethod
    def calculate_exit_signals(self, df: pd.DataFrame, idx: int, row: pd.Series, trade) -> bool:
        """Calculate exit signals for the strategy"""
        pass

    def run_backtest(self, table_name: str, additional_columns: Optional[List[str]] = None):
        """Run the backtest"""
        print("Loading data...")
        df = self.load_data(table_name, additional_columns)
        
        print("Running backtest...")
        # Group by date to identify end-of-day candles
        grouped_df = df.groupby([df['date_and_time'].dt.date, 'symbol'])
        
        for idx, row in df.iterrows():
            symbol = row['symbol']
            date = row['date_and_time'].date().isoformat()
            
            # Check if this is the last candle of the day for this symbol
            is_last_candle = False
            try:
                last_idx = grouped_df.get_group((row['date_and_time'].date(), symbol)).index[-1]
                is_last_candle = (idx == last_idx)
            except (KeyError, IndexError):
                is_last_candle = False
            
            # Check entry conditions if no active trade and we're allowed to trade
            if not self.active_trades[symbol] and self.risk_manager.can_take_trade(date):
                if self.calculate_entry_signals(df, idx, row):
                    self._handle_entry(row, symbol)
            
            # Check exit conditions for active trades
            if self.active_trades[symbol]:
                for trade in self.active_trades[symbol]:
                    if self.calculate_exit_signals(df, idx, row, trade) or is_last_candle:
                        self._handle_exit(row, symbol, trade)

    @abstractmethod
    def _handle_entry(self, row: pd.Series, symbol: str):
        """Handle trade entry logic"""
        pass

    @abstractmethod
    def _handle_exit(self, row: pd.Series, symbol: str, trade):
        """Handle trade exit logic"""
        pass

    def _update_capital(self, new_capital: float, trade_exit_reason: str = ""):
        """Update capital through risk manager"""
        self.risk_manager.update_capital(new_capital, trade_exit_reason)
        self.current_capital = new_capital 