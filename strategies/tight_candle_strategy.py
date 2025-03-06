import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import os
from typing import Dict, List, Tuple, Optional
import re
import sys

# Add parent directory to path so we can import modules from there
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from trade_metrics import (
    calculate_risk_size, 
    calculate_risk_percentage,
    calculate_position_size, 
    calculate_stop_price, 
    calculate_risk_reward_ratio,
    determine_winning_trade,
    calculate_trade_return,
    calculate_trade_duration,
    calculate_capital_required,
    DIRECTION_BULLISH,
    DIRECTION_BEARISH
)

# Strategy metadata
STRATEGY_NAME = "tight_candle"

# Default strategy version
DEFAULT_STRATEGY_VERSION = "1.0.0"
STRATEGY_VERSION = DEFAULT_STRATEGY_VERSION  # Default to 1.0.0 instead of date-based version

def prompt_for_version_update():
    """Ask the user if they want to update the strategy version and validate the format."""
    global STRATEGY_VERSION
    while True:
        update_version = input(f"Current strategy version is {STRATEGY_VERSION}. Would you like to update it? (y/n): ").lower()
        if update_version not in ['y', 'yes']:
            print(f"Keeping version {STRATEGY_VERSION}")
            break
        
        new_version = input("Enter new version (x.x.x format): ")
        # Validate the version format using regex
        if re.match(r'^\d+\.\d+\.\d+$', new_version):
            STRATEGY_VERSION = new_version
            print(f"Version updated to {STRATEGY_VERSION}")
            break
        else:
            print("Invalid version format. Please use x.x.x format where x is a number.")

class Trade:
    def __init__(self, 
                 symbol: str,
                 direction: str,
                 entry_date: str,
                 entry_timestamp: str,
                 entry_price: float,
                 stop_price: float,
                 quantity: int,
                 account_size: float,
                 variant: str):
        self.symbol = symbol
        self.direction = direction
        self.entry_date = entry_date
        self.entry_timestamp = entry_timestamp
        self.entry_price = entry_price
        self.stop_price = stop_price
        self.quantity = quantity
        self.variant = variant
        self.account_size = account_size
        
        # Calculate risk metrics using trade_metrics functions
        self.risk_size = calculate_risk_size(entry_price, stop_price, quantity)
        self.capital_required = calculate_capital_required(entry_price, quantity)
        self.risk_per_trade = calculate_risk_percentage(entry_price, stop_price, quantity, account_size)
        
        # Fields to be populated at exit
        self.exit_price: Optional[float] = None
        self.exit_date: Optional[str] = None
        self.exit_timestamp: Optional[str] = None
        self.trade_duration: Optional[float] = None
        self.winning_trade: Optional[int] = None
        self.risk_reward: Optional[float] = None
        self.perc_return: Optional[float] = None
    
    def set_exit(self, exit_price: float, exit_date: str, exit_timestamp: str):
        self.exit_price = exit_price
        self.exit_date = exit_date
        self.exit_timestamp = exit_timestamp
        
        # Calculate all trade metrics at once
        self.trade_duration = calculate_trade_duration(self.entry_timestamp, exit_timestamp)
        self.risk_reward = calculate_risk_reward_ratio(self.entry_price, exit_price, self.stop_price, self.direction)
        self.winning_trade = determine_winning_trade(self.entry_price, exit_price, self.stop_price, self.direction)
        self.perc_return = calculate_trade_return(self.risk_per_trade, self.risk_reward)

class TightCandleStrategy:
    def __init__(self, 
                 db_path: str,
                 symbols: List[str],
                 start_date: str,
                 end_date: str,
                 # Strategy parameters
                 tightness_threshold: str = 'Ultra Tight',
                 min_volume: float = 100000,
                 stop_loss_amount: float = 0.5,  # Dollar amount for stop loss
                 take_profit_pct: float = 1.0,
                 target_risk_reward: float = 2.0,
                 # Additional filters
                 min_price: float = 0.0,
                 max_price: float = float('inf'),
                 min_prev_candles: int = 3,
                 trend_threshold: float = 0.3,
                 # Portfolio parameters
                 initial_capital: float = 100000.0,
                 risk_per_trade_pct: float = 0.5,  # Maximum risk per trade as percentage of account (0.50%)
                 # Variant identifier
                 variant: str = "default"
                 ):
        self.db_path = db_path
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        
        # Strategy parameters
        self.tightness_threshold = tightness_threshold
        self.min_volume = min_volume
        self.stop_loss_amount = stop_loss_amount  # Dollar amount for stop
        self.take_profit_pct = take_profit_pct
        self.target_risk_reward = target_risk_reward
        
        # Additional filters
        self.min_price = min_price
        self.max_price = max_price
        self.min_prev_candles = min_prev_candles
        self.trend_threshold = trend_threshold
        
        # Portfolio parameters
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.risk_per_trade_pct = risk_per_trade_pct  # Keep as percentage value
        
        # Variant identifier
        self.variant = variant
        
        # Strategy metadata
        self.strategy_name = STRATEGY_NAME
        self.strategy_version = STRATEGY_VERSION
        
        # Performance tracking
        self.trades = []
        
        # Execution date (timestamp when strategy is run)
        self.execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Initialize results
        self.active_trades: Dict[str, List[Trade]] = {symbol: [] for symbol in symbols}
    
    def load_data(self) -> pd.DataFrame:
        """Load and prepare data from database"""
        conn = sqlite3.connect(self.db_path)
        query = f"""
        SELECT date_and_time, symbol, open, high, low, close, volume, 
               diff_pct_open_close, tightness
        FROM historical_data_30mins
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
    
    def calculate_trend(self, df: pd.DataFrame, symbol: str, current_idx: int) -> Tuple[float, str]:
        """Calculate the trend value and direction"""
        # Filter data for this symbol up to the current index
        symbol_data = df[(df['symbol'] == symbol) & (df.index < current_idx)]
        
        if len(symbol_data) >= self.min_prev_candles:
            # Use the last n candles
            recent_data = symbol_data.tail(self.min_prev_candles)
            
            # Calculate trend value: percentage change from first to last close
            first_close = recent_data.iloc[0]['close']
            last_close = recent_data.iloc[-1]['close']
            trend_value = ((last_close - first_close) / first_close) * 100
            
            # Determine trend direction
            if trend_value > self.trend_threshold:
                return trend_value, DIRECTION_BULLISH
            elif trend_value < -self.trend_threshold:
                return trend_value, DIRECTION_BEARISH
        
        # Default: no clear trend
        return 0.0, 'Neutral'
    
    def calculate_take_profit(self, entry_price: float, stop_price: float, direction: str) -> float:
        """Calculate take profit based on target risk/reward ratio"""
        price_difference = abs(entry_price - stop_price)
        if direction == DIRECTION_BULLISH:
            return entry_price + (price_difference * self.target_risk_reward)
        else:
            return entry_price - (price_difference * self.target_risk_reward)
    
    def run_backtest(self):
        """Run the backtest"""
        print("Loading data...")
        df = self.load_data()
        
        print("Running backtest...")
        # Group by date to identify end-of-day candles
        grouped_df = df.groupby([df['date_and_time'].dt.date, 'symbol'])
        
        for idx, row in df.iterrows():
            symbol = row['symbol']
            current_time = row['date_and_time']
            current_date = current_time.date()
            
            # Check if this is the last candle of the day for this symbol
            is_last_candle = False
            try:
                last_idx = grouped_df.get_group((current_date, symbol)).index[-1]
                is_last_candle = (idx == last_idx)
            except (KeyError, IndexError):
                is_last_candle = False
            
            # Calculate trend direction
            trend_value, trend_direction = self.calculate_trend(df, symbol, idx)
            
            # Entry conditions
            if row['tightness'] == self.tightness_threshold:
                # Calculate stop price using delta amount
                stop_price = calculate_stop_price(row['close'], trend_direction, self.stop_loss_amount)
                
                # Calculate max risk amount for this trade
                max_risk_amount = self.current_capital * (self.risk_per_trade_pct / 100)
                
                # Calculate position size based on entry, stop, and max risk
                quantity = calculate_position_size(row['close'], stop_price, max_risk_amount)
                
                # Create new trade
                trade = Trade(
                    symbol=symbol,
                    direction=trend_direction,
                    entry_date=current_time.strftime('%Y-%m-%d'),
                    entry_timestamp=current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    entry_price=row['close'],
                    stop_price=stop_price,
                    quantity=quantity,
                    account_size=self.current_capital,
                    variant=self.variant
                )
                
                self.active_trades[symbol].append(trade)
                self.trades.append(trade)
            
            # Check for exits - either due to stop loss, take profit, or end of session
            for trade in self.active_trades[symbol][:]:
                exit_triggered = False
                exit_price = row['close']
                exit_reason = "End of session"  # Default reason
                
                if trade.direction == DIRECTION_BULLISH:
                    if row['low'] <= trade.stop_price:  # Stop loss
                        exit_triggered = True
                        exit_price = trade.stop_price
                        exit_reason = "Stop loss"
                    elif row['high'] >= self.calculate_take_profit(trade.entry_price, trade.stop_price, trade.direction):
                        exit_triggered = True
                        exit_price = self.calculate_take_profit(trade.entry_price, trade.stop_price, trade.direction)
                        exit_reason = "Take profit"
                else:  # Bearish
                    if row['high'] >= trade.stop_price:  # Stop loss
                        exit_triggered = True
                        exit_price = trade.stop_price
                        exit_reason = "Stop loss"
                    elif row['low'] <= self.calculate_take_profit(trade.entry_price, trade.stop_price, trade.direction):
                        exit_triggered = True
                        exit_price = self.calculate_take_profit(trade.entry_price, trade.stop_price, trade.direction)
                        exit_reason = "Take profit"
                
                # Close trade at end of session if it hasn't been closed by other conditions
                if not exit_triggered and is_last_candle:
                    exit_triggered = True
                    exit_price = row['close']
                    exit_reason = "End of session"
                
                if exit_triggered:
                    print(f"Closing trade {trade.symbol} at {exit_price} - Reason: {exit_reason}")
                    trade.set_exit(
                        exit_price=exit_price,
                        exit_date=current_time.strftime('%Y-%m-%d'),
                        exit_timestamp=current_time.strftime('%Y-%m-%d %H:%M:%S')
                    )
                    self.active_trades[symbol].remove(trade)
                    
                    # Update account capital
                    self.current_capital += trade.quantity * (exit_price - trade.entry_price) if trade.direction == DIRECTION_BULLISH else trade.quantity * (trade.entry_price - exit_price)
    
    def verify_algo_trades_table(self):
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
    
    def save_trades(self):
        """Save trades to the algo_trades table"""
        if not self.trades:
            print("No trades to save")
            return
        
        # First, verify that the algo_trades table exists
        if not self.verify_algo_trades_table():
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for trade in self.trades:
            cursor.execute("""
                INSERT INTO algo_trades (
                    account_id, symbol, strategy, strategy_version, variant, direction, 
                    entry_date, entry_timestamp, instrument_type, quantity, 
                    entry_price, stop_price, exit_price, exit_date, exit_timestamp, 
                    capital_required, trade_duration, winning_trade, risk_reward, 
                    risk_per_trade, perc_return, risk_size, execution_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                'ZZZ',  # account_id (using the backtesting account)
                trade.symbol,
                self.strategy_name,
                self.strategy_version,
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
                self.execution_date  # Add execution_date
            ))
        
        conn.commit()
        conn.close()
        print(f"Saved {len(self.trades)} trades to the 'algo_trades' table")
        print(f"Strategy: {self.strategy_name}, Version: {self.strategy_version}, Variant: {self.variant}")
        print(f"Execution date: {self.execution_date}")

def main():
    # Prompt for version update
    prompt_for_version_update()
    
    # Example usage
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'kairos.db')
    symbols = ['QQQ', 'TSLA', 'META', 'NVDA', 'PLTR', 'HOOD']
    start_date = '2023-04-19'
    end_date = '2024-03-19'
    
    # Create strategy instance with risk_reward 2.0
    strategy_v1 = TightCandleStrategy(
        db_path=db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        tightness_threshold='Ultra Tight',
        min_volume=100000,
        stop_loss_amount=0.5,  # Dollar amount for stop
        take_profit_pct=1.0,
        target_risk_reward=2.0,
        min_price=0.0,
        max_price=float('inf'),
        min_prev_candles=3,
        trend_threshold=0.3,
        initial_capital=100000.0,
        risk_per_trade_pct=0.5,  # 0.5% risk per trade
        variant="RR_2.0"
    )
    
    # Run backtest
    print("Starting backtest for variant RR_2.0...")
    strategy_v1.run_backtest()
    
    # Save trades to database
    strategy_v1.save_trades()
    
    # Create strategy instance with risk_reward 3.0
    strategy_v2 = TightCandleStrategy(
        db_path=db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        tightness_threshold='Ultra Tight',
        min_volume=100000,
        stop_loss_amount=0.5,  # Dollar amount for stop
        take_profit_pct=1.0,
        target_risk_reward=3.0,
        min_price=0.0,
        max_price=float('inf'),
        min_prev_candles=3,
        trend_threshold=0.3,
        initial_capital=100000.0,
        risk_per_trade_pct=0.5,  # 0.5% risk per trade
        variant="RR_3.0"
    )
    
    # Run backtest
    print("\nStarting backtest for variant RR_3.0...")
    strategy_v2.run_backtest()
    
    # Save trades to database
    strategy_v2.save_trades()

if __name__ == "__main__":
    main() 