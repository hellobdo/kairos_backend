import pandas as pd
from datetime import datetime
import os
from typing import Dict, List, Tuple
import sys

# Add parent directory to path so we can import modules from there
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from infrastructure.reports_and_metrics.trade_metrics import (
    calculate_position_size, 
    calculate_stop_price, 
    DIRECTION_BULLISH,
    DIRECTION_BEARISH
)

from infrastructure.core.base_strategy import BaseStrategy
from infrastructure.core.trade import Trade
from infrastructure.data.database_manager import DatabaseManager

class TightCandleStrategy(BaseStrategy):
    def __init__(self, 
                 db_path: str,
                 symbols: list,
                 start_date: str,
                 end_date: str,
                 # Strategy parameters
                 tightness_threshold: str = 'Ultra Tight',
                 min_volume: float = 100000,
                 stop_loss_amount: float = 0.5,
                 target_risk_reward: float = 2.0,
                 # Additional filters
                 min_price: float = 0.0,
                 max_price: float = float('inf'),
                 min_prev_candles: int = 3,
                 trend_threshold: float = 0.3,
                 # Portfolio parameters
                 initial_capital: float = 100000.0,
                 risk_per_trade_pct: float = 0.5,
                 variant: str = "default"):
        
        super().__init__(
            db_path=db_path,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            risk_per_trade_pct=risk_per_trade_pct,
            variant=variant
        )
        
        # Strategy parameters
        self.tightness_threshold = tightness_threshold
        self.min_volume = min_volume
        self.stop_loss_amount = stop_loss_amount
        self.target_risk_reward = target_risk_reward
        
        # Additional filters
        self.min_price = min_price
        self.max_price = max_price
        self.min_prev_candles = min_prev_candles
        self.trend_threshold = trend_threshold

    def get_strategy_name(self) -> str:
        return "tight_candle"

    def get_strategy_version(self) -> str:
        return "1.0.0"

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

    def calculate_entry_signals(self, df: pd.DataFrame, idx: int, row: pd.Series) -> bool:
        """Calculate entry signals for the strategy"""
        # Price filter
        if not (self.min_price <= row['close'] <= self.max_price):
            return False
            
        # Volume filter
        if row['volume'] < self.min_volume:
            return False
            
        # Tightness filter
        if row['tightness'] != self.tightness_threshold:
            return False
            
        # Calculate trend
        trend_value, trend_direction = self.calculate_trend(df, row['symbol'], idx)
        
        # Store trend direction for use in _handle_entry
        self._last_trend_direction = trend_direction
        
        # Return True if we have a valid trend direction
        return trend_direction in [DIRECTION_BULLISH, DIRECTION_BEARISH]

    def calculate_exit_signals(self, df: pd.DataFrame, idx: int, row: pd.Series, trade) -> bool:
        """Calculate exit signals for the strategy"""
        current_price = row['close']
        
        # Stop loss hit
        if (trade.direction == DIRECTION_BULLISH and current_price <= trade.stop_price) or \
           (trade.direction == DIRECTION_BEARISH and current_price >= trade.stop_price):
            trade.exit_reason = "Stop loss"
            return True
            
        # Take profit hit
        take_profit = self.calculate_take_profit(trade.entry_price, trade.stop_price, trade.direction)
        if (trade.direction == DIRECTION_BULLISH and current_price >= take_profit) or \
           (trade.direction == DIRECTION_BEARISH and current_price <= take_profit):
            trade.exit_reason = "Take profit"
            return True
            
        return False

    def _handle_entry(self, row: pd.Series, symbol: str):
        """Handle trade entry logic"""
        # Calculate stop price using delta amount
        stop_price = calculate_stop_price(row['close'], self._last_trend_direction, self.stop_loss_amount)
        
        # Calculate max risk amount for this trade
        max_risk_amount = self.current_capital * (self.risk_per_trade_pct / 100)
        
        # Calculate position size based on entry, stop, and max risk
        quantity = calculate_position_size(row['close'], stop_price, max_risk_amount)
        
        # Create new trade
        trade = Trade(
            symbol=symbol,
            direction=self._last_trend_direction,
            entry_date=row['date_and_time'].date().isoformat(),
            entry_timestamp=row['date_and_time'].isoformat(),
            entry_price=row['close'],
            stop_price=stop_price,
            quantity=quantity,
            account_size=self.current_capital,
            variant=self.variant
        )
        
        # Add trade to active trades
        self.active_trades[symbol].append(trade)
        
        # Update current capital
        self.current_capital -= trade.capital_required

    def _handle_exit(self, row: pd.Series, symbol: str, trade):
        """Handle trade exit logic"""
        # Set exit information
        if not trade.exit_reason:
            trade.exit_reason = "End of session"
            
        trade.set_exit(
            exit_price=row['close'],
            exit_date=row['date_and_time'].date().isoformat(),
            exit_timestamp=row['date_and_time'].isoformat(),
            exit_reason=trade.exit_reason
        )
        
        # Update current capital
        self.current_capital += trade.capital_required * (1 + trade.perc_return / 100)
        
        # Move trade from active to completed
        self.trades.append(trade)
        self.active_trades[symbol].remove(trade)

    def run(self):
        """Run the strategy backtest"""
        self.run_backtest(
            table_name="historical_data_30mins",
            additional_columns=["diff_pct_open_close", "tightness"]
        )

def main():
    # Database path
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data.db')
    
    # Initialize database manager
    db_manager = DatabaseManager(db_path)
    
    # Strategy parameters
    symbols = ['AAPL', 'MSFT', 'NVDA', 'AMD', 'META', 'GOOGL', 'AMZN', 'NFLX', 'TSLA']
    start_date = '2024-01-01'
    end_date = '2024-03-15'
    
    # Create strategy instance with RR 2.0
    strategy_v1 = TightCandleStrategy(
        db_path=db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        target_risk_reward=2.0,
        variant="RR_2.0"
    )
    
    # Run backtest
    print("Starting backtest for variant RR_2.0...")
    strategy_v1.run()
    
    # Save trades to database
    db_manager.save_trades(
        trades=strategy_v1.trades,
        strategy_name=strategy_v1.strategy_name,
        strategy_version=strategy_v1.strategy_version,
        variant=strategy_v1.variant,
        execution_date=strategy_v1.execution_date
    )
    
    # Create strategy instance with RR 3.0
    strategy_v2 = TightCandleStrategy(
        db_path=db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        target_risk_reward=3.0,
        variant="RR_3.0"
    )
    
    # Run backtest
    print("\nStarting backtest for variant RR_3.0...")
    strategy_v2.run()
    
    # Save trades to database
    db_manager.save_trades(
        trades=strategy_v2.trades,
        strategy_name=strategy_v2.strategy_name,
        strategy_version=strategy_v2.strategy_version,
        variant=strategy_v2.variant,
        execution_date=strategy_v2.execution_date
    )

if __name__ == "__main__":
    main() 