from infrastructure.core.vectorbt_strategy import VectorBTStrategy
from infrastructure.indicators.tight_candle import TightCandle
import pandas as pd
from typing import Tuple

class TightCandleStrategy(VectorBTStrategy):
    def __init__(self,
                 db_path: str,
                 symbols: list[str],
                 start_date: str = None,
                 end_date: str = None,
                 initial_capital: float = 100000,
                 risk_per_trade_pct: float = 0.01,
                 tightness_threshold: float = 0.1,
                 target_risk_reward: float = 2.0):
        """
        TightCandle strategy implementation using VectorBT.
        
        Args:
            db_path: Path to SQLite database
            symbols: List of symbols to trade
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            initial_capital: Initial capital for backtesting
            risk_per_trade_pct: Risk per trade as percentage of capital
            tightness_threshold: Maximum ratio for tight candles
            target_risk_reward: Target risk/reward ratio for take profit
        """
        super().__init__(
            db_path=db_path,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            risk_per_trade_pct=risk_per_trade_pct
        )
        
        self.tightness_threshold = tightness_threshold
        self.target_risk_reward = target_risk_reward
        self.indicator = TightCandle(tightness_threshold=tightness_threshold)
    
    def generate_signals(self, symbol: str) -> Tuple[pd.Series, pd.Series]:
        """
        Generate entry and exit signals for a symbol.
        
        Args:
            symbol: Symbol to generate signals for
            
        Returns:
            Tuple of (entries, exits) as boolean Series
        """
        # Get market data
        ohlcv = self.data[symbol]
        
        # Get entry signals from indicator
        long_entries, short_entries = self.indicator.find_entry_signals(ohlcv)
        
        # Calculate stop prices
        long_stops = self.indicator.calculate_stop_price(ohlcv, long_entries, 'long')
        short_stops = self.indicator.calculate_stop_price(ohlcv, short_entries, 'short')
        
        # Calculate take profit levels
        long_tp = pd.Series(index=ohlcv.index, dtype=float)
        short_tp = pd.Series(index=ohlcv.index, dtype=float)
        
        # For long trades
        long_risk = ohlcv['close'] - long_stops
        long_tp[long_entries] = ohlcv.loc[long_entries, 'close'] + (long_risk[long_entries] * self.target_risk_reward)
        
        # For short trades
        short_risk = short_stops - ohlcv['close']
        short_tp[short_entries] = ohlcv.loc[short_entries, 'close'] - (short_risk[short_entries] * self.target_risk_reward)
        
        # Combine entries
        entries = long_entries | short_entries
        
        # Generate exit signals
        exits = pd.Series(False, index=ohlcv.index)
        
        # For each entry point
        for i, (idx, is_entry) in enumerate(entries.items()):
            if not is_entry:
                continue
                
            # Determine if it's a long or short trade
            is_long = long_entries[idx]
            
            # Get stop and target prices
            stop = long_stops[idx] if is_long else short_stops[idx]
            target = long_tp[idx] if is_long else short_tp[idx]
            
            # Look for exit after entry
            future_prices = ohlcv.iloc[i+1:]
            
            if is_long:
                # Exit when price hits stop loss or take profit
                stop_hit = future_prices['low'] <= stop
                target_hit = future_prices['high'] >= target
            else:
                # Exit when price hits stop loss or take profit
                stop_hit = future_prices['high'] >= stop
                target_hit = future_prices['low'] <= target
            
            # Find first exit point
            exit_idx = None
            if stop_hit.any() and target_hit.any():
                stop_idx = stop_hit.idxmax()
                target_idx = target_hit.idxmax()
                exit_idx = min(stop_idx, target_idx)
            elif stop_hit.any():
                exit_idx = stop_hit.idxmax()
            elif target_hit.any():
                exit_idx = target_hit.idxmax()
            
            if exit_idx:
                exits[exit_idx] = True
        
        return entries, exits

def main():
    """Run backtest for TightCandle strategy."""
    db_path = "/Users/brunodeoliveira/Library/Mobile Documents/com~apple~CloudDocs/repos/kairos/kairos.db"
    
    strategy = TightCandleStrategy(
        db_path=db_path,
        symbols=['HOOD', 'META', 'NVDA', 'PLTR', 'QQQ', 'TSLA'],
        initial_capital=100000,
        risk_per_trade_pct=0.01,
        tightness_threshold=0.1,
        target_risk_reward=2.0
    )
    
    # Run backtest
    results = strategy.run_all()
    
    # Print combined statistics
    total_return = sum(pf.total_return for pf in results.values())
    avg_sharpe = sum(pf.sharpe_ratio for pf in results.values()) / len(results)
    max_dd = min(pf.max_drawdown for pf in results.values())
    total_trades = sum(len(pf.trades) for pf in results.values())
    
    print("\nOverall Statistics:")
    print(f"Total Return (all symbols): {total_return:.2%}")
    print(f"Average Sharpe Ratio: {avg_sharpe:.2f}")
    print(f"Maximum Drawdown: {max_dd:.2%}")
    print(f"Total Trades: {total_trades}")

if __name__ == "__main__":
    main() 