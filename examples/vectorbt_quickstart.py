import vectorbt as vbt
import numpy as np
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_quickstart_example():
    """Run the official VectorBT quickstart example with DataFrame outputs."""
    
    # Download Bitcoin price data
    logger.info("Downloading BTC-USD price data...")
    price = vbt.YFData.download('BTC-USD').get('Close')
    
    # Example 1: Simple Buy and Hold
    logger.info("\nExample 1: Buy and Hold Strategy")
    logger.info("-" * 50)
    pf_hold = vbt.Portfolio.from_holding(price, init_cash=100)
    
    # Create performance DataFrame for Buy & Hold
    hold_metrics = pd.DataFrame({
        'Total Profit': [float(pf_hold.total_profit())],
        'Total Return': [float(pf_hold.total_return())],
        'Sharpe Ratio': [float(pf_hold.sharpe_ratio())],
        'Max Drawdown': [float(pf_hold.max_drawdown())]
    }, index=['Buy & Hold'])
    print("\nBuy & Hold Performance:")
    print(hold_metrics)
    
    # Example 2: Moving Average Crossover Strategy
    logger.info("\nExample 2: MA Crossover Strategy")
    logger.info("-" * 50)
    
    # Calculate moving averages
    fast_ma = vbt.MA.run(price, 10)
    slow_ma = vbt.MA.run(price, 50)
    
    # Generate entry/exit signals
    entries = fast_ma.ma_crossed_above(slow_ma)
    exits = fast_ma.ma_crossed_below(slow_ma)
    
    # Run backtest
    pf_ma = vbt.Portfolio.from_signals(price, entries, exits, init_cash=100)
    
    # Get trades DataFrame
    trades_df = pf_ma.trades.records
    
    # Get the dates from the price index
    dates = price.index
    
    # Calculate trade durations in days
    trade_durations = [(dates[exit_idx] - dates[entry_idx]).days 
                      for entry_idx, exit_idx in zip(trades_df['entry_idx'], trades_df['exit_idx'])]
    
    # Format the trades DataFrame with date and time
    formatted_trades = pd.DataFrame({
        'Entry Date': dates[trades_df['entry_idx']].strftime('%Y-%m-%d %H:%M:%S'),
        'Exit Date': dates[trades_df['exit_idx']].strftime('%Y-%m-%d %H:%M:%S'),
        'Entry Price': trades_df['entry_price'].round(2),
        'Exit Price': trades_df['exit_price'].round(2),
        'Size': trades_df['size'].round(6),
        'PnL': trades_df['pnl'].round(2),
        'Return %': (trades_df['return'] * 100).round(2),
        'Duration (days)': trade_durations
    })
    
    # Set display options to show all rows
    pd.set_option('display.max_rows', None)
    
    print("\nMA Crossover Strategy Trades:")
    print(formatted_trades)
    
    # Print trade statistics
    print("\nTrade Statistics:")
    print(f"Total Trades: {len(trades_df)}")
    print(f"Profitable Trades: {len(trades_df[trades_df['pnl'] > 0])}")
    print(f"Loss-Making Trades: {len(trades_df[trades_df['pnl'] < 0])}")
    print(f"Average Profit per Trade: ${trades_df['pnl'].mean():.2f}")
    print(f"Best Trade: ${trades_df['pnl'].max():.2f}")
    print(f"Worst Trade: ${trades_df['pnl'].min():.2f}")
    print(f"Average Trade Duration: {np.mean(trade_durations):.1f} days")
    
    # Example 3: Testing Multiple Window Combinations
    logger.info("\nExample 3: Testing Multiple MA Windows")
    logger.info("-" * 50)
    
    # Generate window combinations
    windows = np.arange(2, 31, 2)  # Test windows from 2 to 30
    fast_ma, slow_ma = vbt.MA.run_combs(price, window=windows, r=2, short_names=['fast', 'slow'])
    entries = fast_ma.ma_crossed_above(slow_ma)
    exits = fast_ma.ma_crossed_below(slow_ma)
    
    # Run backtest for all combinations
    pf_windows = vbt.Portfolio.from_signals(price, entries, exits, init_cash=100)
    
    # Create performance DataFrame for all combinations
    window_results = pd.DataFrame({
        'Fast Window': [pair[0] for pair in pf_windows.wrapper.columns],
        'Slow Window': [pair[1] for pair in pf_windows.wrapper.columns],
        'Total Return': pf_windows.total_return(),
        'Sharpe Ratio': pf_windows.sharpe_ratio(),
        'Max Drawdown': pf_windows.max_drawdown()
    })
    window_results = window_results.sort_values('Total Return', ascending=False)
    
    print("\nTop 5 Window Combinations:")
    print(window_results.head())

if __name__ == "__main__":
    run_quickstart_example() 