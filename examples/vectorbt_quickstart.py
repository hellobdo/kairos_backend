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
    
    # Create DataFrame with price and MAs
    ma_df = pd.DataFrame({
        'Price': price,
        'Fast MA': fast_ma.ma,
        'Slow MA': slow_ma.ma,
        'Buy Signal': entries,
        'Sell Signal': exits
    })
    print("\nLast 5 rows of Price and Signals:")
    print(ma_df.tail())
    
    # Run backtest
    pf_ma = vbt.Portfolio.from_signals(price, entries, exits, init_cash=100)
    
    # Create performance DataFrame for MA Strategy
    ma_metrics = pd.DataFrame({
        'Total Profit': [float(pf_ma.total_profit())],
        'Total Return': [float(pf_ma.total_return())],
        'Sharpe Ratio': [float(pf_ma.sharpe_ratio())],
        'Max Drawdown': [float(pf_ma.max_drawdown())]
    }, index=['MA Crossover'])
    print("\nMA Crossover Performance:")
    print(ma_metrics)
    
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