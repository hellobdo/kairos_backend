import vectorbt as vbt
import numpy as np
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_quickstart_example():
    """Run the official VectorBT quickstart example."""
    
    # Download Bitcoin price data
    logger.info("Downloading BTC-USD price data...")
    price = vbt.YFData.download('BTC-USD').get('Close')
    
    # Example 1: Simple Buy and Hold
    logger.info("\nExample 1: Buy and Hold Strategy")
    logger.info("-" * 50)
    pf_hold = vbt.Portfolio.from_holding(price, init_cash=100)
    logger.info(f"Total Profit (Buy & Hold): ${float(pf_hold.total_profit()):.2f}")
    
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
    logger.info(f"Total Profit (MA Crossover): ${float(pf_ma.total_profit()):.2f}")
    
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
    
    # Find best combination
    total_returns = pf_windows.total_return()
    best_comb_id = total_returns.nlargest(1).index[0]
    best_return = total_returns.max()
    
    logger.info("Best Window Combination:")
    logger.info(f"Fast MA: {best_comb_id[0]} days")
    logger.info(f"Slow MA: {best_comb_id[1]} days")
    logger.info(f"Total Return: {float(best_return):.2%}")

if __name__ == "__main__":
    run_quickstart_example() 