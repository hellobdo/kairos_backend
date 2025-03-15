import argparse
import logging
import json
import pandas as pd
import numpy as np
import vectorbt as vbt
import sys
import os

# Add project root to sys.path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backtests.backtest_loader import setup_backtest
from backtests.helpers.trade_logger import log_trades, format_trades, print_performance_metrics

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_file):
    """Load configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        logger.info(f"Loaded configuration from {config_file}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return None

def run_backtest(config_file):
    """Run a backtest using configurations from a file."""
    try:
        # Load configuration
        config = load_config(config_file)
        if not config:
            return False
        
        # Extract backtest parameters
        bc = config['backtest']
        symbol = bc['symbol']
        
        # Set up the backtest - this gets all configs from DB
        data = setup_backtest(
            symbol, bc['entry_config_id'], bc['stoploss_config_id'], 
            bc['risk_config_id'], bc['exit_config_id'], bc['swing_config_id'], 
            bc.get('exits_swings_config_id'), bc.get('date_range')
        )
        
        if not data:
            logger.error("Failed to set up backtest")
            return False
        
        # Initialize basic parameters
        init_cash = bc.get('init_cash', 10000)  # Get from config or use default
        df = data['df']
        entries = data['entries']
        direction = data['direction']  # Get trade direction
        exit_config = data['exit_config']
        stop_config = data['stop_config']
        risk_config = data['risk_config']
        
        # Calculate stop prices for each entry - we'll need these for custom exits
        stop_prices = pd.Series(index=df.index, dtype=float)
        
        # Determine if long or short for calculating stops
        is_long = direction.lower() == 'long'
        logger.info(f"Running {direction} strategy backtest")
        
        # Calculate the ACTUAL stop prices that will be used for both position sizing and R:R calculation
        if stop_config['stop_type'] == 'perc':
            for i, entry in enumerate(entries):
                if entry:
                    idx = entries.index[i]
                    price = df.loc[idx, 'close']
                    if is_long:
                        # For longs, stop is below entry price
                        stop_prices[idx] = price * (1 - stop_config['stop_value'])
                    else:
                        # For shorts, stop is above entry price
                        stop_prices[idx] = price * (1 + stop_config['stop_value'])
        elif stop_config['stop_type'] == 'abs':
            for i, entry in enumerate(entries):
                if entry:
                    idx = entries.index[i]
                    price = df.loc[idx, 'close']
                    if is_long:
                        # For longs, stop is below entry price
                        stop_prices[idx] = price - stop_config['stop_value']
                    else:
                        # For shorts, stop is above entry price
                        stop_prices[idx] = price + stop_config['stop_value']
        else:  # custom
            for i, entry in enumerate(entries):
                if entry:
                    idx = entries.index[i]
                    price = df.loc[idx, 'close']
                    stop_value = stop_config['stop_func'](price)
                    if is_long:
                        # For longs, stop is below entry price
                        stop_prices[idx] = price * (1 - stop_value)
                    else:
                        # For shorts, stop is above entry price
                        stop_prices[idx] = price * (1 + stop_value)
        
        # Save all entry points and their stop prices for debugging
        entry_stops = []
        for i, entry in enumerate(entries):
            if entry:
                idx = entries.index[i]
                price = df.loc[idx, 'close']
                stop = stop_prices[idx]
                risk_pct = abs((price - stop) / price) * 100
                entry_stops.append({
                    'idx': idx,
                    'price': price,
                    'stop': stop,
                    'risk_pct': risk_pct
                })
        
        logger.info(f"Found {len(entry_stops)} entry points with stops:")
        for i, es in enumerate(entry_stops[:5]):  # Log first 5 entries
            logger.info(f"Entry {i+1}: price=${es['price']:.2f}, stop=${es['stop']:.2f}, risk={es['risk_pct']:.2f}%")
        if len(entry_stops) > 5:
            logger.info(f"... and {len(entry_stops) - 5} more entries")
        
        # Build portfolio kwargs
        kwargs = {
            'close': df['close'],
            'entries': entries,
            'init_cash': init_cash,
            'fees': 0.0,
            'freq': '30min',
            'upon_long_conflict': 'exit',
            'accumulate': False,
        }
        
        # Calculate position sizes based on risk management
        sizes = pd.Series(0.0, index=df.index)
        
        # For each entry point, calculate appropriate position size
        for i, entry in enumerate(entries):
            if entry:
                idx = entries.index[i]
                entry_price = df.loc[idx, 'close']
                
                # Find the stop price for this entry
                stop_price = stop_prices.loc[idx]
                
                # Calculate risk per position in dollars
                risk_amount = init_cash * risk_config['risk_per_trade']
                
                # Calculate position size based on price difference
                if is_long:
                    price_diff = entry_price - stop_price
                else:
                    price_diff = stop_price - entry_price
                
                # Avoid division by zero
                if price_diff <= 0:
                    logger.warning(f"Invalid price difference at {idx}: entry={entry_price}, stop={stop_price}")
                    continue
                
                # Calculate shares based on risk amount and price difference
                shares = risk_amount / price_diff
                # Round to whole number of shares
                shares = round(shares, 0)
                sizes.loc[idx] = shares
                
                logger.info(f"Entry at {idx}: price=${entry_price:.2f}, stop=${stop_price:.2f}, "
                           f"shares={shares}, risk=${risk_amount:.2f}")
        
        # Add size to portfolio kwargs
        kwargs['size'] = sizes
        
        # Set direction based on entry configuration
        if not is_long:
            # For shorts, set direction to -1 (VectorBT uses 1 for long, -1 for short)
            kwargs['direction'] = -1
            logger.info("Setting VectorBT direction to short (-1)")
        
        # Configure stop loss based on type and direction
        if stop_config['stop_type'] == 'perc':
            # For shorts, we need to set different parameters
            if is_long:
                kwargs['sl_stop'] = stop_config['stop_value']
            else:
                kwargs['sl_stop'] = stop_config['stop_value']
                # For shorts, sl_stop is above entry price
                kwargs['sl_stop_above'] = True
        elif stop_config['stop_type'] == 'abs':
            if is_long:
                kwargs['sl_stop_abs'] = stop_config['stop_value']
            else:
                kwargs['sl_stop_abs'] = stop_config['stop_value']
                # For shorts, sl_stop_abs is an absolute increase
                kwargs['sl_stop_above'] = True
        else:
            if is_long:
                kwargs['sl_stop_custom'] = stop_config['stop_func']
            else:
                # For shorts, we need a modified function that returns positive percentages
                def short_stop_func(price):
                    return stop_config['stop_func'](price)
                kwargs['sl_stop_custom'] = short_stop_func
                kwargs['sl_stop_above'] = True
        
        # Create a Series to hold custom stop prices for each entry
        # This is needed because VectorBT may not exit exactly at the stop price
        stop_level_series = pd.Series(np.nan, index=df.index)
        
        # Configure stop loss settings for VectorBT
        # For long positions:
        #   - sl_stop without sl_stop_above: exit when price goes BELOW the level
        # For short positions:
        #   - sl_stop with sl_stop_above: exit when price goes ABOVE the level
        if is_long:
            if stop_config['stop_type'] == 'perc':
                # Percentage-based stop
                kwargs['sl_stop'] = stop_config['stop_value']
            else:
                # We need to use a custom stop loss approach for absolute stops
                # Fill the stop_level_series with actual stop prices where we have entries
                for i, entry in enumerate(entries):
                    if entry:
                        idx = entries.index[i]
                        stop_level_series[idx] = stop_prices[idx]
                
                # Tell VectorBT to use our custom stop series
                kwargs['sl_stop_price'] = stop_level_series
        else:
            # For shorts
            if stop_config['stop_type'] == 'perc':
                kwargs['sl_stop'] = stop_config['stop_value']
                kwargs['sl_stop_above'] = True  # For shorts, exit when price goes above level
            else:
                # Custom stop series for absolute stops
                for i, entry in enumerate(entries):
                    if entry:
                        idx = entries.index[i]
                        stop_level_series[idx] = stop_prices[idx]
                
                kwargs['sl_stop_price'] = stop_level_series
                kwargs['sl_stop_above'] = True  # For shorts, exit when price goes above level
        
        # Create custom exits based on risk/reward if fixed exit configuration
        # Also track which exit timestamps were from take profits
        take_profit_exits = {}  # Dictionary to track which exits were take profits
        
        if exit_config['type'] == 'fixed' and exit_config['risk_reward'] > 0:
            target_rr = exit_config['risk_reward']
            logger.info(f"Using dynamic risk/reward exits with target R:R of {target_rr}")
            
            # Pre-calculate exits based on risk/reward ratio
            exits = pd.Series(False, index=df.index)
            
            # Track open positions and their entry details
            open_positions = {}  # {entry_idx: {'price': price, 'stop': stop_price}}
            
            # Scan through the data chronologically
            for i in range(len(df)):
                idx = df.index[i]
                
                # Check for entry signal
                if entries.iloc[i]:
                    # Record entry information
                    entry_price = df.loc[idx, 'close']
                    stop_price = stop_prices.loc[idx]
                    
                    # Verify we have a valid stop price
                    if pd.isna(stop_price) or stop_price == 0:
                        logger.warning(f"Invalid stop price {stop_price} at {idx}, skipping entry")
                        continue
                        
                    # Calculate the exact take profit price based on the target R:R
                    if is_long:
                        # For longs: TP = entry + (entry - stop) * target_rr
                        risk = entry_price - stop_price
                        take_profit_price = entry_price + (risk * target_rr)
                    else:
                        # For shorts: TP = entry - (stop - entry) * target_rr
                        risk = stop_price - entry_price
                        take_profit_price = entry_price - (risk * target_rr)
                    
                    open_positions[idx] = {
                        'price': entry_price, 
                        'stop': stop_price,
                        'tp_price': take_profit_price
                    }
                    logger.info(f"Entry at {idx}: ${entry_price:.2f}, stop=${stop_price:.2f}, TP=${take_profit_price:.2f}")
                    continue
                
                # Check all open positions for potential exit
                for entry_idx, position in list(open_positions.items()):
                    entry_price = position['price']
                    stop_price = position['stop']
                    take_profit_price = position['tp_price']
                    
                    # Check if price hits take profit level
                    if is_long:
                        # For longs: use high price for take profit
                        current_price = df.loc[idx, 'high']
                        if current_price >= take_profit_price:
                            logger.info(f"Taking profit at {idx}: ${current_price:.2f} >= ${take_profit_price:.2f} (target R:R={target_rr})")
                            exits.loc[idx] = True
                            # Mark this as a take profit exit
                            take_profit_exits[idx] = {
                                'entry_idx': entry_idx, 
                                'entry_price': entry_price,
                                'stop_price': stop_price,
                                'exit_price': take_profit_price,
                                'rr': target_rr
                            }
                            # Remove the position from open positions
                            del open_positions[entry_idx]
                    else:
                        # For shorts: use low price for take profit
                        current_price = df.loc[idx, 'low']
                        if current_price <= take_profit_price:
                            logger.info(f"Taking profit at {idx}: ${current_price:.2f} <= ${take_profit_price:.2f} (target R:R={target_rr})")
                            exits.loc[idx] = True
                            # Mark this as a take profit exit
                            take_profit_exits[idx] = {
                                'entry_idx': entry_idx, 
                                'entry_price': entry_price,
                                'stop_price': stop_price,
                                'exit_price': take_profit_price,
                                'rr': target_rr
                            }
                            # Remove the position from open positions
                            del open_positions[entry_idx]
            
            # Add pre-calculated exits to kwargs
            if 'exits' in kwargs:
                kwargs['exits'] = kwargs['exits'] | exits
            else:
                kwargs['exits'] = exits
            
            logger.info(f"Generated {exits.sum()} take profit exit signals at R:R={target_rr}")
        
        # Remove exit_func if it exists in kwargs (it's not supported)
        if 'exit_func' in kwargs:
            del kwargs['exit_func']
        
        # Add EOD exits if swings not allowed
        if data['swing_config']['id'] == 1 and data['swing_config']['swings_allowed'] == 0:
            if 'market_session' in df.columns:
                last_candles = df[df['market_session'] == 'regular'].groupby(df.index.date).apply(lambda x: x.index[-1])
            else:
                last_candles = df.groupby(df.index.date).apply(lambda x: x.index[-1])
            
            eod_exits = pd.Series(False, index=df.index)
            eod_exits.loc[last_candles] = True
            
            # Combine with any existing exits
            if 'exits' in kwargs:
                kwargs['exits'] = kwargs['exits'] | eod_exits
            else:
                kwargs['exits'] = eod_exits
        
        # Run the VectorBT portfolio
        logger.info(f"Running VectorBT portfolio backtest for {direction} strategy...")
        pf = vbt.Portfolio.from_signals(**kwargs)
        trades = pf.trades.records
        
        if len(trades) == 0:
            logger.warning("No trades were generated")
            return False
        
        # Format and log trades
        logger.info(f"Processing {len(trades)} trades...")
        context = {
            'pf': pf, 
            'direction': direction,
            'take_profit_exits': take_profit_exits  # Pass our take profit exit info
        }
        formatted_trades = format_trades(df, trades, stop_config, risk_config, exit_config, data['swing_config'], context)
        print_performance_metrics(pf, formatted_trades, exit_config)
        log_trades(formatted_trades, data['run_id'], pf, trades, symbol, stop_config, risk_config)
        
        return True
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False

def main():
    """Run the backtest using the default configuration file."""
    # Use the default config file path (no command line arguments needed)
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backtest_config.json')
    logger.info(f"Using config file: {config_file}")
    
    success = run_backtest(config_file)
    logger.info("Backtest completed successfully" if success else "Backtest failed")
    return 0 if success else 1

if __name__ == "__main__":
    exit(main()) 