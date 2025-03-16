import argparse
import logging
import json
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime

# Add project root to sys.path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backtests.backtest_loader import setup_backtest, load_config, load_backtest_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_risk_reward(entry_price, exit_price, stop_price, is_long):
    """
    Calculate risk/reward ratio for a trade.
    
    Args:
        entry_price (float): Entry price
        exit_price (float): Exit price
        stop_price (float): Stop price
        is_long (bool): Whether this is a long trade
        
    Returns:
        float: Risk/reward ratio
    """
    if is_long:
        # For longs: (exit_price - entry_price) / (entry_price - stop_price)
        return (exit_price - entry_price) / (entry_price - stop_price)
    else:
        # For shorts: (entry_price - exit_price) / (stop_price - entry_price)
        return (entry_price - exit_price) / (stop_price - entry_price)

def format_trades(trade_list, df, stop_config, risk_config, exit_config, swing_config):
    """
    Format trades into a structured DataFrame for logging.
    
    Args:
        trade_list (list): List of trade dictionaries
        df (pd.DataFrame): Price data
        stop_config (dict): Stop loss configuration
        risk_config (dict): Risk configuration
        exit_config (dict): Exit configuration
        swing_config (dict): Swing configuration
        
    Returns:
        pd.DataFrame: Formatted trade data
    """
    if not trade_list:
        return pd.DataFrame()
    
    # Initialize DataFrame from trades
    trades_df = pd.DataFrame(trade_list)
    
    # Add additional calculations & formatting
    trades_df['winning_trade'] = (trades_df['risk_reward'] > 0).astype(int)
    trades_df['trade_duration'] = (trades_df['exit_timestamp'] - trades_df['entry_timestamp']).dt.total_seconds() / 3600
    
    # Ensure correct types
    trades_df['entry_timestamp'] = trades_df['entry_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    trades_df['exit_timestamp'] = trades_df['exit_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Log summary of trade classifications
    logger.info(f"Classification Summary:")
    exit_counts = trades_df['exit_type'].value_counts()
    for exit_type, count in exit_counts.items():
        rr_values = trades_df[trades_df['exit_type'] == exit_type]['risk_reward']
        logger.info(f"  {exit_type}: {count} trades")
        if len(rr_values) > 0:
            logger.info(f"    Avg R:R: {rr_values.mean():.2f}, Min: {rr_values.min():.2f}, Max: {rr_values.max():.2f}")
    
    return trades_df

def log_trades_to_db(trades_df, run_id, symbol):
    """
    Log trades to database.
    
    Args:
        trades_df (pd.DataFrame): Formatted trade data
        run_id (int): Backtest run ID
        symbol (str): Trading symbol
    """
    import sqlite3
    
    if trades_df.empty:
        logger.warning("No trades to log to database")
        return
    
    try:
        conn = sqlite3.connect('data/algos.db')
        cursor = conn.cursor()
        
        # First check if any trades already exist for this run_id
        cursor.execute("SELECT COUNT(*) FROM trades WHERE run_id = ?", (run_id,))
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            logger.warning(f"Found {existing_count} existing trades for run_id {run_id}. Deleting before inserting new trades.")
            cursor.execute("DELETE FROM trades WHERE run_id = ?", (run_id,))
        
        for _, trade in trades_df.iterrows():
            cursor.execute("""
                INSERT INTO trades (
                    run_id,
                    symbol,
                    entry_timestamp,
                    exit_timestamp,
                    entry_price,
                    exit_price,
                    stop_price,
                    position_size,
                    risk_size,
                    risk_per_trade,
                    risk_reward,
                    perc_return,
                    winning_trade,
                    trade_duration,
                    capital_required,
                    direction,
                    exit_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                symbol,
                trade['entry_timestamp'],
                trade['exit_timestamp'],
                trade['entry_price'],
                trade['exit_price'],
                trade['stop_price'],
                trade['position_size'],
                trade['risk_size'],
                trade['risk_per_trade'],
                trade['risk_reward'],
                trade['perc_return'],
                trade['winning_trade'],
                trade['trade_duration'],
                trade['capital_required'],
                trade['direction'],
                trade['exit_type']
            ))
        
        conn.commit()
        conn.close()
        logger.info(f"Successfully logged {len(trades_df)} trades to database for run_id {run_id}")
    except Exception as e:
        logger.error(f"Error logging trades to database: {e}")

def run_backtest(config_file):
    """Run a backtest using configurations from a file without VectorBT."""
    try:
        # Load configuration using the backtest loader
        config_params = load_backtest_config(config_file)
        if not config_params:
            return False
        
        # Extract parameters from the configuration tuple
        symbol, entry_config_id, stoploss_config_id, risk_config_id, exit_config_id, \
        swing_config_id, exits_swings_config_id, date_range, bc = config_params
        
        # Set up the backtest - this gets all configs from DB
        data = setup_backtest(
            symbol, entry_config_id, stoploss_config_id, 
            risk_config_id, exit_config_id, swing_config_id, 
            exits_swings_config_id, date_range
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
        swing_config = data['swing_config']
        run_id = data['run_id']
        
        # Determine if long or short
        is_long = direction.lower() == 'long'
        logger.info(f"Running {direction} strategy backtest")
        
        # Calculate stop prices for each entry
        stop_prices = pd.Series(index=df.index, dtype=float)
        
        # Calculate the stop prices for each entry
        for i, entry in enumerate(entries):
            if not entry:
                continue
            
            idx = entries.index[i]
            price = df.loc[idx, 'close']
            
            if stop_config['stop_type'] == 'perc':
                if is_long:
                    stop_prices[idx] = price * (1 - stop_config['stop_value'])
                else:
                    stop_prices[idx] = price * (1 + stop_config['stop_value'])
            elif stop_config['stop_type'] == 'abs':
                if is_long:
                    stop_prices[idx] = price - stop_config['stop_value']
                else:
                    stop_prices[idx] = price + stop_config['stop_value']
            else:  # custom
                stop_value = stop_config['stop_func'](price)
                if is_long:
                    stop_prices[idx] = price * (1 - stop_value)
                else:
                    stop_prices[idx] = price * (1 + stop_value)
        
        # Log entry points and their stop prices
        entry_points = []
        for i, entry in enumerate(entries):
            if not entry:
                continue
            
            idx = entries.index[i]
            price = df.loc[idx, 'close']
            stop = stop_prices[idx]
            risk_pct = abs((price - stop) / price) * 100
            entry_points.append({
                'timestamp': idx,
                'price': price,
                'stop': stop,
                'risk_pct': risk_pct
            })
        
        logger.info(f"Found {len(entry_points)} entry points with stops:")
        for i, ep in enumerate(entry_points[:5]):  # Log first 5 entries
            logger.info(f"Entry {i+1}: price=${ep['price']:.2f}, stop=${ep['stop']:.2f}, risk={ep['risk_pct']:.2f}%")
        if len(entry_points) > 5:
            logger.info(f"... and {len(entry_points) - 5} more entries")
        
        # Get target risk/reward
        target_rr = exit_config.get('risk_reward', 2.0) if exit_config and exit_config['type'] == 'fixed' else 2.0
        logger.info(f"Using target risk/reward ratio of {target_rr}")
        
        # Process each day individually
        trades = []
        active_positions = {}  # {entry_timestamp: position_data}
        
        # Find last candle of each day for EOD exits
        if 'market_session' in df.columns:
            # Convert index to pandas Series first and extract date properly
            dates = pd.Series(df.index).dt.date
            last_candles = df[df['market_session'] == 'regular'].groupby(dates).apply(lambda x: x.index[-1])
        else:
            # Convert index to pandas Series first and extract date properly
            dates = pd.Series(df.index).dt.date
            last_candles = df.groupby(dates).apply(lambda x: x.index[-1])
        
        # Iterate through each candle
        for i in range(len(df.index)):
            current_idx = df.index[i]
            
            # 1. Check for entry signals
            if entries.iloc[i]:
                # Only open a new position if no positions are currently open
                if not active_positions:
                    entry_price = df['close'].iloc[i]
                    stop_price = stop_prices.iloc[i]
                    
                    # Skip invalid entries
                    if pd.isna(stop_price) or stop_price <= 0:
                        logger.warning(f"Invalid stop price {stop_price} at {current_idx}, skipping entry")
                        continue
                        
                    # Calculate risk amount
                    risk_amount = init_cash * risk_config['risk_per_trade'] / 100

                    # Calculate price difference for position sizing
                    price_diff = abs(entry_price - stop_price)

                    # Calculate position size
                    position_size = round(risk_amount / price_diff)
                    
                    # Calculate take profit price
                    if is_long:
                        risk = entry_price - stop_price
                        take_profit_price = entry_price + (risk * target_rr)
                    else:
                        risk = stop_price - entry_price
                        take_profit_price = entry_price - (risk * target_rr)
                    
                    # Store position information
                    active_positions[current_idx] = {
                        'entry_timestamp': current_idx,
                        'entry_price': entry_price,
                        'stop_price': stop_price,
                        'take_profit_price': take_profit_price,
                        'position_size': position_size,
                        'direction': direction,
                        'risk_per_trade': risk_config['risk_per_trade'] * 100,
                        'capital_required': position_size * entry_price,
                        'risk_size': position_size * abs(entry_price - stop_price)
                    }
                    
                    logger.info(f"Entry at {current_idx}: ${entry_price:.2f}, stop=${stop_price:.2f}, "
                                f"TP=${take_profit_price:.2f}, size={position_size}")
                else:
                    # Skip entry signal because a position is already open
                    logger.info(f"Skipping entry signal at {current_idx} - position already open")
            
            # 2. Check for exits on active positions
            for entry_time, position in list(active_positions.items()):
                # Get current prices
                high_price = df['high'].iloc[i]
                low_price = df['low'].iloc[i]
                close_price = df['close'].iloc[i]
                
                # Variables for checking exit conditions
                exit_price = None
                exit_type = None
                
                # Check for stop loss hit
                if is_long and low_price <= position['stop_price']:
                    # Long position hit stop loss
                    exit_price = position['stop_price']  # Exit at exact stop price
                    exit_type = "Stop Loss"
                elif not is_long and high_price >= position['stop_price']:
                    # Short position hit stop loss
                    exit_price = position['stop_price']  # Exit at exact stop price
                    exit_type = "Stop Loss"
                
                # Check for take profit hit (if stop loss wasn't hit)
                elif is_long and high_price >= position['take_profit_price'] and not exit_price:
                    # Long position hit take profit
                    exit_price = position['take_profit_price']  # Exit at exact take profit price
                    exit_type = "Take Profit"
                elif not is_long and low_price <= position['take_profit_price'] and not exit_price:
                    # Short position hit take profit
                    exit_price = position['take_profit_price']  # Exit at exact take profit price
                    exit_type = "Take Profit"
                
                # Check for end of day exit
                elif current_idx in last_candles and not exit_price:
                    # End of day exit
                    exit_price = close_price  # Exit at close price
                    exit_type = "End of Day"
                # Add stricter check for end-of-day exit when swing trading is not allowed
                elif swing_config['swings_allowed'] == 0 and i < len(df.index) - 1 and pd.Timestamp(df.index[i]).date() != pd.Timestamp(df.index[i+1]).date() and not exit_price:
                    # End of day exit (stricter check to prevent swing trading)
                    exit_price = close_price  # Exit at close price
                    exit_type = "End of Day (No Swings)"
                
                # Process exit if conditions are met
                if exit_price:
                    # Calculate risk/reward ratio
                    risk_reward = calculate_risk_reward(
                        position['entry_price'], 
                        exit_price, 
                        position['stop_price'], 
                        is_long
                    )
                    
                    # Calculate percentage return - risk_per_trade is already in percentage form
                    perc_return = risk_reward * position['risk_per_trade']
                    
                    # Create trade record
                    trade = {
                        'entry_timestamp': position['entry_timestamp'],
                        'exit_timestamp': current_idx,
                        'entry_price': position['entry_price'],
                        'exit_price': exit_price,
                        'stop_price': position['stop_price'],
                        'position_size': position['position_size'],
                        'risk_size': position['risk_size'],
                        'risk_per_trade': position['risk_per_trade'],
                        'risk_reward': risk_reward,
                        'perc_return': perc_return,
                        'capital_required': position['capital_required'],
                        'direction': position['direction'],
                        'exit_type': exit_type
                    }
                    
                    # Add trade to list
                    trades.append(trade)
                    
                    # Log trade
                    logger.info(f"Exit at {current_idx}: ${exit_price:.2f}, type={exit_type}, R:R={risk_reward:.2f}")
                    
                    # Remove position from active positions
                    del active_positions[entry_time]
        
        # Check for any positions still open at the end of data
        for entry_time, position in list(active_positions.items()):
            # Force close at last available price
            last_idx = df.index[-1]
            last_price = df['close'].iloc[-1]
            
            risk_reward = calculate_risk_reward(
                position['entry_price'], 
                last_price, 
                position['stop_price'], 
                is_long
            )
            
            # Calculate percentage return - risk_per_trade is already in percentage form
            perc_return = risk_reward * position['risk_per_trade']
            
            # Create trade record
            trade = {
                'entry_timestamp': position['entry_timestamp'],
                'exit_timestamp': last_idx,
                'entry_price': position['entry_price'],
                'exit_price': last_price,
                'stop_price': position['stop_price'],
                'position_size': position['position_size'],
                'risk_size': position['risk_size'],
                'risk_per_trade': position['risk_per_trade'],
                'risk_reward': risk_reward,
                'perc_return': perc_return,
                'capital_required': position['capital_required'],
                'direction': position['direction'],
                'exit_type': "End of Backtest"
            }
            
            # Add trade to list
            trades.append(trade)
            logger.info(f"Forced exit at end of data: ${last_price:.2f}, R:R={risk_reward:.2f}")
        
        if not trades:
            logger.warning("No trades were generated")
            return False
        
        # Convert timestamps to datetime objects for proper calculation
        for trade in trades:
            if isinstance(trade['entry_timestamp'], str):
                trade['entry_timestamp'] = pd.to_datetime(trade['entry_timestamp'])
            if isinstance(trade['exit_timestamp'], str):
                trade['exit_timestamp'] = pd.to_datetime(trade['exit_timestamp'])
        
        # Format trades into a DataFrame
        trades_df = format_trades(trades, df, stop_config, risk_config, exit_config, swing_config)
        
        # Log trades to database
        log_trades_to_db(trades_df, run_id, symbol)
        
        return True
    except Exception as e:
        logger.error(f"Error running backtest: {e}", exc_info=True)
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