import pandas as pd
import sqlite3
import logging
from datetime import datetime
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def log_trades_to_db(trades_df, run_id, symbol):
    """
    Log trades to the database from a pre-formatted DataFrame.
    
    Args:
        trades_df (pd.DataFrame): DataFrame containing all trade information
        run_id (int): ID of the backtest run
        symbol (str): Trading symbol
    """
    conn = sqlite3.connect('data/algos.db')
    cursor = conn.cursor()
    
    # First check if any trades already exist for this run_id
    cursor.execute("SELECT COUNT(*) FROM trades WHERE run_id = ?", (run_id,))
    existing_count = cursor.fetchone()[0]
    
    if existing_count > 0:
        logger.warning(f"Found {existing_count} existing trades for run_id {run_id}. Deleting before inserting new trades.")
        cursor.execute("DELETE FROM trades WHERE run_id = ?", (run_id,))
    
    for _, trade in trades_df.iterrows():
        try:
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
        except Exception as e:
            logger.error(f"Error inserting trade: {e}")
            logger.error(f"Trade data: {trade}")
    
    conn.commit()
    conn.close()
    logger.info(f"Successfully logged {len(trades_df)} trades to database for run_id {run_id}")

def prepare_trade_data(pf, price_data, stop_config, exit_config=None, stop_prices=None):
    """
    Prepare trade data from VectorBT Portfolio for database logging.
    
    Args:
        pf (vbt.Portfolio): VectorBT Portfolio object
        price_data (pd.DataFrame): Price data with datetime index
        stop_config (dict): Stop loss configuration
        exit_config (dict, optional): Exit configuration
        stop_prices (pd.Series, optional): Pre-calculated stop prices for each trade
        
    Returns:
        pd.DataFrame: DataFrame ready for database logging
    """
    # Get raw trade records from VectorBT
    trades = pf.trades.records
    
    # Create base DataFrame with VectorBT data
    trades_df = pd.DataFrame({
        'entry_timestamp': price_data.index[trades['entry_idx']].strftime('%Y-%m-%d %H:%M:%S'),
        'exit_timestamp': price_data.index[trades['exit_idx']].strftime('%Y-%m-%d %H:%M:%S'),
        'entry_price': trades['entry_price'],
        'exit_price': trades['exit_price'],
        'position_size': trades['size'].astype(int),
        'pnl': trades['pnl'],
        'direction': trades['direction'].map({0: 'long', 1: 'short'}),  # Use VectorBT's direction field
        'winning_trade': (trades['pnl'] > 0).astype(int)
    })
    
    # Calculate trade duration in hours
    trades_df['trade_duration'] = (
        (pd.to_datetime(trades_df['exit_timestamp']) - 
         pd.to_datetime(trades_df['entry_timestamp'])).dt.total_seconds() / 3600
    )
    
    # Calculate capital required
    trades_df['capital_required'] = trades_df['position_size'] * trades_df['entry_price']
    
    # Calculate stop prices if not provided
    if stop_prices is not None:
        # Use pre-calculated stop prices
        trades_df['stop_price'] = stop_prices
    else:
        # Calculate stop prices based on config
        trades_df['stop_price'] = 0.0
        
        # Vectorized calculation for common stop types
        if stop_config['stop_type'] == 'perc':
            trades_df['stop_price'] = trades_df['entry_price'] * (1 - stop_config['stop_value'])
        elif stop_config['stop_type'] == 'abs':
            trades_df['stop_price'] = trades_df['entry_price'] - stop_config['stop_value']
        else:  # custom function
            for i, row in trades_df.iterrows():
                stop_value = stop_config['stop_func'](row['entry_price'])
                trades_df.at[i, 'stop_price'] = row['entry_price'] * (1 - stop_value)
    
    # Calculate risk size
    trades_df['risk_size'] = trades_df['position_size'] * (trades_df['entry_price'] - trades_df['stop_price']).abs()
    
    # Get initial cash from VectorBT portfolio
    init_cash = pf.init_cash
    
    # Calculate risk per trade as a percentage of initial account size
    trades_df['risk_per_trade'] = trades_df['risk_size'] / init_cash * 100
    
    # Calculate risk/reward ratio - different formulas for long vs short
    long_mask = trades_df['direction'] == 'long'
    
    # For longs: (exit_price - entry_price) / (entry_price - stop_price)
    # For shorts: (entry_price - exit_price) / (stop_price - entry_price)
    trades_df['risk_reward'] = np.where(
        long_mask,
        (trades_df['exit_price'] - trades_df['entry_price']) / (trades_df['entry_price'] - trades_df['stop_price']),
        (trades_df['entry_price'] - trades_df['exit_price']) / (trades_df['stop_price'] - trades_df['entry_price'])
    )
    
    # Handle any division by zero
    trades_df['risk_reward'] = trades_df['risk_reward'].replace([np.inf, -np.inf], 0)
    
    # Calculate percentage return based on risk/reward
    trades_df['perc_return'] = trades_df['risk_reward'] * trades_df['risk_per_trade']
    
    # Determine exit type
    trades_df['exit_type'] = 'Unknown'
    
    # Check if VectorBT has exit_reason field
    if hasattr(trades, 'exit_reason') and 'exit_reason' in trades.columns:
        # Use VectorBT's exit reason if available
        exit_reason_map = {
            'sl_stop': 'Stop Loss',
            'tp_stop': 'Take Profit'
        }
        for i, exit_reason in enumerate(trades['exit_reason']):
            if exit_reason in exit_reason_map:
                trades_df.at[i, 'exit_type'] = exit_reason_map[exit_reason]
    
    return trades_df

def print_performance_summary(trades_df, pf):
    """
    Print a simple performance summary.
    
    Args:
        trades_df (pd.DataFrame): DataFrame with trade information
        pf (vbt.Portfolio): VectorBT Portfolio object
    """
    print("\nStrategy Performance Summary:")
    print(f"Total Return: {(pf.total_return() * 100):.2f}%")
    print(f"Total Trades: {len(trades_df)}")
    print(f"Win Rate: {(trades_df['winning_trade'].mean() * 100):.2f}%")
    
    # Count trades by exit type
    exit_counts = trades_df['exit_type'].value_counts()
    print("\nTrades by Exit Type:")
    for exit_type, count in exit_counts.items():
        print(f"  {exit_type}: {count}")
    
    # Average PnL by exit type
    avg_pnl = trades_df.groupby('exit_type')['pnl'].mean()
    print("\nAverage PnL by Exit Type:")
    for exit_type, pnl in avg_pnl.items():
        print(f"  {exit_type}: ${pnl:.2f}")

# Function aliases to match expected function names
def format_trades(df, trades, stop_config, risk_config, exit_config, swing_config, portfolio_kwargs):
    """
    Format trade data from VectorBT Portfolio for database logging.
    
    Args:
        df (pd.DataFrame): Price data with datetime index
        trades (pd.DataFrame): VectorBT trade records
        stop_config (dict): Stop loss configuration
        risk_config (dict): Risk configuration
        exit_config (dict): Exit configuration
        swing_config (dict): Swing configuration
        portfolio_kwargs (dict): Portfolio constructor arguments
        
    Returns:
        pd.DataFrame: DataFrame ready for database logging
    """
    # Get the trade data from VectorBT
    trades_df = prepare_trade_data(
        pf=portfolio_kwargs.get('pf'),  # Portfolio object
        price_data=df,
        stop_config=stop_config,
        exit_config=exit_config
    )
    
    # Get our custom take profit exits dictionary if available
    take_profit_exits = portfolio_kwargs.get('take_profit_exits', {})
    
    # Get target R:R
    target_rr = exit_config.get('risk_reward', 2.0) if exit_config else 2.0
    
    # Find trades that should be classified as stop losses
    for i, row in trades_df.iterrows():
        entry_price = row['entry_price']
        exit_price = row['exit_price'] 
        stop_price = row['stop_price']
        
        # Check if this is a stop loss - should be when exit price is very close to stop price
        # For longs: exit_price <= stop_price (allowing small slippage)
        # For shorts: exit_price >= stop_price (allowing small slippage)
        is_stop_loss = False
        if row['direction'] == 'long':
            # Check if exit price is at or below stop price (with small tolerance)
            price_diff_pct = abs((exit_price - stop_price) / stop_price)
            is_stop_loss = exit_price <= (stop_price * 1.01)  # Allow 1% slippage
        else:
            # For shorts, check if exit price is at or above stop price
            price_diff_pct = abs((exit_price - stop_price) / stop_price)
            is_stop_loss = exit_price >= (stop_price * 0.99)  # Allow 1% slippage
            
        # Check exit timestamp
        exit_time = pd.to_datetime(row['exit_timestamp'])
        exit_idx = df.index.get_indexer([exit_time], method='nearest')[0]
        exit_timestamp = df.index[exit_idx]
        
        # PRIORITIZED CLASSIFICATION:
        
        # 1. First, check if it's a Stop Loss based on price relationship
        if is_stop_loss:
            trades_df.at[i, 'exit_type'] = 'Stop Loss'
            # Don't modify the actual risk_reward value
            
        # 2. Next check for Take Profit - based on pre-calculated exits or actual R:R
        elif exit_timestamp in take_profit_exits:
            trades_df.at[i, 'exit_type'] = 'Take Profit'
            # Don't modify the actual risk_reward value
            
            # Log detailed info about take profits that differ from target
            target_tp_price = take_profit_exits[exit_timestamp].get('exit_price')
            actual_exit_price = row['exit_price']
            target_rr = take_profit_exits[exit_timestamp].get('rr', target_rr)
            actual_rr = row['risk_reward']
            
            if abs(actual_rr - target_rr) > 0.1:  # If difference is significant
                logger.info(f"Take Profit execution difference at {exit_timestamp}:")
                logger.info(f"  Target price: ${target_tp_price:.2f}, Actual exit: ${actual_exit_price:.2f}")
                logger.info(f"  Target R:R: {target_rr:.2f}, Actual R:R: {actual_rr:.2f}")
                logger.info(f"  Price difference: ${actual_exit_price - target_tp_price:.2f}")
                logger.info(f"  R:R difference: {actual_rr - target_rr:.2f}")
        
        # 3. If the trade actually has R:R >= target, classify it as Take Profit
        elif row['risk_reward'] >= target_rr:
            trades_df.at[i, 'exit_type'] = 'Take Profit' 
            
        # 4. Otherwise it's End of Day
        else:
            trades_df.at[i, 'exit_type'] = 'End of Day'
    
    # Log summary for verification
    logger.info(f"Classification Summary:")
    exit_counts = trades_df['exit_type'].value_counts()
    for exit_type, count in exit_counts.items():
        rr_values = trades_df[trades_df['exit_type'] == exit_type]['risk_reward']
        logger.info(f"  {exit_type}: {count} trades")
        if len(rr_values) > 0:
            logger.info(f"    Avg R:R: {rr_values.mean():.2f}, Min: {rr_values.min():.2f}, Max: {rr_values.max():.2f}")
    
    return trades_df

def log_trades(formatted_trades, run_id, pf, trades, symbol, stop_config, risk_config):
    """
    Log trades to the database.
    
    Args:
        formatted_trades (pd.DataFrame): Formatted trade data
        run_id (int): ID of the backtest run
        pf (vbt.Portfolio): VectorBT Portfolio object
        trades (pd.DataFrame): VectorBT trade records
        symbol (str): Trading symbol
        stop_config (dict): Stop loss configuration
        risk_config (dict): Risk configuration
    """
    return log_trades_to_db(formatted_trades, run_id, symbol)

def print_performance_metrics(pf, formatted_trades, exit_config):
    """
    Print performance metrics.
    
    Args:
        pf (vbt.Portfolio): VectorBT Portfolio object
        formatted_trades (pd.DataFrame): Formatted trade data
        exit_config (dict): Exit configuration
    """
    return print_performance_summary(formatted_trades, pf) 