import vectorbt as vbt
import pandas as pd
import sqlite3
import logging
from datetime import datetime
import numpy as np
from helpers.get_stoploss_config import get_stoploss_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Strategy configuration
SYMBOL = 'QQQ'
STRATEGY_ID = 1
PORTFOLIO_ID = 1
STOPLOSS_CONFIG_ID = 3
RISK_CONFIG_ID = 1

def create_backtest_run():
    """Create a new backtest run record and return its ID."""
    conn = sqlite3.connect('data/algos.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO backtest_runs (
            portfolio_id,
            execution_date,
            stoploss_config_id,
            risk_config_id
        ) VALUES (?, ?, ?, ?)
    """, (
        PORTFOLIO_ID,
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        STOPLOSS_CONFIG_ID,
        RISK_CONFIG_ID
    ))
    
    run_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return run_id

def load_data_from_db(symbol):
    """Load historical data from SQLite database."""
    conn = sqlite3.connect('data/algos.db')
    
    query = """
    SELECT 
        date_and_time,
        open,
        high,
        low,
        close,
        volume,
        tightness,
        market_session
    FROM historical_data_30mins
    WHERE symbol = ? 
    ORDER BY date_and_time
    """
    
    # Load data into DataFrame
    df = pd.read_sql_query(query, conn, params=(symbol,))
    
    # Convert date_and_time to datetime index
    df['date_and_time'] = pd.to_datetime(df['date_and_time'])
    df.set_index('date_and_time', inplace=True)
    
    conn.close()
    return df

def log_trades(trades_df, run_id, pf, trades_records, symbol, strategy_id, stop_config):
    """Log trades to algo_trades table."""
    conn = sqlite3.connect('data/algos.db')
    cursor = conn.cursor()
    
    for idx, trade in trades_df.iterrows():
        # Calculate trade duration in hours
        duration = (pd.to_datetime(trade['Exit Date']) - pd.to_datetime(trade['Entry Date'])).total_seconds() / 3600
        
        # Calculate stop price based on the stoploss configuration
        entry_price = trade['Entry Price']
        if stop_config['stop_type'] == 'perc':
            stop_price = entry_price * (1 - stop_config['stop_value'])
        elif stop_config['stop_type'] == 'abs':
            stop_price = entry_price - stop_config['stop_value']
        else:  # custom function
            stop_value = stop_config['stop_func'](entry_price)
            stop_price = entry_price * (1 - stop_value)
        
        # Fixed risk per trade (1%)
        risk_per_trade = 0.01
        
        # Calculate risk size based on cash before trade
        risk_size = risk_per_trade * pf.cash().iloc[trades_records['entry_idx'][idx] - 1]
        
        # Calculate position size based on risk
        position_size = round(risk_size / abs(trade['Entry Price'] - stop_price))
        
        cursor.execute("""
            INSERT INTO algo_trades (
                run_id,
                strategy_id,
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
                direction
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id,
            strategy_id,
            symbol,
            trade['Entry Date'],
            trade['Exit Date'],
            trade['Entry Price'],
            trade['Exit Price'],
            stop_price,
            position_size,  # Using calculated position size
            risk_size,
            risk_per_trade * 100,
            abs(trade['PnL']) / (trade['Size'] * trade['Entry Price'] * 0.01),  # Risk/Reward ratio using 1% of position
            trade['Return %'] * risk_per_trade / 0.01,  # Scale return by risk (if risk_per_trade=0.5%, return will be halved)
            1 if trade['PnL'] > 0 else 0,  # Winning trade flag
            duration,
            position_size * trade['Entry Price'],  # Capital required using new position size
            trade['Direction']  # Use the direction from VectorBT
        ))
    
    conn.commit()
    conn.close()

def run_backtest(df, entries, exits, stop_config):
    """
    Run a backtest with the given entry and exit signals.
    
    Args:
        df (pd.DataFrame): Price data with market session and tightness
        entries (pd.Series): Boolean series for entries
        exits (pd.Series): Boolean series for exits
        stop_config (dict): Stoploss configuration from get_stoploss_config
    """
    # Debug print signals
    print("\nSignal Analysis:")
    print(f"Entry signals: {entries.sum()}")
    print(f"Exit signals: {exits.sum()}")
    print("\nFirst 5 entry dates:")
    print(df.index[entries][:5])
    
    # Create signal DataFrame for debugging
    signal_df = pd.DataFrame({
        'close': df['close'],
        'entry': entries,
        'exit': exits
    })
    
    # Show first 10 rows where we have any signals
    has_signals = entries | exits
    signal_sample = signal_df[has_signals].head(10)
    print("\nFirst 10 rows with signals:")
    print(signal_sample)
    
    # Prepare portfolio kwargs
    portfolio_kwargs = {
        'close': df['close'],
        'entries': entries,
        'exits': exits,
        'init_cash': 10000,
        'size': 1.0,  # Positive size for longs
        'fees': 0.001,
        'freq': '30min',
        'upon_opposite_entry': 'ignore',
        'upon_long_conflict': 'ignore',
        'accumulate': False
    }
    
    # Add stoploss configuration
    if stop_config['stop_type'] == 'perc':
        portfolio_kwargs['sl_stop'] = stop_config['stop_value']
    elif stop_config['stop_type'] == 'abs':
        portfolio_kwargs['sl_stop_abs'] = stop_config['stop_value']
    elif stop_config['stop_type'] == 'custom':
        portfolio_kwargs['sl_stop_custom'] = stop_config['stop_func']
    
    # Debug print portfolio configuration
    print("\nPortfolio Configuration:")
    for key, value in portfolio_kwargs.items():
        if isinstance(value, (pd.Series, np.ndarray)):
            print(f"{key}: Series with {sum(value)} True values")
        elif callable(value):
            print(f"{key}: Custom function")
        else:
            print(f"{key}: {value}")
    
    # Run backtest
    logger.info("Running backtest...")
    pf = vbt.Portfolio.from_signals(**portfolio_kwargs)
    
    return pf

def run_tightness_strategy():
    """Run a simple strategy that trades only on Ultra Tight conditions."""
    
    # Get stoploss configuration
    logger.info(f"Getting stoploss configuration for ID {STOPLOSS_CONFIG_ID}...")
    stop_config = get_stoploss_config(STOPLOSS_CONFIG_ID)
    if not stop_config:
        logger.error("Failed to get stoploss configuration")
        return
    
    # Create backtest run record
    logger.info("Creating backtest run record...")
    run_id = create_backtest_run()
    logger.info(f"Created backtest run with ID: {run_id}")
    
    # Load data
    logger.info("Loading historical data from database...")
    df = load_data_from_db(SYMBOL)
    
    # Create entry/exit signals
    is_regular_session = (df['market_session'] == 'regular')
    is_ultra_tight = (df['tightness'] == 'Ultra Tight')
    
    # Long signals - enter when ultra tight during regular session
    entries = is_ultra_tight & is_regular_session
    exits = ~is_ultra_tight & is_regular_session  # Exit when not ultra tight
    
    print("\nSignal Generation:")
    print(f"Regular session periods: {is_regular_session.sum()}")
    print(f"Ultra tight periods: {is_ultra_tight.sum()}")
    print(f"NOT ultra tight periods: {(~is_ultra_tight).sum()}")
    
    # Add detailed signal analysis
    print("\nDetailed Signal Analysis:")
    print(f"  Total entry signals: {entries.sum()}")
    print(f"  Total exit signals: {exits.sum()}")
    
    # Sample of signal dates
    print("\nSignal Dates Sample:")
    print("First 3 entry dates:")
    print(df.index[entries][:3])
    
    # Create a sample DataFrame showing signals
    signal_sample = pd.DataFrame({
        'close': df['close'],
        'tightness': df['tightness'],
        'market_session': df['market_session'],
        'entry': entries,
        'exit': exits
    })
    print("\nFirst 10 rows with any signals:")
    has_signals = entries | exits
    print(signal_sample[has_signals].head(10))
    
    # Run backtest
    pf = run_backtest(df, entries, exits, stop_config)
    
    # Get trades
    trades = pf.trades.records
    
    # Debug print trade structure
    print("\nTrade Records Structure:")
    print(f"Type of trades: {type(trades)}")
    print("\nTrade records columns:")
    print(trades.columns)
    print("\nFirst few trades:")
    print(trades.head())
    
    # More detailed trade analysis
    print("\nDetailed Trade Analysis:")
    print(f"Total trades: {len(trades)}")
    print("\nFirst few trades:")
    for i, (_, trade) in enumerate(trades.iterrows()):
        if i >= 5: break  # Only show first 5 trades
        print(f"Trade {i}:")
        print(f"  Size: {trade.size}")
        print(f"  Entry Price: {trade.entry_price}")
        print(f"  Exit Price: {trade.exit_price}")
        print(f"  Entry Index: {trade.entry_idx}")
        print(f"  Exit Index: {trade.exit_idx}")
        print(f"  PnL: {trade.pnl}")
        print(f"  Return: {trade['return']}")  # return is a reserved word, need to use ['return']
        print()
    
    # Format trades DataFrame
    formatted_trades = pd.DataFrame({
        'Entry Date': df.index[trades['entry_idx']].strftime('%Y-%m-%d %H:%M:%S'),
        'Exit Date': df.index[trades['exit_idx']].strftime('%Y-%m-%d %H:%M:%S'),
        'Entry Price': trades['entry_price'].round(2),
        'Exit Price': trades['exit_price'].round(2),
        'Size': trades['size'].round(2),
        'PnL': trades['pnl'].round(2),
        'Return %': (trades['return'] * 100).round(2),
        'Cash': pf.cash().iloc[trades['entry_idx'] - 1].values,  # Get cash available BEFORE each trade
        'Direction': 'long'  # All trades are long
    })
    
    # Debug print all trades before filtering
    print("\nAll trades before filtering:")
    print(formatted_trades[['Entry Date', 'Cash', 'Entry Price', 'Size']])
    print("\nCash series info:")
    print(pf.cash().describe())
    
    # Filter out trades where cash was zero
    valid_trades = formatted_trades[formatted_trades['Cash'] > 0].copy()
    logger.info(f"Filtered out {len(formatted_trades) - len(valid_trades)} trades with zero cash")
    
    # Debug print cash values
    print("\nCash values at trade entries:")
    print(valid_trades[['Entry Date', 'Cash']])
    
    # Print results
    print("\nTrade List:")
    print(valid_trades)
    
    print("\nStrategy Performance:")
    print(f"Total Return: {(pf.total_return() * 100):.2f}%")
    print(f"Total Trades: {len(valid_trades)}")
    print(f"Win Rate: {(pf.trades.win_rate() * 100):.2f}%")
    
    # Log trades to database
    logger.info("Logging trades to database...")
    log_trades(valid_trades, run_id, pf, trades, SYMBOL, STRATEGY_ID, stop_config)
    logger.info("Trades logged successfully")

if __name__ == "__main__":
    run_tightness_strategy() 