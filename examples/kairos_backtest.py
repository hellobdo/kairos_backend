import vectorbt as vbt
import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_data_from_db(symbol='QQQ'):
    """Load historical data from SQLite database."""
    conn = sqlite3.connect('kairos.db')
    
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

def log_trades(trades_df, run_id=1, strategy_id=1, symbol='QQQ'):
    """Log trades to algo_trades table."""
    conn = sqlite3.connect('kairos.db')
    cursor = conn.cursor()
    
    for _, trade in trades_df.iterrows():
        # Calculate trade duration in hours
        duration = (pd.to_datetime(trade['Exit Date']) - pd.to_datetime(trade['Entry Date'])).total_seconds() / 3600
        
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
            trade['Entry Price'] * 0.99,  # Simple stop loss at 1% below entry
            trade['Size'],
            trade['Size'] * trade['Entry Price'] * 0.01,  # Risk size (1% of position)
            0.01,  # Fixed 1% risk per trade
            abs(trade['PnL']) / (trade['Size'] * trade['Entry Price'] * 0.01),  # Risk/Reward ratio
            trade['Return %'],
            1 if trade['PnL'] > 0 else 0,  # Winning trade flag
            duration,
            trade['Size'] * trade['Entry Price'],  # Capital required
            'long'  # Only long trades in this strategy
        ))
    
    conn.commit()
    conn.close()

def run_tightness_strategy(symbol='QQQ'):
    """Run a simple strategy that trades only on Ultra Tight conditions."""
    
    # Load data
    logger.info("Loading historical data from database...")
    df = load_data_from_db(symbol)
    
    # Create entry/exit signals
    entries = (df['tightness'] == 'Ultra Tight') & (df['market_session'] == 'regular')
    exits = (df['tightness'] != 'Ultra Tight') | (df['market_session'] != 'regular')
    
    # Run backtest
    logger.info("Running backtest...")
    pf = vbt.Portfolio.from_signals(
        close=df['close'],
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001,
        freq='30min'
    )
    
    # Get trades
    trades = pf.trades.records
    
    # Format trades DataFrame
    formatted_trades = pd.DataFrame({
        'Entry Date': df.index[trades['entry_idx']].strftime('%Y-%m-%d %H:%M:%S'),
        'Exit Date': df.index[trades['exit_idx']].strftime('%Y-%m-%d %H:%M:%S'),
        'Entry Price': trades['entry_price'].round(2),
        'Exit Price': trades['exit_price'].round(2),
        'Size': trades['size'].round(2),
        'PnL': trades['pnl'].round(2),
        'Return %': (trades['return'] * 100).round(2)
    })
    
    # Print results
    print("\nTrade List:")
    print(formatted_trades)
    
    print("\nStrategy Performance:")
    print(f"Total Return: {(pf.total_return() * 100):.2f}%")
    print(f"Total Trades: {len(trades)}")
    print(f"Win Rate: {(pf.trades.win_rate() * 100):.2f}%")
    
    # Log trades to database
    logger.info("Logging trades to database...")
    log_trades(formatted_trades)
    logger.info("Trades logged successfully")

if __name__ == "__main__":
    run_tightness_strategy() 