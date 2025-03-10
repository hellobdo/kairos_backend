import vectorbt as vbt
import pandas as pd
import sqlite3
import logging
from datetime import datetime

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
STOPLOSS_CONFIG_ID = 1
RISK_CONFIG_ID = 1
STOP_LOSS_PCT = 0.01  # 1% stop loss

def create_backtest_run():
    """Create a new backtest run record and return its ID."""
    conn = sqlite3.connect('kairos.db')
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

def log_trades(trades_df, run_id, pf, trades_records, symbol, strategy_id):
    """Log trades to algo_trades table."""
    conn = sqlite3.connect('kairos.db')
    cursor = conn.cursor()
    
    for idx, trade in trades_df.iterrows():
        # Calculate trade duration in hours
        duration = (pd.to_datetime(trade['Exit Date']) - pd.to_datetime(trade['Entry Date'])).total_seconds() / 3600
        
        # Calculate stop price using configured stop loss percentage
        stop_price = trade['Entry Price'] * (1 - STOP_LOSS_PCT)
        
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

def run_tightness_strategy():
    """Run a simple strategy that trades only on Ultra Tight conditions."""
    
    # Create backtest run record
    logger.info("Creating backtest run record...")
    run_id = create_backtest_run()
    logger.info(f"Created backtest run with ID: {run_id}")
    
    # Load data
    logger.info("Loading historical data from database...")
    df = load_data_from_db(SYMBOL)
    
    # Create entry/exit signals
    entries = (df['tightness'] == 'Ultra Tight') & (df['market_session'] == 'regular')
    exits = (df['tightness'] != 'Ultra Tight') | (df['market_session'] != 'regular')
    
    # Run backtest with stop loss
    logger.info("Running backtest...")
    pf = vbt.Portfolio.from_signals(
        close=df['close'],
        entries=entries,
        exits=exits,
        sl_stop=STOP_LOSS_PCT,  # Use configured stop loss percentage
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
        'Return %': (trades['return'] * 100).round(2),
        'Cash': pf.cash().iloc[trades['entry_idx'] - 1].values,  # Get cash available BEFORE each trade
        'Direction': ['long' if d == 1 else 'short' for d in trades['direction']]  # Get trade direction
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
    log_trades(valid_trades, run_id, pf, trades, SYMBOL, STRATEGY_ID)
    logger.info("Trades logged successfully")

if __name__ == "__main__":
    run_tightness_strategy() 