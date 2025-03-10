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

def run_tightness_strategy():
    """Run a simple strategy that trades only on Ultra Tight conditions."""
    
    # Load data
    logger.info("Loading historical data from database...")
    df = load_data_from_db('QQQ')
    
    # Create entry signals when:
    # 1. Tightness becomes Ultra Tight
    # 2. Only during regular market session
    entries = (df['tightness'] == 'Ultra Tight') & (df['market_session'] == 'regular')
    
    # Create exit signals when:
    # 1. Tightness is no longer Ultra Tight OR
    # 2. Market session ends
    exits = (df['tightness'] != 'Ultra Tight') | (df['market_session'] != 'regular')
    
    # Run backtest
    logger.info("Running backtest...")
    pf = vbt.Portfolio.from_signals(
        close=df['close'],
        entries=entries,
        exits=exits,
        init_cash=10000,  # Starting with $10,000
        fees=0.001,  # 0.1% trading fee
        freq='30min'  # Specify the data frequency
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
    
    # Calculate average return manually
    avg_return = trades['return'].mean() * 100 if len(trades) > 0 else 0
    
    # Set display options
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    
    # Print results
    print("\nTrade List:")
    print(formatted_trades)
    
    print("\nStrategy Performance:")
    print(f"Total Return: {(pf.total_return() * 100):.2f}%")
    print(f"Total Trades: {len(trades)}")
    print(f"Win Rate: {(pf.trades.win_rate() * 100):.2f}%")
    print(f"Average Trade Return: {avg_return:.2f}%")
    print(f"Max Drawdown: {(pf.max_drawdown() * 100):.2f}%")
    print(f"Sharpe Ratio: {pf.sharpe_ratio():.2f}")
    
    # Additional statistics
    print("\nDetailed Statistics:")
    print(f"Best Trade: ${trades['pnl'].max():.2f}")
    print(f"Worst Trade: ${trades['pnl'].min():.2f}")
    print(f"Average PnL per Trade: ${trades['pnl'].mean():.2f}")
    
    # Calculate unique trading days using pandas
    unique_days = df.index.normalize().nunique()
    print(f"Total Trading Days: {unique_days}")
    print(f"Average Trades per Day: {len(trades) / unique_days:.2f}")

if __name__ == "__main__":
    run_tightness_strategy() 