import pandas as pd
import sqlite3
from typing import List, Optional
from tight_candle import TightCandle
import os
from datetime import datetime

def get_available_data(db_path: str) -> tuple:
    """
    Get all available symbols and date range from the database.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Tuple of (symbols list, min date, max date)
    """
    conn = sqlite3.connect(db_path)
    
    # Get unique symbols
    symbols_query = "SELECT DISTINCT symbol FROM historical_data_30mins ORDER BY symbol"
    symbols = pd.read_sql_query(symbols_query, conn)['symbol'].tolist()
    
    # Get date range
    date_query = """
    SELECT 
        MIN(date_and_time) as min_date,
        MAX(date_and_time) as max_date
    FROM historical_data_30mins
    """
    dates = pd.read_sql_query(date_query, conn)
    min_date = dates['min_date'].iloc[0]
    max_date = dates['max_date'].iloc[0]
    
    conn.close()
    
    return symbols, min_date, max_date

def load_market_data(db_path: str, 
                    symbols: List[str], 
                    start_date: Optional[str] = None, 
                    end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Load market data from the database.
    
    Args:
        db_path: Path to SQLite database
        symbols: List of symbols to load
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        
    Returns:
        DataFrame with OHLCV data
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT date_and_time, symbol, open, high, low, close, volume
    FROM historical_data_30mins
    WHERE symbol IN ({})
    {}
    AND market_session = 'regular'
    ORDER BY date_and_time
    """.format(
        ','.join(['?'] * len(symbols)),
        "AND date_and_time BETWEEN ? AND ?" if start_date and end_date else ""
    )
    
    params = symbols
    if start_date and end_date:
        params += [start_date, end_date]
    
    df = pd.read_sql_query(query, conn, params=params)
    
    # Convert date_and_time to datetime
    df['date_and_time'] = pd.to_datetime(df['date_and_time'])
    
    conn.close()
    
    return df

def analyze_tight_candles(db_path: str,
                         symbols: Optional[List[str]] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         tightness_threshold: float = 0.1,
                         context_bars: int = 20,
                         save_plots: bool = True,
                         output_dir: str = "analysis/tight_candles") -> None:
    """
    Analyze and visualize tight candles for the given symbols.
    
    Args:
        db_path: Path to SQLite database
        symbols: List of symbols to analyze (if None, analyzes all available symbols)
        start_date: Start date in YYYY-MM-DD format (if None, uses earliest available)
        end_date: End date in YYYY-MM-DD format (if None, uses latest available)
        tightness_threshold: Maximum ratio for tight candles
        context_bars: Number of bars to show before and after the signal
        save_plots: Whether to save plots to files
        output_dir: Directory to save plots and results
    """
    # Get available data if not specified
    available_symbols, min_date, max_date = get_available_data(db_path)
    
    # Handle symbols and dates
    if symbols is None:
        symbols = available_symbols
    if start_date is None:
        start_date = min_date
    if end_date is None:
        end_date = max_date
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Load data
    print(f"Loading data for {len(symbols)} symbols from {start_date} to {end_date}...")
    df = load_market_data(db_path, symbols, start_date, end_date)
    
    # Initialize indicator
    indicator = TightCandle(tightness_threshold=tightness_threshold, context_bars=context_bars)
    
    # Prepare results summary
    results = []
    
    # Analyze each symbol
    for symbol in symbols:
        print(f"\nAnalyzing {symbol}...")
        
        # Create symbol directory
        symbol_dir = os.path.join(output_dir, symbol)
        os.makedirs(symbol_dir, exist_ok=True)
        
        # Get symbol data
        symbol_data = df[df['symbol'] == symbol].set_index('date_and_time')
        
        if len(symbol_data) == 0:
            print(f"No data found for {symbol}")
            continue
        
        # Calculate signals
        signals = indicator.calculate(symbol_data)
        
        # Create overview plot
        fig = indicator.plot(symbol_data, signals, 
                           title=f"Tight Candle Analysis - {symbol}")
        
        if save_plots:
            # Save overview plot
            fig.write_html(os.path.join(symbol_dir, "summary.html"))
        else:
            fig.show()
        
        # Get tight candles
        tight_candles = signals[signals['is_tight']]
        bullish_signals = len(tight_candles[tight_candles['trend'] == 'bullish'])
        bearish_signals = len(tight_candles[tight_candles['trend'] == 'bearish'])
        
        # Create detailed plots for each signal
        signal_details = []
        for i, (idx, _) in enumerate(tight_candles.iterrows(), 1):
            signal_idx = symbol_data.index.get_loc(idx)
            
            # Create detailed plot
            detail_fig = indicator.plot_signal_detail(
                symbol_data,
                signals,
                signal_idx,
                title=f"Tight Candle Detail - {symbol} - Signal {i}"
            )
            
            # Save detail plot
            if save_plots:
                timestamp = idx.strftime('%Y%m%d_%H%M')
                filename = f"signal_{timestamp}.html"
                detail_fig.write_html(os.path.join(symbol_dir, filename))
            
            # Store signal details
            signal_info = tight_candles.loc[idx]
            signal_details.append({
                'signal_number': i,
                'timestamp': idx,
                'trend': signal_info['trend'],
                'tightness': signal_info['tightness'],
                'body_size': signal_info['body_size'],
                'total_size': signal_info['total_size'],
                'upper_wick': signal_info['upper_wick'],
                'lower_wick': signal_info['lower_wick']
            })
        
        # Save signal details
        if signal_details:
            pd.DataFrame(signal_details).to_csv(
                os.path.join(symbol_dir, "signals.csv"),
                index=False
            )
        
        # Store symbol results
        results.append({
            'symbol': symbol,
            'total_candles': len(signals),
            'tight_candles': len(tight_candles),
            'tight_candles_pct': len(tight_candles)/len(signals)*100,
            'bullish_signals': bullish_signals,
            'bearish_signals': bearish_signals,
            'first_date': symbol_data.index.min(),
            'last_date': symbol_data.index.max()
        })
        
        # Print statistics
        print(f"Total candles: {len(signals)}")
        print(f"Tight candles: {len(tight_candles)} ({len(tight_candles)/len(signals)*100:.2f}%)")
        print(f"Bullish signals: {bullish_signals}")
        print(f"Bearish signals: {bearish_signals}")
        print(f"Date range: {symbol_data.index.min()} to {symbol_data.index.max()}")
        print("-" * 50)
    
    # Save summary results
    results_df = pd.DataFrame(results)
    results_df.to_csv(os.path.join(output_dir, "analysis_summary.csv"), index=False)
    print(f"\nAnalysis complete. Results saved to {output_dir}")

if __name__ == "__main__":
    db_path = "/Users/brunodeoliveira/Library/Mobile Documents/com~apple~CloudDocs/repos/kairos/kairos.db"
    analyze_tight_candles(
        db_path=db_path,
        symbols=['QQQ'],
        tightness_threshold=0.1,
        context_bars=20,
        save_plots=True,
        output_dir='analysis/tight_candles'
    ) 