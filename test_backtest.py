import os
import sqlite3
from datetime import datetime
import logging
import json
from infrastructure.core.strategy_runner import StrategyRunner
from typing import Dict, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def get_risk_config(db_path: str, risk_config_id: int) -> Dict:
    """Get risk management configuration."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT risk_per_trade, max_daily_risk
            FROM risk_manager
            WHERE id = ?
        """, (risk_config_id,))
        
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No risk config found for ID {risk_config_id}")
        
        return {
            'risk_per_trade': row[0],
            'max_daily_risk': row[1]
        }
        
    finally:
        conn.close()

def get_stoploss_config(db_path: str, stoploss_config_id: int) -> Tuple[Dict, Optional[Dict]]:
    """
    Get stop loss configuration and variable style if applicable.
    Returns (stoploss_config, variable_ranges or None)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get stop loss config
        cursor.execute("""
            SELECT type, delta_abs, delta_perc, variable_style_id
            FROM stoploss
            WHERE id = ?
        """, (stoploss_config_id,))
        
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No stop loss config found for ID {stoploss_config_id}")
        
        stoploss_config = {
            'type': row[0],
            'delta_abs': row[1],
            'delta_perc': row[2]
        }
        
        # If it's a variable stop loss, get the ranges
        if row[0] == 'variable' and row[3] is not None:
            cursor.execute("""
                SELECT vs.name, vs.description, 
                       pr.min_price, pr.max_price, pr.delta_abs, pr.delta_perc
                FROM stoploss_variable_styles vs
                JOIN stoploss_price_ranges pr ON pr.style_id = vs.id
                WHERE vs.id = ?
                ORDER BY pr.min_price
            """, (row[3],))
            
            ranges = cursor.fetchall()
            if not ranges:
                raise ValueError(f"No price ranges found for variable style ID {row[3]}")
            
            variable_config = {
                'name': ranges[0][0],
                'description': ranges[0][1],
                'price_ranges': [
                    {
                        'min_price': r[2],
                        'max_price': r[3],
                        'delta_abs': r[4],
                        'delta_perc': r[5]
                    }
                    for r in ranges
                ]
            }
            return stoploss_config, variable_config
        
        return stoploss_config, None
        
    finally:
        conn.close()

def list_strategies(db_path: str):
    """List all strategies in the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT strategy_id, strategy_name, indicator_name
            FROM algo_strategies
            ORDER BY strategy_id
        """)
        
        strategies = cursor.fetchall()
        if not strategies:
            print("\nNo strategies found in database!")
            return
        
        print("\nAvailable strategies:")
        print("ID | Name | Indicator")
        print("-" * 50)
        for strategy in strategies:
            print(f"{strategy[0]} | {strategy[1]} | {strategy[2]}")
            
    finally:
        conn.close()

def get_portfolio_strategies(db_path: str, portfolio_id: int) -> list:
    """Get all strategies for a portfolio."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT p.portfolio_name, p.total_capital,
                   s.strategy_id, s.strategy_name, s.indicator_name,
                   s.indicator_parameters, s.symbols, p.allocation_percentage
            FROM backtest_portfolios p
            JOIN algo_strategies s ON p.strategy_id = s.strategy_id
            WHERE p.portfolio_id = ?
        """, (portfolio_id,))
        
        rows = cursor.fetchall()
        
        # Parse JSON parameters and symbols
        return [
            (row[0], row[1], row[2], row[3], row[4], 
             json.loads(row[5]), json.loads(row[6]), row[7])
            for row in rows
        ]
        
    finally:
        conn.close()

def create_backtest_run(db_path: str, portfolio_id: int, risk_config_id: int, stoploss_config_id: int) -> int:
    """Create a new backtest run."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO backtest_runs (
                portfolio_id, 
                execution_date,
                risk_config_id,
                stoploss_config_id
            )
            VALUES (?, ?, ?, ?)
            RETURNING run_id
        """, (portfolio_id, datetime.now().isoformat(), risk_config_id, stoploss_config_id))
        
        run_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Created backtest run with ID: {run_id}")
        return run_id
        
    finally:
        conn.close()

def run_backtest(db_path: str, portfolio_id: int, risk_config_id: int, stoploss_config_id: int, start_date: str, end_date: str) -> Dict[str, Dict]:
    """Run backtest for a portfolio."""
    # Get portfolio strategies
    strategies = get_portfolio_strategies(db_path, portfolio_id)
    if not strategies:
        raise ValueError(f"No strategies found for portfolio {portfolio_id}")
    
    # Get risk and stop loss configurations
    risk_config = get_risk_config(db_path, risk_config_id)
    stoploss_config, variable_config = get_stoploss_config(db_path, stoploss_config_id)
    
    logger.info(f"Using risk config: {risk_config}")
    logger.info(f"Using stop loss config: {stoploss_config}")
    if variable_config:
        logger.info(f"Using variable stop loss style: {variable_config}")
    
    # Create backtest run
    run_id = create_backtest_run(db_path, portfolio_id, risk_config_id, stoploss_config_id)
    
    # Run each strategy
    results = {}
    total_capital = strategies[0][1]  # All rows have same total_capital
    
    for _, _, strategy_id, strategy_name, indicator_name, params, symbols, allocation_pct in strategies:
        logger.info(f"\nRunning strategy: {strategy_name}")
        logger.info(f"Indicator: {indicator_name}")
        logger.info(f"Parameters: {params}")
        logger.info(f"Trading symbols: {symbols}")
        
        try:
            # Create strategy runner with risk and stop loss configs
            runner = StrategyRunner(
                db_path=db_path,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                initial_capital=total_capital * (allocation_pct / 100.0),
                risk_config=risk_config,
                stoploss_config=stoploss_config,
                variable_stoploss_config=variable_config,
                run_id=run_id
            )
            
            # Get indicator instance
            indicator = runner._get_indicator(indicator_name, params)
            
            # Run strategy for each symbol
            strategy_results = {}
            for symbol in symbols:
                logger.info(f"\nProcessing {symbol}...")
                
                # Debug data loading
                logger.info(f"Data shape for {symbol}: {runner.data[symbol].shape}")
                logger.info(f"Date range: {runner.data[symbol].index[0]} to {runner.data[symbol].index[-1]}")
                logger.info(f"Sample data:\n{runner.data[symbol].head()}")
                
                # Calculate indicator values first
                signal_data = indicator.calculate(runner.data[symbol])
                logger.info(f"\nFound {signal_data['is_tight'].sum()} tight candles")
                logger.info(f"Bullish signals: {(signal_data['is_tight'] & (signal_data['trend'] == 'bullish')).sum()}")
                logger.info(f"Bearish signals: {(signal_data['is_tight'] & (signal_data['trend'] == 'bearish')).sum()}")
                
                # Generate signals
                entries, exits, sizes, capital_required, stops = runner.generate_signals(symbol, indicator, params)
                n_entries = entries.sum()
                n_exits = exits.sum()
                logger.info(f"After risk checks - Entry signals: {n_entries}, Exit signals: {n_exits}")
                
                # Run backtest
                pf = runner.backtest(symbol, strategy_id, run_id, indicator_name, params)
                strategy_results[symbol] = pf
                
                # Check trades
                if hasattr(pf, 'trades'):
                    n_trades = len(pf.trades)
                    logger.info(f"Generated {n_trades} trades")
            
            results[strategy_name] = strategy_results
            logger.info(f"Strategy {strategy_name} completed successfully")
            
        except Exception as e:
            logger.error(f"Error running strategy {strategy_name}: {e}")
            results[strategy_name] = {'error': str(e)}
    
    return results

def main():
    # Load configuration
    config_path = 'backtest_config/configs/backtest/config.json'
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Get configuration values
    db_path = config['database']['path']
    portfolio_id = config['backtest']['portfolio_id']
    risk_config_id = config['backtest']['risk_config_id']
    stoploss_config_id = config['backtest']['stoploss_config_id']
    start_date = config['backtest']['date_range']['start']
    end_date = config['backtest']['date_range']['end']
    
    # Ensure database exists
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found at {db_path}")
    
    # List available strategies first
    list_strategies(db_path)
    
    # Get portfolio strategies
    strategies = get_portfolio_strategies(db_path, portfolio_id)
    if not strategies:
        raise ValueError(f"No strategies found for portfolio {portfolio_id}")
    
    # Print portfolio info
    portfolio_name, total_capital = strategies[0][:2]  # All rows have same values
    print(f"\nPortfolio: {portfolio_name}")
    print(f"Total Capital: ${total_capital:,.2f}")
    print("\nStrategies:")
    for _, _, _, strategy_name, _, params, symbols, allocation_pct in strategies:
        print(f"- {strategy_name}: {allocation_pct:.1f}% ({', '.join(symbols)})")
    
    try:
        # Run backtest
        print("\nStarting backtest...")
        results = run_backtest(
            db_path=db_path,
            portfolio_id=portfolio_id,
            risk_config_id=risk_config_id,
            stoploss_config_id=stoploss_config_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Print results
        for strategy_name, strategy_results in results.items():
            if 'error' in strategy_results:
                print(f"\nStrategy {strategy_name} failed:")
                print(strategy_results['error'])
            else:
                print(f"\nStrategy {strategy_name} results:")
                for symbol, pf in strategy_results.items():
                    print(f"\n{symbol}:")
                    if len(pf.trades) == 0:
                        print("No trades generated")
                    else:
                        try:
                            print(f"Total Return: {float(pf.total_return):.2%}")
                            print(f"Sharpe Ratio: {float(pf.sharpe_ratio):.2f}")
                            print(f"Max Drawdown: {float(pf.max_drawdown):.2%}")
                            print(f"Total Trades: {len(pf.trades)}")
                        except (ValueError, TypeError, AttributeError) as e:
                            print(f"Error calculating metrics: {e}")
                            print(f"Total Trades: {len(pf.trades)}")
    
    except Exception as e:
        logger.error(f"Error during backtest: {e}")
        raise

if __name__ == '__main__':
    main() 