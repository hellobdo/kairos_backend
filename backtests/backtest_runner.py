import os
import sys
import runpy
from pathlib import Path
import argparse
from backtests.utils.process_executions import process_csv_to_executions, process_executions_to_trades
from backtests.utils.backtest_data_to_db import insert_to_db
from analytics.trade_results import run_report

def process_data(trades_file):
    """
    Process the trades file through both processing steps.
    
    Args:
        trades_file (str): Path to the trades CSV file
        
    Returns:
        tuple: (executions_df, trades_df), where:
            - executions_df: DataFrame with processed executions
            - trades_df: DataFrame with processed trades
            Returns (None, None) if any step fails
    """
    try:
        # Process CSV to executions
        executions_df = process_csv_to_executions(trades_file)
        if executions_df is False:
            print("Failed to process CSV to executions")
            return None, None
            
        # Process executions to trades
        trades_df = process_executions_to_trades(executions_df)
        if trades_df is False:
            print("Failed to process executions to trades")
            return None, None
            
        return executions_df, trades_df
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return None, None

def get_latest_backtest_files():
    """
    Find the most recently created settings.json and trades.csv files in the logs directory.
    
    Returns:
        tuple: (settings_file, trades_file) paths. None for either if not found.
    """
    try:
        logs_dir = Path('logs')
        if not logs_dir.exists():
            print("Logs directory not found")
            return None, None
            
        # Find all matching files and their creation times
        settings_files = [(f, f.stat().st_ctime) for f in logs_dir.glob('*_settings.json')]
        trades_files = [(f, f.stat().st_ctime) for f in logs_dir.glob('*_trades.csv')]
        
        # Get the most recent files
        latest_settings = max(settings_files, key=lambda x: x[1])[0] if settings_files else None
        latest_trades = max(trades_files, key=lambda x: x[1])[0] if trades_files else None
        
        return str(latest_settings), str(latest_trades)
        
    except Exception as e:
        print(f"Error finding latest files: {str(e)}")
        return None, None

def run_backtest(file_path):
    """
    Run a backtest file directly.
    
    Args:
        file_path (str): Path to the Python file containing the Strategy class
        
    Returns:
        The result from running the backtest file
    """
    try:
        # Add the project root to Python path
        sys.path.insert(0, os.path.dirname(os.path.dirname(file_path)))
        
        # Run the file
        return runpy.run_path(file_path)
        
    except Exception as e:
        print(f"Error running backtest: {str(e)}")
        return None

def generate_reports(trades_df):
    """
    Generate reports for different time periods.
    
    Args:
        trades_df (pd.DataFrame): DataFrame containing processed trades
        
    Returns:
        dict: Dictionary containing reports for week, month, and year periods
    """
    try:
        reports = {}
        for period in ['week', 'month', 'year']:
            reports[period] = run_report(trades_df, period)
        return reports
    except Exception as e:
        print(f"Error generating reports: {str(e)}")
        return None

if __name__ == "__main__":
    # Parse only the db flag
    parser = argparse.ArgumentParser(description='Run backtest and process results')
    parser.add_argument('--db', action='store_true', help='Also insert data into database')
    args = parser.parse_args()

    # Example usage
    strategy_file = "backtests/dt-tshaped.py"
    result = run_backtest(strategy_file)
    
    # Get latest files
    settings_file, trades_file = get_latest_backtest_files()
    if settings_file and trades_file:
        print(f"Latest settings file: {settings_file}")
        print(f"Latest trades file: {trades_file}")
        
        # Process the trades file
        executions_df, trades_df = process_data(trades_file)
        if executions_df is not None and trades_df is not None:
            print("Successfully processed data:")
            print(f"Executions shape: {executions_df.shape}")
            print(f"Trades shape: {trades_df.shape}")
            
            # Generate reports
            reports = generate_reports(trades_df)
            if reports:
                print("\nGenerated reports for different time periods:")
                for period, report in reports.items():
                    print(f"\n{period.capitalize()} Report:")
                    print(report)
            
            # If --db flag is set, also insert into database
            if args.db:
                print("\nInserting data into database...")
                success = insert_to_db(trades_file, settings_file)
                if success:
                    print("Successfully inserted data into database")
                else:
                    print("Failed to insert data into database")
