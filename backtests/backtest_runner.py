import os
import sys
import subprocess
from pathlib import Path
import argparse
from backtests.utils import process_csv_to_executions, process_executions_to_trades
from analytics.trade_results import run_report

def get_backtest_files_for_display():
    """Get all backtest files from backtests/backtests directory.
    
    Returns:
        dict: Dictionary with file names as keys and full paths as values
    """
    backtest_dir = Path("backtests/backtests")
    files = {}
    
    if backtest_dir.exists():
        for file in backtest_dir.glob("*"):
            if file.is_file():
                files[file.name] = str(file)
    
    return files

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

def get_latest_trades_files():
    """
    Find the most recently created custom_trades.csv files in the logs directory.
    
    Returns:
        str: trades_file path. None if not found.
    """
    try:
        logs_dir = Path('logs')
        if not logs_dir.exists():
            print("Logs directory not found")
            return None
            
        # Find all matching files and their creation times
        trades_files = [(f, f.stat().st_ctime) for f in logs_dir.glob('*custom_trades.csv')]
        
        # Get the most recent files
        latest_trades = max(trades_files, key=lambda x: x[1])[0] if trades_files else None
        
        return str(latest_trades)
        
    except Exception as e:
        print(f"Error finding latest files: {str(e)}")
        return None

def run_backtest(file_path):
    """
    Run a backtest file and process its results.
    
    Args:
        file_path (str): Path to the Python file containing the Strategy class
        
    Returns:
        tuple: (executions_df, trades_df, reports) containing the processed data and reports
    """
    try:
        # Convert file path to module path (e.g. backtests/backtests/dt-tshaped.py -> backtests.backtests.dt_tshaped)
        rel_path = os.path.relpath(file_path)
        module_path = os.path.splitext(rel_path)[0].replace('/', '.').replace('-', '_')
        
        # Run the strategy file as a module
        result = subprocess.run([sys.executable, '-m', module_path], check=True)
        if result.returncode != 0:
            raise Exception(f"Backtest failed with return code {result.returncode}")
        
        # Get latest files
        trades_file = get_latest_trades_files()
        if not trades_file:
            raise Exception("Could not find output files after running backtest")
            
        # Process the trades file
        executions_df, trades_df = process_data(trades_file)
        if executions_df is None or trades_df is None:
            raise Exception("Failed to process backtest data")
            
        # Generate reports
        reports = generate_reports(trades_df)
        if not reports:
            raise Exception("Failed to generate reports")
                
        return executions_df, trades_df, reports
        
    except Exception as e:
        print(f"Error in backtest pipeline: {str(e)}")
        return None, None, None

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
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run backtest and process results')
    parser.add_argument('--file', type=str, default="backtests/backtests/dt-tshaped.py", help='Path to strategy file')
    args = parser.parse_args()

    # Run the backtest pipeline
    executions_df, trades_df, reports = run_backtest(args.file)
    
    if executions_df is not None:
        print("\nBacktest completed successfully:")
        print(f"Executions shape: {executions_df.shape}")
        print(f"Trades shape: {trades_df.shape}")
        print("\nReports generated for:")
        for period in reports.keys():
            print(f"- {period}")