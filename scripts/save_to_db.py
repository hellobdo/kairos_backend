#!/usr/bin/env python3
"""
Save Backtest Results to Database

This script processes trade execution data from HTML reports and saves it to the SQLite database.
It uses the HTML tearsheet file as the source of truth, rather than CSV files.

Usage:
    python save_to_db.py                      # Uses latest HTML report
    python save_to_db.py --file path/to/file.html  # Uses specific file
"""
import argparse
from pathlib import Path
import os
from datetime import datetime
import sys
import sqlite3
from contextlib import contextmanager
import pandas as pd
from bs4 import BeautifulSoup

# Database configuration
DB_PATH = Path("data/trades.db")

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

def get_latest_html_report(logs_dir: Path = None) -> Path:
    """
    Find the most recent HTML trade report file in the logs directory.
    
    Args:
        logs_dir: Optional path to logs directory. Defaults to 'logs' in current directory.
        
    Returns:
        Path to the latest HTML report file
        
    Raises:
        FileNotFoundError: If no HTML report files are found
    """
    if logs_dir is None:
        logs_dir = Path("logs")
    
    if not logs_dir.exists():
        raise FileNotFoundError(f"Logs directory not found: {logs_dir}")
    
    # Find all HTML files with 'trade_report' in the name
    report_files = list(logs_dir.glob("*trade_report*.html"))
    
    if not report_files:
        raise FileNotFoundError(f"No HTML trade report files found in {logs_dir}")
    
    # Get the latest file based on creation time
    latest_file = max(report_files, key=os.path.getctime)
    
    print(f"Latest HTML report found: {latest_file}")
    print(f"Created at: {datetime.fromtimestamp(os.path.getctime(latest_file))}")
    
    return latest_file

def map_html_to_backtest_runs(header_data, html_file_name):
    """
    Maps HTML header fields to backtest_runs table fields.
    
    Args:
        header_data (dict): Dictionary containing the extracted header data from HTML
        html_file_name (str): Name of the HTML file being processed
        
    Returns:
        dict: Mapped data ready for insertion into backtest_runs table
    """
    # Define mapping of HTML fields to database fields
    field_mapping = {
        'Report Generated': 'timestamp',
        'Strategy Name': 'strategy_name',
        'Symbols Traded': 'symbols_traded',
        'Side': 'direction',
        'Stop Loss': 'stop_loss',
        'Risk Reward': 'risk_reward',
        'Risk Per Trade': 'risk_per_trade',
        'Backtesting Start': 'backtest_start_date',
        'Backtesting End': 'backtest_end_date',
        'Source File': 'source_file'
    }
    
    # Create dictionary for database insertion
    db_data = {}
    
    # Map fields according to mapping
    for html_field, db_field in field_mapping.items():
        if html_field in header_data:
            # Handle special cases for data conversion
            if db_field == 'timestamp':
                # Convert timestamp to SQLite format
                try:
                    dt = datetime.strptime(header_data[html_field], "%Y-%m-%d %H:%M:%S")
                    db_data[db_field] = dt.isoformat()
                except ValueError:
                    db_data[db_field] = datetime.now().isoformat()
            elif db_field == 'risk_per_trade' and header_data[html_field].endswith('%'):
                # Convert percentage string to decimal
                db_data[db_field] = float(header_data[html_field].strip('%')) / 100
            elif db_field in ['stop_loss', 'risk_reward']:
                # Convert strings to floats
                try:
                    db_data[db_field] = float(header_data[html_field])
                except ValueError:
                    db_data[db_field] = None
            else:
                db_data[db_field] = header_data[html_field]
    
    # Set HTML file name as the source
    db_data['source_file'] = html_file_name
    
    return db_data

def extract_header_data(html_file_path):
    """
    Extract header data from the HTML report.
    
    Args:
        html_file_path (Path): Path to the HTML report file
        
    Returns:
        dict: Dictionary containing the extracted header data
    """
    # Read HTML file
    with open(html_file_path, 'r') as f:
        html_content = f.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the summary div that contains the header info
    summary_div = soup.find('div', class_='summary')
    if not summary_div:
        raise ValueError("Summary section not found in HTML report")
    
    # Extract all <p> elements in the summary
    header_data = {}
    for p in summary_div.find_all('p'):
        # Each <p> has format: <span class="highlight">Field Name:</span> Value
        span = p.find('span', class_='highlight')
        if span:
            field_name = span.text.strip().rstrip(':')
            # Get the text after the span
            value = p.text.replace(span.text, '').strip()
            header_data[field_name] = value
    
    return header_data

def save_to_backtest_runs(data):
    """
    Save the mapped data to the backtest_runs table.
    
    Args:
        data (dict): Dictionary containing the mapped data
        
    Returns:
        int: The ID of the inserted run
        
    Raises:
        ValueError: If the report has already been processed
    """
    # First try to find if this report was already processed
    select_sql = "SELECT run_id FROM backtest_runs WHERE source_file = :source_file"
    insert_sql = """
    INSERT INTO backtest_runs (
        timestamp, strategy_name, symbols_traded, direction,
        stop_loss, risk_reward, risk_per_trade,
        backtest_start_date, backtest_end_date, source_file
    ) VALUES (
        :timestamp, :strategy_name, :symbols_traded, :direction,
        :stop_loss, :risk_reward, :risk_per_trade,
        :backtest_start_date, :backtest_end_date, :source_file
    )
    """
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Check if report already exists
        cursor.execute(select_sql, {"source_file": data["source_file"]})
        existing_run = cursor.fetchone()
        
        if existing_run:
            raise ValueError(f"HTML already processed with run ID: {existing_run[0]}")
        
        # Insert new run
        cursor.execute(insert_sql, data)
        run_id = cursor.lastrowid
        conn.commit()
        print(f"Inserted backtest run with ID: {run_id}")
        return run_id

def extract_trade_summary(html_file_path):
    """
    Extract trade summary data from the HTML report.
    
    Args:
        html_file_path (Path): Path to the HTML report file
        
    Returns:
        list of dict: List of dictionaries containing the extracted trade summary data
    """
    # Read HTML file
    with open(html_file_path, 'r') as f:
        html_content = f.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the section containing the trade summary
    trade_summary_section = None
    for section in soup.find_all('div', class_='section'):
        if section.find('h2') and section.find('h2').text.strip() == 'Trade Summary':
            trade_summary_section = section
            break
            
    if not trade_summary_section:
        raise ValueError("Trade summary section not found in HTML report")
    
    # Find the table within the trade summary section
    trade_summary_table = trade_summary_section.find('table')
    if not trade_summary_table:
        raise ValueError("Trade summary table not found in HTML report")
    
    # Extract table headers
    headers = [th.text.strip() for th in trade_summary_table.find('thead').find_all('th')]
    
    # Extract table rows
    trade_summary_data = []
    for row in trade_summary_table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        if len(cells) != len(headers):
            continue  # Skip malformed rows
            
        # Create trade data dictionary
        trade_data = {}
        for i, header in enumerate(headers):
            value = cells[i].text.strip()
            
            # Convert numeric values
            if header in ['trade_id', 'num_executions', 'year']:
                trade_data[header] = int(value)
            elif header in ['duration_hours', 'quantity', 'entry_price', 'stop_price', 'exit_price', 'actual_risk_reward']:
                try:
                    # Remove commas from numbers like "94,291.01"
                    value = value.replace(',', '')
                    trade_data[header] = float(value)
                except ValueError:
                    trade_data[header] = None
            elif header == 'winning_trade':
                trade_data[header] = int(value)  # Convert 0/1 to integer
            elif header == 'perc_return':
                # Convert percentage string to decimal
                if value.endswith('%'):
                    value = value.rstrip('%')
                    if value.startswith('+'):
                        value = value[1:]  # Remove leading '+'
                    trade_data[header] = float(value)
                else:
                    trade_data[header] = None
            elif header == 'capital_required':
                # Remove commas and convert to float
                value = value.replace(',', '')
                trade_data[header] = float(value)
            else:
                trade_data[header] = value
                
        trade_summary_data.append(trade_data)
    
    return trade_summary_data

def save_trades_to_db(trade_summary_data, run_id):
    """
    Save the trade summary data to the backtest_trades table.
    
    Args:
        trade_summary_data (list of dict): List of dictionaries containing trade summary data
        run_id (int): The ID of the backtest run to associate with these trades
    """
    # SQL for insertion
    insert_sql = """
    INSERT INTO backtest_trades (
        trade_id, num_executions, symbol, start_date, start_time, 
        end_date, end_time, duration_hours, quantity, entry_price, 
        stop_price, exit_price, capital_required, exit_type, 
        take_profit_price, risk_reward, winning_trade, perc_return, 
        week, month, year, run_id
    ) VALUES (
        :trade_id, :num_executions, :symbol, :start_date, :start_time, 
        :end_date, :end_time, :duration_hours, :quantity, :entry_price, 
        :stop_price, :exit_price, :capital_required, :exit_type, 
        :take_profit_price, :actual_risk_reward, :winning_trade, :perc_return, 
        :week, :month, :year, :run_id
    )
    """
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for trade_data in trade_summary_data:
            # Add run_id to each trade data
            trade_data['run_id'] = run_id
            cursor.execute(insert_sql, trade_data)
        conn.commit()
    
    print(f"Inserted {len(trade_summary_data)} trades for run ID: {run_id}")

def extract_executions(html_file_path):
    """
    Extract executions data from the HTML report.
    
    Args:
        html_file_path (Path): Path to the HTML report file
        
    Returns:
        list of dict: List of dictionaries containing the executions data
    """
    # Read HTML file
    with open(html_file_path, 'r') as f:
        html_content = f.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the section containing the executions
    executions_section = None
    for section in soup.find_all('div', class_='section'):
        if section.find('h2') and section.find('h2').text.strip() == 'Processed Executions':
            executions_section = section
            break
            
    if not executions_section:
        raise ValueError("Executions section not found in HTML report")
    
    # Find the table within the executions section
    executions_table = executions_section.find('table')
    if not executions_table:
        raise ValueError("Executions table not found in HTML report")
    
    # Extract table headers
    headers = [th.text.strip() for th in executions_table.find('thead').find_all('th')]
    
    # Extract table rows
    executions_data = []
    for row in executions_table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        if len(cells) != len(headers):
            continue  # Skip malformed rows
            
        # Create execution data dictionary
        execution_data = {}
        for i, header in enumerate(headers):
            value = cells[i].text.strip()
            
            # Convert numeric values
            if header in ['filled_quantity', 'price', 'open_volume']:
                try:
                    # Remove commas from numbers like "94,291.01"
                    value = value.replace(',', '')
                    execution_data[header] = float(value)
                except ValueError:
                    execution_data[header] = None
            elif header == 'trade_id':
                execution_data[header] = int(value)
            else:
                execution_data[header] = value
                
        executions_data.append(execution_data)
    
    return executions_data

def save_executions_to_db(executions_data, run_id):
    """
    Save the executions data to the backtest_executions table.
    
    Args:
        executions_data (list of dict): List of dictionaries containing executions data
        run_id (int): The ID of the backtest run to associate with these executions
    """
    # SQL for insertion
    insert_sql = """
    INSERT INTO backtest_executions (
        execution_timestamp, date, time_of_day, identifier, symbol,
        side, filled_quantity, price, trade_id, open_volume, run_id
    ) VALUES (
        :execution_timestamp, :date, :time_of_day, :identifier, :symbol,
        :side, :filled_quantity, :price, :trade_id, :open_volume, :run_id
    )
    """
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for execution_data in executions_data:
            # Add run_id to each execution data
            execution_data['run_id'] = run_id
            cursor.execute(insert_sql, execution_data)
        conn.commit()
    
    print(f"Inserted {len(executions_data)} executions for run ID: {run_id}")

def main():
    parser = argparse.ArgumentParser(description="Save backtest results to database from HTML report")
    parser.add_argument("--file", type=str, help="Path to HTML report file")
    
    args = parser.parse_args()
    
    try:
        # Get the HTML report file
        if args.file:
            file_path = Path(args.file)
            if not file_path.exists():
                print(f"Error: File not found: {file_path}")
                return 1
        else:
            # Try to find latest file
            try:
                file_path = get_latest_html_report()
            except FileNotFoundError as e:
                print(f"Error: {str(e)}")
                return 1
        
        print(f"\nUsing HTML report file: {file_path}")
        
        # Extract header data from the HTML report
        try:
            header_data = extract_header_data(file_path)
            print("\nExtracted header data:")
            for key, value in header_data.items():
                print(f"  {key}: {value}")
        except Exception as e:
            print(f"Error extracting header data: {str(e)}")
            return 1
        
        # Map HTML header data to database fields
        db_data = map_html_to_backtest_runs(header_data, file_path.name)
        print("\nMapped data for database:")
        for key, value in db_data.items():
            print(f"  {key}: {value}")
        
        # Save to backtest_runs table - if this fails, we don't proceed with trades and executions
        try:
            run_id = save_to_backtest_runs(db_data)
        except ValueError as e:
            print(f"\nError: {str(e)}")
            print("Skipping trades and executions processing.")
            return 1
            
        print(f"\nSaved backtest run with ID: {run_id}")
        
        # Extract trade summary data from the HTML report
        try:
            trade_summary_data = extract_trade_summary(file_path)
            print(f"\nExtracted {len(trade_summary_data)} trades from trade summary")
        except Exception as e:
            print(f"Error extracting trade summary: {str(e)}")
            return 1
        
        # Save trades to backtest_trades table
        save_trades_to_db(trade_summary_data, run_id)
        
        # Extract executions data from the HTML report
        try:
            executions_data = extract_executions(file_path)
            print(f"\nExtracted {len(executions_data)} executions")
        except Exception as e:
            print(f"Error extracting executions: {str(e)}")
            return 1
        
        # Save executions to backtest_executions table
        save_executions_to_db(executions_data, run_id)
        
        print("\nDone!")
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 