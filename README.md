# Kairos Trading System

A modular, extensible backtesting and trading system built designed for developing and testing systematic trading strategies.

## Scripts

The Kairos system includes several utility scripts to help manage data, process trades, and maintain the system.

### process_trades.py

**Purpose:** Analyzes trade execution data by identifying and grouping related executions into complete trades.

**Functionality:**
- Scans the logs directory for the most recent trades CSV file
- Cleans timestamps and removes unnecessary columns
- Tracks open positions per symbol to identify complete trades (entry to exit)
- Generates trade summaries with metrics like duration, volume, and average price
- Creates separate date and time columns for easier analysis
- Generates HTML reports with trade data and statistics in a user-friendly format

**HTML Reports:**
The script automatically generates HTML reports in the `reports` directory, with tables showing:
- Summary statistics (total trades, symbols traded)
- Complete trade summary with metrics for each trade
- Recent trade executions with details

To use the HTML report functionality directly:
```python
from process_trades import get_latest_trades_file, clean_trades_file, identify_trades, generate_html_report

# Process the data
file_path = get_latest_trades_file()
cleaned_data = clean_trades_file(file_path)
trades_df, trades_summary = identify_trades(cleaned_data)

# Generate an HTML report
report_file = generate_html_report(trades_df, trades_summary, "my_trade_report.html")
```

### clean_logs.py

**Purpose:** Utility script to delete all log files when they're no longer needed.

**Functionality:**
- Removes all files from the logs directory
- Calculates and displays total size of files to be deleted
- Requires confirmation before deletion (can be bypassed)
- Reports success/failure statistics after cleaning

### to_csv.py

**Purpose:** Exports market data from the SQLite database to CSV format for analysis.

**Functionality:**
- Extracts data for specified symbols from the historical database
- Saves each symbol's data as a separate CSV file
- Creates the output directory if it doesn't exist
- Reports the number of records exported for each symbol