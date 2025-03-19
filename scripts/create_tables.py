#!/usr/bin/env python3
"""
Create SQLite database tables for storing backtest results.
"""
import sqlite3
from pathlib import Path

# Ensure data directory exists
Path("data").mkdir(exist_ok=True)

# Connect to database
conn = sqlite3.connect("data/trades.db")
cursor = conn.cursor()

# Drop existing tables if they exist
cursor.executescript('''
DROP TABLE IF EXISTS backtest_trades;
DROP TABLE IF EXISTS backtest_executions;
''')

# Create backtest_executions table
cursor.execute('''
CREATE TABLE backtest_executions (
    execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_timestamp TEXT NOT NULL,
    date TEXT NOT NULL,
    time_of_day TEXT NOT NULL,
    identifier TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    filled_quantity DECIMAL(18,8) NOT NULL,
    price DECIMAL(18,8) NOT NULL,
    trade_id INTEGER NOT NULL,
    open_volume DECIMAL(18,8) NOT NULL,
    run_id INTEGER NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id)
)
''')

# Create backtest_trades table
cursor.execute('''
CREATE TABLE backtest_trades (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    num_executions INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    start_date TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_date TEXT NOT NULL,
    end_time TEXT NOT NULL,
    duration_hours DECIMAL(10,2) NOT NULL,
    quantity REAL NOT NULL,
    entry_price REAL NOT NULL,
    stop_price REAL,
    exit_price REAL NOT NULL,
    capital_required REAL NOT NULL,
    exit_type TEXT NOT NULL,
    take_profit_price REAL,
    risk_reward REAL NOT NULL,
    winning_trade BOOLEAN NOT NULL,
    perc_return REAL NOT NULL,
    week TEXT NOT NULL,
    month TEXT NOT NULL,
    year INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id)
    )
''')

# Create indexes for better query performance
cursor.executescript('''
-- Executions indexes
CREATE INDEX idx_executions_symbol ON backtest_executions(symbol);
CREATE INDEX idx_executions_timestamp ON backtest_executions(execution_timestamp);
CREATE INDEX idx_executions_run ON backtest_executions(run_id);

-- Trades indexes
CREATE INDEX idx_trades_symbol ON backtest_trades(symbol);
CREATE INDEX idx_trades_dates ON backtest_trades(start_date, end_date);
CREATE INDEX idx_trades_year_month ON backtest_trades(year, month);
''')

# Commit changes and close connection
conn.commit()
conn.close()

print("Tables created successfully!") 