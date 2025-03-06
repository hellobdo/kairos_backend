import sqlite3
import pandas as pd
from datetime import datetime

# Read CSV data
columns_to_select = ['prev_trade_id', 'symbol', 'strategy', 'entry_type', 'direction', 'entry_date', 'entry_timestamp', 'instrument_type', 'quantity', 'entry_price', 'stop_price', 'exit_price', 'exit_date', 'exit_timestamp', 'capital_required', 'trade_duration', 'winning_trade', 'risk_reward', 'risk_per_trade', 'perc_return', 'risk_size']
csv_data = pd.read_csv('../migration/files/improved_trades.csv', usecols=columns_to_select)

# Connect to the database
conn = sqlite3.connect("../../kairos.db")
cursor = conn.cursor()

# Insert data into improved_trades table
columns_to_insert = ['symbol', 'strategy', 'entry_type', 'direction', 'entry_date', 'entry_timestamp', 'instrument_type', 
                     'quantity', 'entry_price', 'stop_price', 'exit_date', 'exit_timestamp', 'capital_required', 
                     'trade_duration', 'winning_trade', 'risk_reward', 'risk_per_trade', 'perc_return', 'risk_size', 'prev_trade_id']


# Drop the table if it exists
cursor.execute("DROP TABLE IF EXISTS improved_trades;")
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS improved_trades (
    improved_trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL,
    entry_type TEXT,
    direction TEXT NOT NULL,
    entry_date TEXT NOT NULL,
    entry_timestamp TEXT,
    instrument_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    stop_price REAL NOT NULL,
    exit_date TEXT NOT NULL,
    exit_timestamp TEXT,
    capital_required REAL NOT NULL,
    trade_duration REAL NOT NULL,
    winning_trade INTEGER NOT NULL,
    risk_reward REAL NOT NULL,
    risk_per_trade REAL NOT NULL,
    perc_return REAL NOT NULL,
    prev_trade_id INTEGER,
    risk_size REAL NOT NULL
);
""")


for _, row in csv_data.iterrows():
    try:
        cursor.execute(f"""
            INSERT INTO improved_trades ({', '.join(columns_to_insert)}) 
            VALUES ({', '.join(['?'] * len(columns_to_insert))})
        """, tuple(row[columns_to_insert]))
    except Exception as e:
        print(f"Error inserting row {row.to_dict()}: {e}")

# Commit and close the connection
conn.commit()
conn.close()

print("Data insertion completed successfully.")