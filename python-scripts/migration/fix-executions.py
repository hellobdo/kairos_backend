# csv file only from trades 310 since that has a strategy assigned to it
import sqlite3
import pandas as pd
from datetime import datetime

columns_to_select = ['prev_trade_id', 'accountId', 'symbol', 'quantity', 'price', 'netCashWithBillable', 'date_and_time', 'commission']
df = pd.read_csv('../migration/files/executions.csv', usecols=columns_to_select)

# Connect to the SQLite database (create if not exists)
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Drop the table if it exists
cursor.execute("DROP TABLE IF EXISTS executions")

# Recreate the table
cursor.execute("""
    CREATE TABLE executions (
        execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id INTEGER NOT NULL,
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price REAL NOT NULL,
        net_cash_with_billable REAL NOT NULL,
        date_and_time TEXT NOT NULL,
        date TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        commission REAL NOT NULL,
        prev_trade_id INTEGER
    )
""")

# Initialize a dictionary to track open positions and their corresponding trade_ids
open_positions = {}

# Initialize a dictionary to track the last assigned trade_id
last_trade_id = 0

# Function to calculate trade_id based on the rules
def calculate_trade_id(symbol, quantity):
    global last_trade_id
    
    # If there is no open position for the symbol, start with a new trade_id
    if symbol not in open_positions:
        last_trade_id += 1
        open_positions[symbol] = {
            "trade_id": last_trade_id,
            "quantity": quantity  # Track current position quantity
        }
        return open_positions[symbol]["trade_id"]
    
    # If there is an open position, update based on quantity
    else:
        position = open_positions[symbol]
        new_quantity = position["quantity"] + quantity

        # If new quantity is 0, it means the position is being closed
        if new_quantity == 0:
            # Remove the position since it's closed
            prev_id = open_positions[symbol]["trade_id"]
            del open_positions[symbol]
            return prev_id

        # If position is still open, update the quantity and keep the same trade_id
        if new_quantity != 0:
            position["quantity"] = new_quantity
            return position["trade_id"]

    # If position is closed and we are re-opening with a new quantity, assign a new trade_id
    last_trade_id += 1
    open_positions[symbol] = {
        "trade_id": last_trade_id,
        "quantity": quantity  # Start a new position with the new quantity
    }
    return last_trade_id

def calculate_date_and_time(date_and_time):
    date_part, time_part = date_and_time.split(';')
    date = datetime.strptime(date_part, "%Y-%m-%d").strftime("%d-%m-%Y")
    time = datetime.strptime(time_part, "%H%M%S").strftime("%H:%M:%S")
    return date, time


# Insert each row into the executions table
for index, row in df.iterrows():
    trade_id = calculate_trade_id(row['symbol'], row['quantity'])
    date = calculate_date_and_time(row['date_and_time'])[0]
    time = calculate_date_and_time(row['date_and_time'])[1]
    
    cursor.execute("""
        INSERT INTO executions (
            trade_id, account_id, symbol, quantity, price, 
            net_cash_with_billable, date_and_time, date, timestamp, commission, prev_trade_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (trade_id, row['accountId'], row['symbol'], row['quantity'], row['price'], 
          row['netCashWithBillable'], row['date_and_time'], date, time, row['commission'], row['prev_trade_id']))

conn.commit()
conn.close()