# csv file only from trades 310 since that has a strategy assigned to it

import sqlite3
import pandas as pd

columns_to_select = ['accountId', 'symbol', 'quantity', 'price', 'netCashWithBillable', 'date_and_time', 'commission']
df = pd.read_csv('../migration/executions.csv', usecols=columns_to_select)

# Connect to the SQLite database (create if not exists)
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

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
    position = open_positions[symbol]
    new_quantity = position["quantity"] + quantity
    
    # If the position is still open, keep the same trade_id
    if new_quantity != 0:
        position["quantity"] = new_quantity
        return position["trade_id"]
    
    # If position is closed, assign a new trade_id
    last_trade_id += 1
    open_positions[symbol] = {
        "trade_id": last_trade_id,
        "quantity": quantity  # Start a new position with the new quantity
    }
    return last_trade_id

# Insert each row into the executions table
for index, row in df.iterrows():
    trade_id = calculate_trade_id(row['symbol'], row['quantity'])
    
    cursor.execute("""
        INSERT INTO executions (
            trade_id, account_id, symbol, quantity, price, 
            net_cash_with_billable, date_and_time, commission
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (trade_id, row['accountId'], row['symbol'], row['quantity'], row['price'], 
          row['netCashWithBillable'], row['date_and_time'], row['commission']))

conn.commit()
conn.close()