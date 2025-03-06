import sqlite3
import pandas as pd
from datetime import datetime

# Read CSV data
columns_to_select = ['prev_trade_id', 'strategy', 'entry_type', 'stop_price']
csv_data = pd.read_csv('../migration/files/trades.csv', usecols=columns_to_select)

# Replace NaN values in 'entry_type' with an empty string (or another default value if needed)
csv_data['entry_type'].fillna('', inplace=True)

# Connect to SQLite (creates a file if it doesn’t exist)
conn = sqlite3.connect("../kairos.db")
cursor = conn.cursor()

# Fetch execution data from 'executions' table
cursor.execute("SELECT * FROM executions")
executions = cursor.fetchall()

# Convert the execution data to a DataFrame and inspect it
executions_df = pd.DataFrame(executions, columns=[
    'execution_id', 'trade_id', 'account_id', 'symbol', 'quantity', 'price', 
    'net_cash_with_billable', 'date_and_time', 'date', 'timestamp', 'commission', 'prev_trade_id'])

# Merge the executions data with the CSV data
merged_data = executions_df.merge(csv_data, left_on='prev_trade_id', right_on='prev_trade_id', how='left')

# For each trade_id, perform calculations based on your rules
import numpy as np
EPSILON = 1e-6  # This value can be adjusted based on the sensitivity you want

trades_data = []

for trade_id, group in merged_data.groupby('trade_id'):
    symbol = group['symbol'].iloc[0]  # All executions in the group have the same symbol
    direction = 'bullish' if group['quantity'].iloc[0] > 0 else 'bearish'
    
    # Sum the quantities and ensure positive value for bearish trades
    quantity = group.loc[group['quantity'] > 0, 'quantity'].sum() if direction == 'bullish' else group.loc[group['quantity'] < 0, 'quantity'].abs().sum()

    # Entry price
    entry_price = (group.loc[group['quantity'] > 0, 'net_cash_with_billable'].abs().sum() / quantity if direction == 'bullish' else group.loc[group['quantity'] < 0, 'net_cash_with_billable'].abs().sum() / quantity) 
    
    # Exit price
    exit_price = (group.loc[group['quantity'] < 0, 'net_cash_with_billable'].abs().sum() / quantity if direction == 'bullish' else group.loc[group['quantity'] > 0, 'net_cash_with_billable'].abs().sum() / quantity) 

    # Capital required
    capital_required = group.loc[group['quantity'] > 0, 'net_cash_with_billable'].abs().sum() if direction == 'bullish' else group.loc[group['quantity'] < 0, 'net_cash_with_billable'].sum()

    strategy = group['strategy'].iloc[0]
    entry_type = group['entry_type'].iloc[0]
    instrument_type = "stock"
    stop_price = group['stop_price'].iloc[0]
    exit_date = group['date'].max()  # Last execution date in the group
    exit_timestamp = group['timestamp'].max()  # Last execution date in the group
    prev_trade_id = group['prev_trade_id'].iloc[0]
    entry_date = group['date'].min()  # First execution date in the group
    entry_timestamp = group['timestamp'].min()  # First execution timestamp in the group

    # trade duration
    # Example: Start and End timestamps in the 'YYYY-MM-DD;HHMMSS' format
    entry_time = group['date_and_time'].min()
    exit_time = group['date_and_time'].max()
    
    # Function to convert 'YYYY-MM-DD;HHMMSS' format to a datetime object
    def convert_to_datetime(entry_time, exit_time):
        # Convert string timestamps to datetime objects
        date_part, time_part = entry_time.split(';')
        start_time = datetime.strptime(date_part + ' ' + time_part, "%Y-%m-%d %H%M%S")
        
        date_part, time_part = exit_time.split(';')
        end_time = datetime.strptime(date_part + ' ' + time_part, "%Y-%m-%d %H%M%S")
        
        # Calculate the duration (difference between end and start time)
        duration = end_time - start_time
        
        # Get the total duration in seconds, minutes, hours, or days
        duration_in_seconds = duration.total_seconds()
        duration_in_minutes = duration_in_seconds / 60
        duration_in_hours = duration_in_minutes / 60

        return duration_in_hours
    
    
    trade_duration = convert_to_datetime(entry_time, exit_time)
    
    winning_trade = 1 if group['net_cash_with_billable'].sum() > 0 else 0
    
    # Check if the difference between entry_price and stop_price is too small (close to zero)
    if abs(entry_price - stop_price) < EPSILON:
        risk_reward = 0
    else: 
        if direction == 'bullish':
            risk_reward = (exit_price - entry_price) / (entry_price - stop_price)
        elif direction == 'bearish':
            risk_reward = (entry_price - exit_price) / (stop_price - entry_price)
    
    risk_per_trade = 0
    perc_return = 0
    

    trade_data = (
        trade_id, symbol, strategy, entry_type, direction, entry_date, entry_timestamp,
        instrument_type, quantity, entry_price, stop_price, exit_price, exit_date, exit_timestamp, 
        capital_required, trade_duration, winning_trade, risk_reward, risk_per_trade, perc_return, prev_trade_id
    )

    trades_data.append(trade_data)

# Print trades data before inserting into the database
trades_df = pd.DataFrame(trades_data, columns=[
    'trade_id', 'symbol', 'strategy', 'entry_type', 'direction', 'entry_date', 'entry_timestamp', 
    'instrument_type', 'quantity', 'entry_price', 'stop_price', 'exit_price', 'exit_date', 
    'exit_timestamp', 'capital_required', 'trade_duration', 'winning_trade', 'risk_reward', 
    'risk_per_trade', 'perc_return', 'prev_trade_id'])


# Insert data from the DataFrame into the trades table
for row in trades_df.itertuples(index=False, name=None):
    cursor.execute('''
    INSERT INTO trades (
        trade_id, symbol, strategy, entry_type, direction, entry_date, entry_timestamp, 
        instrument_type, quantity, entry_price, stop_price, exit_price, exit_date, 
        exit_timestamp, capital_required, trade_duration, winning_trade, risk_reward, 
        risk_per_trade, perc_return, prev_trade_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ''', row)

# Commit the transaction
conn.commit()

# Close the connection
conn.close()