import sqlite3
import pandas as pd

# Connect to your SQLite database
conn = sqlite3.connect('../kairos.db')

# Query to fetch data from 'trades' table
trades_query = "SELECT trade_id, entry_date FROM trades"
trades_df = pd.read_sql(trades_query, conn)

# Query to fetch data from 'account_size' table
account_size_query = "SELECT date, account_size FROM account_size"
account_size_df = pd.read_sql(account_size_query, conn)

# Close the connection to the database
conn.close()

# Perform a 'vlookup' (merge) based on 'entry_date' from trades and 'date' from account_size
result_df = pd.merge(trades_df, account_size_df, left_on='entry_date', right_on='date', how='left')

# Save the result DataFrame to a CSV file
result_df.to_csv('result.csv', index=False)