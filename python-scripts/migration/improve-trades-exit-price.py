import pandas as pd
import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('../../kairos.db')
cursor = conn.cursor()

# Step 2: Load the CSV into a pandas DataFrame
df = pd.read_csv('./files/improved_trades_exit_price.csv')

# Step 3: Load the improved_trades table into a pandas DataFrame
improved_trades_df = pd.read_sql('SELECT prev_trade_id, exit_price FROM improved_trades', conn)

# Step 4: Merge the CSV DataFrame with the improved_trades DataFrame to match prev_trade_id with trade_id
df_merged = df.merge(improved_trades_df, how='left', left_on='prev_trade_id', right_on='prev_trade_id', suffixes=('', '_prev'))

# Step 5: Update the exit_price in the CSV DataFrame with the merged exit_price from the improved_trades DataFrame
df['exit_price'] = df_merged['exit_price']

# Step 6: Update the exit_price in the database based on the CSV DataFrame
for index, row in df.iterrows():
    cursor.execute("""
        UPDATE improved_trades
        SET exit_price = ?
        WHERE prev_trade_id = ?
    """, (row['exit_price'], row['prev_trade_id']))

# Commit the changes and close the connection
conn.commit()
conn.close()

print("exit_price column has been added, and data has been updated in the database.")