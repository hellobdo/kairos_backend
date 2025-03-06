import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Create the CTE (Common Table Expression) to join trades and account_size tables
update_query = """
WITH account_data AS (
    SELECT t.trade_id, 
           t.entry_price, 
           t.stop_price, 
           t.quantity, 
           t.risk_reward, 
           a.account_size
    FROM trades t
    JOIN account_size a ON t.entry_date = a.date
)
UPDATE trades
SET risk_size = ABS(entry_price - stop_price) * quantity,
    risk_per_trade = (ABS(entry_price - stop_price) * quantity) / (
        SELECT account_size FROM account_data WHERE trade_id = trades.trade_id
    ),
    perc_return = risk_per_trade * risk_reward;
"""

# Execute the update query
cursor.execute(update_query)

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()

print("The 'risk_size', 'risk_per_trade', and 'perc_return' fields in the 'trades' table have been updated.")
