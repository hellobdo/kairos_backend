import sqlite3

# Connect to your database (replace 'your_database.db' with your actual database)
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# SQL query to swap entry_date and exit_date for trade_id 134
query = """
UPDATE trades
SET entry_date = exit_date,
    exit_date = entry_date
WHERE trade_id = 134;
"""

# Execute the query
cursor.execute(query)

# Commit the changes
conn.commit()

# Close the connection
conn.close()