import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Update query to calculate and update perc_return
update_perc_return_query = """
UPDATE trades
SET perc_return = risk_per_trade * risk_reward;
"""

# Execute the update query
cursor.execute(update_perc_return_query)

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()

print("The 'perc_return' field in the 'trades' table has been updated.")
