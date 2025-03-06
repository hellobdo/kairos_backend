import sqlite3

# Connect to your database (replace 'your_database.db' with your actual database)
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# SQL query to add the new column risk_size
query = """
ALTER TABLE trades
ADD COLUMN risk_size REAL;
"""

# Execute the query
cursor.execute(query)

# Commit the changes
conn.commit()

# Close the connection
conn.close()
