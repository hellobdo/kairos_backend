import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# SQL query to insert a new row into the 'account_size' table
insert_query = """
INSERT INTO account_size (date, account_size)
VALUES ('2025-01-31', 27000);
"""

# Execute the insert query
cursor.execute(insert_query)

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()