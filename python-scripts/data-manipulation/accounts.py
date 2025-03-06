import sqlite3

# Connect to the database
conn = sqlite3.connect('../../kairos.db')
cursor = conn.cursor()

# Drop the 'death_accounts' table
cursor.execute("DROP TABLE IF EXISTS death_accounts")

# Commit and close the connection
conn.commit()
conn.close()

print("Table 'death_accounts' dropped successfully.")
