import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Rename the table from 'account_size' to 'accounts'
cursor.execute("ALTER TABLE account_size RENAME TO accounts;")

# Add new columns 'account_id' and 'account_type'
cursor.execute("ALTER TABLE accounts ADD COLUMN account_id TEXT;")
cursor.execute("ALTER TABLE accounts ADD COLUMN account_type TEXT;")

# Update account_type to "paper" for all existing rows
cursor.execute("UPDATE accounts SET account_type = 'paper';")

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()

print("The table has been renamed to 'accounts' and the new columns 'account_id' and 'account_type' have been added.")
