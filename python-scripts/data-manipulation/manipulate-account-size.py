import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Update query to remove commas from account_size
update_query = "UPDATE account_size SET account_size = REPLACE(account_size, ',', '');"

# Execute the update query
cursor.execute(update_query)

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()

print("Commas have been removed from the 'account_size' column.")