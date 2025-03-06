import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect('../../kairos.db')
cursor = conn.cursor()

# Delete rows with schema_ID 39 and 40 from the schema table
cursor.execute("DELETE FROM schema WHERE schema_ID IN (39, 40);")

# Update description for schema_ID = 37 to 'date as YYYY-MM-DD'
cursor.execute("""
UPDATE schema
SET description = 'date as YYYY-MM-DD'
WHERE schema_ID = 37;
""")

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()

print("Rows with schema_ID 39 and 40 have been deleted, and description for schema_ID 37 has been updated.")
