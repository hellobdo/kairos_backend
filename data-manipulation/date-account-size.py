import sqlite3

# Connect to SQLite (creates a file if it doesn’t exist)
conn = sqlite3.connect("../kairos.db")
cursor = conn.cursor()

# Update the 'date' field in the table to change the format from 'DD/MM/YYYY' to 'DD-MM-YYYY'
cursor.execute("""
    UPDATE account_size
    SET date = REPLACE(date, '/', '-')
""")

# Commit the changes
conn.commit()

# Close the connection
conn.close()
