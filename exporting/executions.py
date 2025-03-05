import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("../kairos.db")

# Read the table into a DataFrame
df = pd.read_sql_query("SELECT * FROM executions", conn)

# Export to CSV
df.to_csv("executions.csv", index=False)

# Close the connection
conn.close()