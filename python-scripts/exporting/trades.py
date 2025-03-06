import sqlite3
import pandas as pd

# Connect to your SQLite database
conn = sqlite3.connect('../kairos.db')

# Query to fetch all data from the 'trades' table
query = "SELECT * FROM trades"

# Use pandas to read the query result into a DataFrame
trades_df = pd.read_sql(query, conn)

# Specify the path where you want to save the CSV file
csv_file_path = 'trades_table.csv'

# Save the DataFrame to a CSV file
trades_df.to_csv(csv_file_path, index=False)

# Close the connection to the database
conn.close()

print(f"The 'trades' table has been saved to {csv_file_path}.")