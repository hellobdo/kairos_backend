import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('../../kairos.db')
cursor = conn.cursor()

# 2. Read the CSV with account data
csv_file_path = '../migration/files/accounts.csv'  # Update this with the actual path to your CSV
csv_data = pd.read_csv(csv_file_path)

# 3. Insert new records into the `accounts` table
for _, row in csv_data.iterrows():
    # Insert new record into the accounts table
    cursor.execute("""
    INSERT INTO accounts (account_id, account_type, account_description)
    VALUES (?, ?, ?);
    """, (row['account_id'], row['account_type'], row['account_description']))

# Commit and close the connection
conn.commit()
conn.close()

print("Account data successfully inserted.")