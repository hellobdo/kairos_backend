import sqlite3
import pandas as pd

columns_to_select = ['date', 'account_size']
df = pd.read_csv('../migration/account_size.csv', usecols=columns_to_select)

# Connect to the SQLite database (create if not exists)
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Insert each row into the account_size table
for index, row in df.iterrows():
    
    cursor.execute("""
        INSERT INTO account_size (
            date, account_size
        ) VALUES (?, ?)
    """, (row['date'], row['account_size']))

conn.commit()
conn.close()