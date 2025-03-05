import sqlite3
import pandas as pd

columns_to_select = ['name', 'description']
df = pd.read_csv('../migration/files/entry_types.csv', usecols=columns_to_select)

# Connect to the SQLite database (create if not exists)
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Insert each row into the account_size table
for index, row in df.iterrows():
    
    cursor.execute("""
        INSERT INTO entry_type (
            name, description
        ) VALUES (?, ?)
    """, (row['name'], row['description']))

conn.commit()
conn.close()