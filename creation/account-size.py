import sqlite3

# Connect to SQLite (creates a file if it doesn’t exist)
conn = sqlite3.connect("../kairos.db")
cursor = conn.cursor() # creates the cursor, the intermediary between python and the DB

# creates executions table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS account_size (
        account_size_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        account_size REAL NOT NULL
    )
""")


# Insert the schema details for the trades table
cursor.execute("""
    INSERT INTO schema (table_name, column_name, data_type, constraints, description)
    VALUES
    ('account_size', 'account_size_id', 'INTEGER', 'PRIMARY KEY AUTOINCREMENT', 'Unique identifier for each entry in this table'),
    ('account_size', 'date', 'TEXT', 'NOT NULL', 'The date of recording of the account size'),
    ('account_size', 'account_size', 'REAL', 'NOT NULL', 'The account size, ie, total cash held excluding open positions')
""")


conn.commit()
conn.close()