import sqlite3

# Connect to SQLite (creates a file if it doesn’t exist)
conn = sqlite3.connect("../kairos.db")
cursor = conn.cursor() # creates the cursor, the intermediary between python and the DB

# creates executions table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS executions (
        execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id INTEGER,
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price REAL NOT NULL,
        net_cash_with_billable REAL NOT NULL,
        date_and_time TEXT NOT NULL,
        commission REAL NOT NULL
    )
""")


# Create a table that holds schema metadata for the tables
cursor.execute("""
    CREATE TABLE IF NOT EXISTS schema (
        schema_id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        data_type TEXT NOT NULL,
        constraints TEXT,
        description TEXT
    )
""")

# Insert the schema details the tables
cursor.execute("""
    INSERT INTO schema (table_name, column_name, data_type, constraints, description)
    VALUES
    ('executions', 'execution_id', 'INTEGER', 'PRIMARY KEY AUTOINCREMENT', 'Unique identifier for each execution'),
    ('executions', 'trade_id', 'INTEGER', 'NOT NULL', 'Unique identifier for each trade as defined as a continuation of executions in a given symbol'),
    ('executions', 'account_id', 'TEXT', 'NOT NULL', 'Account identifier for the execution'),
    ('executions', 'symbol', 'TEXT', 'NOT NULL', 'The symbol of the asset being traded'),
    ('executions', 'quantity', 'INTEGER', 'NOT NULL', 'The number of shares/contracts traded'),
    ('executions', 'price', 'REAL', 'NOT NULL', 'Price at which the asset was traded'),
    ('executions', 'net_cash_with_billable', 'REAL', 'NOT NULL', 'The total cash involved in the execution including commission'),
    ('executions', 'date_and_time', 'TEXT', 'NOT NULL', 'Timestamp of the execution'),
    ('executions', 'commission', 'REAL', 'NOT NULL', 'Commission charged for the execution')
""")

conn.commit()
conn.close()