import sqlite3

# Connect to SQLite (creates a file if it doesn’t exist)
conn = sqlite3.connect("../kairos.db")
cursor = conn.cursor() # creates the cursor, the intermediary between python and the DB

# creates executions table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS entry_type (
        entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT NOT NULL
    )
""")


# Insert the schema details for the trades table
cursor.execute("""
    INSERT INTO schema (table_name, column_name, data_type, constraints, description)
    VALUES
    ('entry_type', 'entry_id', 'INTEGER', 'PRIMARY KEY AUTOINCREMENT', 'Unique identifier for each entry in this table'),
    ('entry_type', 'name', 'TEXT', 'NOT NULL', 'Entry name'),
    ('entry_type', 'description', 'TEXT', 'NOT NULL', 'Entry description')
""")


conn.commit()
conn.close()