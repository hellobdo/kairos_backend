import sqlite3

# Connect to SQLite (creates a file if it doesn’t exist)
conn = sqlite3.connect("../kairos.db")
cursor = conn.cursor() # creates the cursor, the intermediary between python and the DB

# creates executions table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS strategies (
        strategy_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT NOT NULL
    )
""")


# Insert the schema details for the trades table
cursor.execute("""
    INSERT INTO schema (table_name, column_name, data_type, constraints, description)
    VALUES
    ('strategies', 'strategy_id', 'INTEGER', 'PRIMARY KEY AUTOINCREMENT', 'Unique identifier for each entry in this table'),
    ('strategies', 'name', 'TEXT', 'NOT NULL', 'Strategy name'),
    ('strategies', 'description', 'TEXT', 'NOT NULL', 'Strategy description')
""")


conn.commit()
conn.close()