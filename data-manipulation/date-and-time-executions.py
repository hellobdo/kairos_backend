from datetime import datetime
import sqlite3

# Connect to SQLite (creates a file if it doesn’t exist)
conn = sqlite3.connect("../kairos.db")
conn.row_factory = sqlite3.Row
cursor = conn.cursor() # creates the cursor, the intermediary between python and the DB

# Add the 'date' and 'time' columns to the 'executions' table
'''
cursor.execute("""
    ALTER TABLE executions
    ADD COLUMN date TEXT;
""")
cursor.execute("""
    ALTER TABLE executions
    ADD COLUMN timestamp TEXT;
""")
'''

# adds info to schema
cursor.execute("""
    INSERT INTO schema (table_name, column_name, data_type, constraints, description)
    VALUES
    ('executions', 'date', 'TEXT', 'NOT NULL', 'date as DD-MM-YYYY'),
    ('executions', 'timestamp', 'TEXT', 'NOT NULL', 'time as HH-MM-SS')
""")

cursor.execute("SELECT * FROM executions")
executions = cursor.fetchall()

for row in executions:
    date_and_time = row['date_and_time']
    
    # Split and format the date and time
    date_part, time_part = date_and_time.split(';')
    date = datetime.strptime(date_part, "%Y-%m-%d").strftime("%d-%m-%Y")
    time = datetime.strptime(time_part, "%H%M%S").strftime("%H:%M:%S")
    
    # Now you can insert this into the database as separate fields
    cursor.execute("""
        UPDATE executions 
        SET date = ?, timestamp = ?
        WHERE execution_id = ?
    """, (date, time, row['execution_id']))

# Commit the changes
conn.commit()
conn.close()