import sqlite3
from datetime import datetime

# Function to convert date from DD-MM-YYYY to YYYY-MM-DD
def convert_date_format(date_str):
    try:
        # Convert from DD-MM-YYYY to YYYY-MM-DD
        return datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
    except ValueError:
        # Return the original date if it cannot be converted
        return date_str

# Connect to your SQLite database
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Helper function to update date fields in any table
def update_table_dates(table_name, date_column):
    # Query to fetch all rows from the given table and column
    select_query = f"SELECT rowid, {date_column} FROM {table_name}"
    cursor.execute(select_query)
    
    # Fetch all rows
    rows = cursor.fetchall()
    
    # Iterate through the rows and update the date format
    for row in rows:
        rowid, date_str = row
        # Convert the date to ISO format (YYYY-MM-DD)
        iso_date = convert_date_format(date_str)
        
        # Update the row with the new date format
        update_query = f"""
        UPDATE {table_name}
        SET {date_column} = ?
        WHERE rowid = ?;
        """
        cursor.execute(update_query, (iso_date, rowid))

# Update date columns in the three tables

# Update 'date' in 'account_size' table
update_table_dates('account_size', 'date')

# Update 'date' in 'executions' table
update_table_dates('executions', 'date')

# Update 'entry_date' and 'exit_date' in 'trades' table
update_table_dates('trades', 'entry_date')
update_table_dates('trades', 'exit_date')

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()

print("All dates in the 'account_size', 'executions', and 'trades' tables have been updated to the ISO format (YYYY-MM-DD).")