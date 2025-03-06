import sqlite3
from datetime import datetime

# Function to convert date_and_time from 'YYYY-MM-DD;HHMMSS' to 'YYYY-MM-DD HH:MM:SS'
def convert_to_preferred_format(date_and_time_str):
    try:
        # Split the string into date and time
        date_part, time_part = date_and_time_str.split(';')
        
        # Format the time part as HH:MM:SS
        formatted_time = f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"
        
        # Combine the date and time into the preferred format
        formatted_date_and_time = f"{date_part} {formatted_time}"
        
        return formatted_date_and_time
    except ValueError:
        # Return the original date_and_time if it cannot be converted
        return date_and_time_str

# Connect to your SQLite database
conn = sqlite3.connect('../kairos.db')
cursor = conn.cursor()

# Query to fetch all rows from the executions table
select_query = "SELECT rowid, date_and_time FROM executions"
cursor.execute(select_query)

# Fetch all rows
rows = cursor.fetchall()

# Iterate through the rows and update the date_and_time format
for row in rows:
    rowid, date_and_time_str = row
    # Convert the date_and_time to the preferred format
    new_date_and_time = convert_to_preferred_format(date_and_time_str)
    
    # Update the row with the new date_and_time format
    update_query = """
    UPDATE executions
    SET date_and_time = ?
    WHERE rowid = ?;
    """
    cursor.execute(update_query, (new_date_and_time, rowid))

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()

print("The 'date_and_time' field in the 'executions' table has been updated to the preferred format (YYYY-MM-DD HH:MM:SS).")