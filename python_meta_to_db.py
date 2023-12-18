import sqlite3
import pandas as pd

# Load the data
file_path = 'count_metadata.csv'  # replace with your file path
data = pd.read_csv(file_path)

# Connect to SQLite database (this will create the database file if it doesn't exist)
conn = sqlite3.connect('count_metadata.db')  # replace with your database name

# Create a table
# Adjust the data types based on your specific needs
create_table_query = '''
CREATE TABLE IF NOT EXISTS counts (
    _id INTEGER PRIMARY KEY,
    count_id INTEGER,
    count_date TEXT,
    location_id INTEGER,
    location TEXT,
    lng REAL,
    lat REAL,
    centreline_type REAL,
    centreline_id REAL,
    px REAL
)
'''
conn.execute(create_table_query)

# Insert data into the table
data.to_sql('counts', conn, if_exists='append', index=False)

# Close the connection
conn.close()
