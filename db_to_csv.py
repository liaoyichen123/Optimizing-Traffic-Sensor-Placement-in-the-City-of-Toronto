import pandas as pd
import sqlite3
from datetime import datetime

# Assuming the data is stored in a SQLite database
db_connection = sqlite3.connect('./data/traffic_cam_data_final.db')

# Read data from the database
query = "SELECT * FROM TRAFFIC_CAM_COUNT"
df = pd.read_sql_query(query, db_connection)

# Transform the data
# Create a pivot table with CAM_ID as rows, COUNT_DATE as columns, and COUNT_NUM as values
pivot_df = df.pivot(index='CAM_ID', columns='COUNT_DATE', values='COUNT_NUM')

# Adding the additional columns (CAM_ROAD, LAT, LONG) from the original dataframe to the pivot table
pivot_df = pivot_df.join(df.set_index('CAM_ID')[['LAT', 'LONG', 'CAM_ROAD']].drop_duplicates())

# Reformat COUNT_DATE columns to match the format in the uploaded CSV
pivot_df.columns = [datetime.strptime(str(col), '%Y-%m-%d').strftime('x%Y_%m_%d') if not isinstance(col, str) else col for col in pivot_df.columns]

# Save the transformed data into a CSV file
output_file_path = './data/traffic_cam_data_final.csv'
pivot_df.to_csv(output_file_path)

print(f"Data saved to {output_file_path}")
