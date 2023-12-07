import sqlite3
import pandas as pd

data_folder = './data/'

# File paths
count_metadata_file = data_folder + 'count_metadata.csv'
locations_file = data_folder + 'locations.csv'
raw_data_file = data_folder + 'raw-data-2020-2029.csv'

# Load CSV files
count_metadata_df = pd.read_csv(count_metadata_file)
locations_df = pd.read_csv(locations_file)
raw_data_df = pd.read_csv(raw_data_file)

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('./data/traffic_data.db')
cursor = conn.cursor()

# Drop tables if they exist
conn.execute("DROP TABLE IF EXISTS count_metadata")
conn.execute("DROP TABLE IF EXISTS locations")
conn.execute("DROP TABLE IF EXISTS raw_data")

# Create tables
conn.execute('''create table count_metadata
(
    _id             INTEGER,
    count_id        INTEGER,
    count_date      TEXT,
    location_id     INTEGER,
    location        TEXT,
    lng             REAL,
    lat             REAL,
    centreline_type REAL,
    centreline_id   REAL,
    px              REAL
)''')

conn.execute('''create table locations
(
    _id               INTEGER,
    location_id       INTEGER,
    location          TEXT,
    lng               REAL,
    lat               REAL,
    centreline_type   REAL,
    centreline_id     REAL,
    px                REAL,
    latest_count_date TEXT
)''')

# Assuming raw_data has many columns, define them accordingly
conn.execute('''create table raw_data
(
    _id             INTEGER,
    count_id        INTEGER,
    count_date      TEXT,
    location_id     INTEGER,
    location        TEXT,
    lng             REAL,
    lat             REAL,
    centreline_type INTEGER,
    centreline_id   INTEGER,
    px              REAL,
    time_start      TEXT,
    time_end        TEXT,
    sb_cars_r       INTEGER,
    sb_cars_t       INTEGER,
    sb_cars_l       INTEGER,
    nb_cars_r       INTEGER,
    nb_cars_t       INTEGER,
    nb_cars_l       INTEGER,
    wb_cars_r       INTEGER,
    wb_cars_t       INTEGER,
    wb_cars_l       INTEGER,
    eb_cars_r       INTEGER,
    eb_cars_t       INTEGER,
    eb_cars_l       INTEGER,
    sb_truck_r      INTEGER,
    sb_truck_t      INTEGER,
    sb_truck_l      INTEGER,
    nb_truck_r      INTEGER,
    nb_truck_t      INTEGER,
    nb_truck_l      INTEGER,
    wb_truck_r      INTEGER,
    wb_truck_t      INTEGER,
    wb_truck_l      INTEGER,
    eb_truck_r      INTEGER,
    eb_truck_t      INTEGER,
    eb_truck_l      INTEGER,
    sb_bus_r        INTEGER,
    sb_bus_t        INTEGER,
    sb_bus_l        INTEGER,
    nb_bus_r        INTEGER,
    nb_bus_t        INTEGER,
    nb_bus_l        INTEGER,
    wb_bus_r        INTEGER,
    wb_bus_t        INTEGER,
    wb_bus_l        INTEGER,
    eb_bus_r        INTEGER,
    eb_bus_t        INTEGER,
    eb_bus_l        INTEGER,
    nx_peds         INTEGER,
    sx_peds         INTEGER,
    ex_peds         INTEGER,
    wx_peds         INTEGER,
    nx_bike         INTEGER,
    sx_bike         INTEGER,
    ex_bike         INTEGER,
    wx_bike         INTEGER,
    nx_other        INTEGER,
    sx_other        INTEGER,
    ex_other        INTEGER,
    wx_other        INTEGER
                )''')



# Insert data into tables
count_metadata_df.to_sql('count_metadata', conn, if_exists='replace', index=False)
locations_df.to_sql('locations', conn, if_exists='replace', index=False)
raw_data_df.to_sql('raw_data', conn, if_exists='replace', index=False)

# Load CSV files and get row counts
csv_row_counts = {
    'count_metadata': pd.read_csv(count_metadata_file).shape[0],
    'locations': pd.read_csv(locations_file).shape[0],
    'raw_data': pd.read_csv(raw_data_file).shape[0]
}

db_row_counts = {}
for table in csv_row_counts.keys():
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    db_row_counts[table] = count

# Close the connection
conn.close()

for table in csv_row_counts:
    print(f"Table: {table}")
    print(f"CSV row count: {csv_row_counts[table]}")
    print(f"Database row count: {db_row_counts[table]}\n")
