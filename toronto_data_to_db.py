import pandas as pd
import sqlite3
from shapely import wkt

# Function to extract latitude and longitude from WKT column
def extract_lat_long(wkt_point):
    point = wkt.loads(wkt_point)
    return point.y, point.x

def main():
    # Load and transform the data
    file_path = './data/tf-ft-eng.csv'  # Update this with the path to your CSV file
    data = pd.read_csv(file_path)
    data[['latitude', 'longitude']] = data['WKT'].apply(lambda x: pd.Series(extract_lat_long(x)))
    data_transformed = data.drop(columns=['WKT', 'traffic_source'])
    data_melted = data_transformed.melt(id_vars=['traffic_camera', 'camera_road', 'latitude', 'longitude'],
                                        var_name='count_date', value_name='count_num')
    data_melted['count_date'] = pd.to_datetime(data_melted['count_date'], format='x%Y_%m_%d').dt.strftime('%Y-%m-%d')
    data_melted['count_num'] = data_melted['count_num'].where(pd.notnull(data_melted['count_num']), None)

    # Connect to the SQLite database
    db_file_path = './data/traffic_cam_data.db'  # Update this with your desired database file path
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    # Create the table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS TRAFFIC_CAM_COUNT(
            CAM_ID STRING,
            LAT NUMERIC(10,6),
            LONG NUMERIC(10,6),
            CAM_ROAD STRING,
            COUNT_DATE DATE,
            COUNT_NUM INT
        );
    '''
    cursor.execute(create_table_query)

    # Insert data into the table
    batch_size = 1000  # You can adjust this batch size
    for i in range(0, len(data_melted), batch_size):
        batch = data_melted.iloc[i:i+batch_size]
        cursor.executemany(
            '''
            INSERT INTO TRAFFIC_CAM_COUNT (CAM_ID, LAT, LONG, CAM_ROAD, COUNT_DATE, COUNT_NUM)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            batch[['traffic_camera', 'latitude', 'longitude', 'camera_road', 'count_date', 'count_num']].values.tolist()
        )
        conn.commit()

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
