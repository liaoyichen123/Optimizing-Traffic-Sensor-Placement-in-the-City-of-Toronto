import sqlite3
import pandas as pd
from datetime import timedelta
import logging
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def load_database_to_memory(disk_db_path):
    """Loads the database from disk into memory."""
    mem_conn = sqlite3.connect(':memory:')
    disk_conn = sqlite3.connect(disk_db_path)
    for line in disk_conn.iterdump():
        if 'BEGIN TRANSACTION' in line: continue
        mem_conn.execute(line)
    disk_conn.close()
    return mem_conn

def fetch_data(conn):
    """Fetches data from the database."""
    query = "SELECT * FROM TRAFFIC_CAM_COUNT"
    return pd.read_sql_query(query, conn)

def find_replacement_value(df, cam_id, date):
    """Finds a replacement value for a null count_num following the specified rules."""
    # Convert string date to datetime
    date = pd.to_datetime(date)

    # Rule 1: Same date next year
    replacement_date = date.replace(year=date.year + 1)
    replacement_value = df.loc[(df['CAM_ID'] == cam_id) & (pd.to_datetime(df['COUNT_DATE']) == replacement_date), 'COUNT_NUM']
    if not replacement_value.empty and not pd.isna(replacement_value.iloc[0]):
        return replacement_value.iloc[0]

    # Rule 2: Same date last year
    replacement_date = date.replace(year=date.year - 1)
    replacement_value = df.loc[(df['CAM_ID'] == cam_id) & (pd.to_datetime(df['COUNT_DATE']) == replacement_date), 'COUNT_NUM']
    if not replacement_value.empty and not pd.isna(replacement_value.iloc[0]):
        return replacement_value.iloc[0]

    # Rule 3 & 4: Next/Last week same weekday
    for days in [7, -7]:
        replacement_date = date + timedelta(days=days)
        replacement_value = df.loc[(df['CAM_ID'] == cam_id) & (pd.to_datetime(df['COUNT_DATE']) == replacement_date), 'COUNT_NUM']
        if not replacement_value.empty and not pd.isna(replacement_value.iloc[0]):
            return replacement_value.iloc[0]

    for days in [1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        replacement_date = date + timedelta(days=days)
        replacement_value = df.loc[(df['CAM_ID'] == cam_id) & (pd.to_datetime(df['COUNT_DATE']) == replacement_date), 'COUNT_NUM']
        if not replacement_value.empty and not pd.isna(replacement_value.iloc[0]):
            return replacement_value.iloc[0]

    return None

def update_database(conn, updates):
    """Updates the database with the new count_num values."""
    cursor = conn.cursor()
    for cam_id, count_date, count_num in updates:
        update_query = """
            UPDATE TRAFFIC_CAM_COUNT
            SET COUNT_NUM = ?
            WHERE CAM_ID = ? AND COUNT_DATE = ?
        """
        cursor.execute(update_query, (count_num, cam_id, count_date))
    conn.commit()

def save_memory_db_to_disk(mem_conn, disk_db_path):
    """Saves the in-memory database back to disk after deleting the old disk-based database."""
    logging.info("Deleting the old disk-based database (if it exists).")
    if os.path.exists(disk_db_path):
        os.remove(disk_db_path)

    logging.info("Saving the in-memory database back to disk.")
    with sqlite3.connect(disk_db_path) as disk_conn:
        # Dump the in-memory database into the new disk-based database file
        for line in mem_conn.iterdump():
            disk_conn.execute(line)
    logging.info("In-memory database saved to disk successfully.")


def main():
    disk_db_path = './data/traffic_cam_data.db'  # Update with your database file path
    mem_conn = load_database_to_memory(disk_db_path)
    df = fetch_data(mem_conn)

    null_rows = df[df['COUNT_NUM'].isna()]
    total_rows = len(null_rows)
    logging.info(f"Processing {total_rows} rows with null count_num.")

    updates = []
    for index, row in enumerate(null_rows.iterrows()):
        cam_id, count_date = row[1]['CAM_ID'], row[1]['COUNT_DATE']
        replacement_value = find_replacement_value(df, cam_id, count_date)
        if replacement_value is not None:
            updates.append((cam_id, count_date, replacement_value))
        logging.info(f"Processed row {index + 1} of {total_rows}. Remaining: {total_rows - index - 1}")

    if updates:
        update_database(mem_conn, updates)
    else:
        logging.info("No updates required.")

    save_memory_db_to_disk(mem_conn, disk_db_path)
    mem_conn.close()

if __name__ == "__main__":
    main()
