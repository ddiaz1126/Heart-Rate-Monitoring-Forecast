from dotenv import load_dotenv
import psycopg2
import pandas as pd 
from psycopg2 import sql
import os 

# Load environment variables
load_dotenv()

# Retrieve credentials from environment
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")


# Define the folder containing your JSON files
input_folder = '/Users/daviddiaz/Desktop/Main/Heart-Rate-Monitoring-Forecast/MapMyRunData/parsed_json_file' 

# Initialize an empty list to hold DataFrames
df_list = []

# Iterate through each file in the folder
for filename in os.listdir(input_folder):
    if filename.endswith('.json'):  # Only process .json files
        # Full path to the JSON file
        file_path = os.path.join(input_folder, filename)
        
        # Read the JSON file into a DataFrame
        df = pd.read_json(file_path, lines=True)  # Specify lines=True for line-delimited JSON
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        df['elapsed_seconds'] = (df['time'] - df['time'].iloc[0]).dt.total_seconds()
        df_list.append(df)  # Add the DataFrame to the list

# Concatenate all DataFrames into a single DataFrame
map_my_runs_df = pd.concat(df_list, ignore_index=True)

# Connect to the database
try:
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    print("Connected to the database successfully!")

    # Create a cursor
    cursor = conn.cursor()

    # Prepare the insert statement
    insert_query = sql.SQL("""
    INSERT INTO map_my_run_db (
        time, latitude, longitude, altitude, distance, 
        activity_sport, activity_id, lap_start_time, lap_total_time_seconds, 
        lap_distance_meters, date, elapsed_seconds
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)

    # Convert DataFrame to list of tuples
    rows_to_insert = map_my_runs_df.to_records(index=False).tolist()

    # Execute the batch insert
    cursor.executemany(insert_query, rows_to_insert)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    print(f"{len(rows_to_insert)} rows inserted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")