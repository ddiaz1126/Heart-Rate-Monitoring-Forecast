from dotenv import load_dotenv
from datetime import datetime
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

# Path to the folder of data
folder_path = 'PolarFlowData'
number_of_files = 0

# Create an empty list to store all running data
all_running_data = []

# Create an empty list to store all aggregated data
all_headers_data = []
# Iterate through each file in the folder
for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    
    # Check if the file is a CSV
    if filename.endswith('.CSV'):
        print(f"Processing file: {filename}")
        number_of_files += 1
        
        try:
            # Read the first row separately (aggregated statistics)
            header = pd.read_csv(file_path, nrows=1)
            
            # Read the rest of the data, skipping the first two rows
            running = pd.read_csv(file_path, skiprows=[0, 1])
            
            # Extract Date and Start Time from header
            date_str = header['Date'].values[0]  # Assuming it's in 'Date' column
            start_time_str = header['Start time'].values[0]  # Assuming it's in 'Start time' column

            # Combine the date and start time to form a datetime object
            start_datetime_str = f"{date_str} {start_time_str}"
            start_datetime = datetime.strptime(start_datetime_str, "%d-%m-%Y %H:%M:%S")
            
            # Convert 'Time' column (hh:mm:ss format) to timedeltas
            running['Time'] = pd.to_timedelta(running['Time'])

            # Create a new column 'Real_Time' by adding 'Time' column to the start datetime
            running['Real_Time'] = running['Time'] + start_datetime

            running['Source_File'] = filename

            # Append the Headers to the list
            all_headers_data.append(header)
            
            # Append the processed DataFrame to the list
            all_running_data.append(running)

        except pd.errors.EmptyDataError:
            print(f"Error: {filename} is empty or has invalid format")
        except FileNotFoundError:
            print(f"Error: {filename} not found")
        except Exception as e:
            print(f"An error occurred while processing {filename}: {e}")

print(f"\n Total Amount of Files Processes: {number_of_files}")

# After processing all files, concatenate all the DataFrames into one
if all_running_data:
    combined_running_df = pd.concat(all_running_data, ignore_index=True)
    
else:
    print("No data to concatenate.")

# Rename the Columns into Name without whitespaces
combined_running_df.rename(columns={'HR (bpm)': 'HR', 'Speed (mi/h)': 'Speed', 'Pace (min/mi)': 'Original_Pace', 'Distances (ft)': 'distances'}, inplace=True)

polar_data_filtered = combined_running_df[['Time', 'HR', 'Speed', 'Original_Pace', 'distances', 'Real_Time', 'Source_File' ]]


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
    INSERT INTO polar_data_db (
        Time, HR, Speed, Original_Pace, distances, Real_Time, Source_File
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """)

    # Convert DataFrame to list of tuples
    rows_to_insert = polar_data_filtered.to_records(index=False).tolist()

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