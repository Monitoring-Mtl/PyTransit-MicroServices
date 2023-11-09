import pandas as pd
import boto3
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):
    #Init S3 client
    s3 = boto3.client('s3')

    #Define the bucket
    input_bucket = 'pfe014-stm-data-static'
    output_bucket = 'pfe014-stm-data-static-daily'

    #Define file paths
    calendar_file_path = 'calendar/calendar.csv'
    trips_file_path = 'trips/trips.csv'
    stop_times_file_path = 'stop_times/stop_times.csv'

    current_date = datetime.now().strftime('%Y%m%d')
    current_day_of_week = datetime.now().strftime('%A').lower()

    folder_name = datetime.now().strftime('%Y-%m-%d')
    output_base_path = f"{folder_name}/"

    def read_csv_from_s3(bucket, key):
        response = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(response['Body'])

    def write_df_to_csv_on_s3(df, bucket, key):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        full_key = f"{output_base_path}{key}"  # Prepend the base path
        s3.put_object(Bucket=bucket, Key=full_key, Body=csv_buffer.getvalue())

    # Load calendar.csv from s3 into a DataFrame
    calendar_df = read_csv_from_s3(input_bucket, calendar_file_path)

    calendar_df[calendar_df.columns[1:8]] = calendar_df[calendar_df.columns[1:8]].astype(int)

    # Filter to keep only the row where "start_date" is older or equal to the "current_date" and "end_date" is further or equal the "current_date"
    calendar_df = calendar_df[(calendar_df['start_date'].astype(str) <= current_date) & (calendar_df['end_date'].astype(str) >= current_date)]

    # Filter to keep only the value that corresponds to the "current_day_of_week"
    calendar_df = calendar_df[calendar_df[current_day_of_week] == 1]

    # We only keep the values of service_id that correspond to all the filters
    service_ids_df = calendar_df['service_id']

    write_df_to_csv_on_s3(service_ids_df, output_bucket, 'service_ids/service_ids.csv')

    # Load trips.csv from S3 into DataFrame
    trips_df = read_csv_from_s3(input_bucket, trips_file_path)

    # Merge DataFrames on service_id
    filtered_trips_df = pd.merge(service_ids_df, trips_df, on='service_id')

    # Write filtered_trips.csv to S3
    write_df_to_csv_on_s3(filtered_trips_df, output_bucket, 'filtered_trips/filtered_trips.csv')

    # Extract unique trip_ids from the filtered DataFrame
    unique_trip_ids = filtered_trips_df['trip_id'].unique()

    # Load stop_times.csv from S3 into DataFrame
    stop_times_df = read_csv_from_s3(input_bucket, stop_times_file_path)

    # Filter stop_times DataFrame based on trip_id
    filtered_stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(unique_trip_ids)]

    # Write filtered_stop_times.csv to S3
    write_df_to_csv_on_s3(filtered_stop_times_df, output_bucket, 'filtered_stop_times/filtered_stop_times.csv')
