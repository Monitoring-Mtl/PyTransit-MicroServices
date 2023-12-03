import polars as pl
import boto3
import os
import pytz
from datetime import datetime

# Init S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):

    # Define the buckets and file paths
    input_bucket = event['input_bucket']
    output_bucket = event['output_bucket']
    timezone = event['timezone']
    passed_date_str = event['date']

    calendar_file_path = 'calendar/calendar.parquet'
    trips_file_path = 'trips/trips.parquet'
    stop_times_file_path = 'stop_times/stop_times.parquet'

    # Setup the timezone
    eastern = pytz.timezone(timezone)

    # New: Parse the passed date instead of using datetime.now()
    now = datetime.strptime(passed_date_str, '%Y%m%d').replace(tzinfo=eastern)

    # Use the passed date and day of the week
    current_date = now.strftime('%Y%m%d')
    current_day_of_week = now.strftime('%A').lower()

    # Define folder and file names for output
    folder_name = now.strftime('%Y/%m/%d')
    file_name = now.strftime('%Y-%m-%d')
    output_base_path = f"{folder_name}/"

    def download_file_to_tmp(bucket, key):
        local_path = f"/tmp/{os.path.basename(key)}"
        s3.download_file(Bucket=bucket, Key=key, Filename=local_path)
        return local_path

    def upload_file_from_tmp(bucket, key, local_path):
        s3.upload_file(Filename=local_path, Bucket=bucket, Key=key)

    def read_parquet_from_tmp(local_path):
        return pl.read_parquet(local_path)

    def write_df_to_parquet_to_tmp(df, local_path):
        df.write_parquet(local_path)

    # Load calendar.csv from S3 into a DataFrame
    local_calendar_file_path = download_file_to_tmp(input_bucket, calendar_file_path)
    calendar_df = read_parquet_from_tmp(local_calendar_file_path)

    # Convert 'start_date' and 'end_date' to datetime format
    # Check if they are not already in datetime format
    if calendar_df['start_date'].dtype != pl.Date:
        calendar_df = calendar_df.with_columns(pl.col('start_date').cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d"))
    if calendar_df['end_date'].dtype != pl.Date:
        calendar_df = calendar_df.with_columns(pl.col('end_date').cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d"))

    # Convert current_date to the same format for comparison
    current_date = datetime.now().strftime('%Y%m%d')
    current_date = pl.lit(current_date).str.strptime(pl.Date, "%Y%m%d")


    # Filter calendar DataFrame
    calendar_df = calendar_df.filter((pl.col('start_date') <= current_date) & (pl.col('end_date') >= current_date))
    calendar_df = calendar_df.filter(pl.col(current_day_of_week) == 1)

    # Keep only the 'service_id' values
    service_ids_df = calendar_df.select(['service_id'])

    # Write service_ids to /tmp and upload to S3
    local_service_ids_path = f"/tmp/service_ids_{file_name}.parquet"
    write_df_to_parquet_to_tmp(service_ids_df, local_service_ids_path)
    upload_file_from_tmp(output_bucket, f'{output_base_path}service_ids/service_ids_{file_name}.parquet', local_service_ids_path)

    # Load trips.csv from S3 into DataFrame
    local_trips_file_path = download_file_to_tmp(input_bucket, trips_file_path)
    trips_df = read_parquet_from_tmp(local_trips_file_path)

    # Merge DataFrames on service_id
    filtered_trips_df = service_ids_df.join(trips_df, on='service_id')

    # Write filtered_trips to /tmp and upload to S3
    local_filtered_trips_path = f"/tmp/filtered_trips_{file_name}.parquet"
    write_df_to_parquet_to_tmp(filtered_trips_df, local_filtered_trips_path)
    upload_file_from_tmp(output_bucket, f'{output_base_path}filtered_trips/filtered_trips_{file_name}.parquet', local_filtered_trips_path)

    # Extract unique trip_ids from the filtered DataFrame
    unique_trip_ids = filtered_trips_df.select('trip_id').unique().to_numpy().flatten()

    # Load stop_times.csv from S3 into DataFrame
    local_stop_times_file_path = download_file_to_tmp(input_bucket, stop_times_file_path)
    stop_times_df = read_parquet_from_tmp(local_stop_times_file_path)

    # Filter stop_times DataFrame based on trip_id
    filtered_stop_times_df = stop_times_df.filter(pl.col('trip_id').is_in(unique_trip_ids))

    # Write filtered_stop_times to /tmp and upload to S3
    local_filtered_stop_times_path = f"/tmp/filtered_stop_times_{file_name}.parquet"
    write_df_to_parquet_to_tmp(filtered_stop_times_df, local_filtered_stop_times_path)
    upload_file_from_tmp(output_bucket, f'{output_base_path}filtered_stop_times/filtered_stop_times_{file_name}.parquet', local_filtered_stop_times_path)

    # Clean up the /tmp directory 
    os.remove(local_calendar_file_path)
    os.remove(local_service_ids_path)
    os.remove(local_trips_file_path)
    os.remove(local_filtered_trips_path)
    os.remove(local_stop_times_file_path)