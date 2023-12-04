import boto3
import pandas as pd
import os
from datetime import datetime, timedelta
import pytz

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    static_bucket = event['static_bucket']
    daily_static_bucket = event['daily_static_bucket']
    output_bucket = event['output_bucket']
    local_timezone = event.get('timezone', 'America/Montreal')  # Default to 'America/Montreal' if not specified

    eastern = pytz.timezone(local_timezone)

    # Extract the date from the event, or use the current date in the specified timezone (Format YYYYMMDD)
    date_str = event.get('date', datetime.now(eastern).strftime('%Y%m%d'))

    # Parse the date string into a datetime object
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    date_obj = eastern.localize(date_obj)

    # Use date_obj for folder_name and file_name
    folder_name = date_obj.strftime('%Y/%m/%d')
    file_name = date_obj.strftime('%Y-%m-%d')

    filtered_trips_path = f'{folder_name}/filtered_trips/filtered_trips_{file_name}.parquet'
    filtered_stop_times_path = f'{folder_name}/filtered_stop_times/filtered_stop_times_{file_name}.parquet'

    # Download files from S3 to /tmp
    stops_local_path = download_file_to_tmp(static_bucket, 'stops/stops.parquet')
    filtered_trips_local_path = download_file_to_tmp(daily_static_bucket, filtered_trips_path)
    filtered_stop_times_local_path = download_file_to_tmp(daily_static_bucket, filtered_stop_times_path)
    routes_local_path = download_file_to_tmp(static_bucket, 'routes/routes.parquet')

    # Read the necessary files from /tmp
    stops_df = read_parquet_from_tmp(stops_local_path)
    
    filtered_trips_df = read_parquet_from_tmp(filtered_trips_local_path)
    filtered_stop_times_df = read_parquet_from_tmp(filtered_stop_times_local_path)
    routes_df = read_parquet_from_tmp(routes_local_path)

    # Process the stop times into UNIX timestamp
    filtered_stop_times_df['arrival_time_unix'] = filtered_stop_times_df['arrival_time'].apply(lambda x: convert_to_unix(x, date_obj, local_timezone))

    # Ensure data types for 'stop_id' match
    stops_df['stop_id'] = stops_df['stop_id'].astype(str)
    filtered_stop_times_df['stop_id'] = filtered_stop_times_df['stop_id'].astype(str)

    # Merge DataFrames
    merged_df = pd.merge(
        filtered_stop_times_df,
        filtered_trips_df[['trip_id', 'route_id', 'trip_headsign', 'direction_id', 'shape_id', 'wheelchair_accessible']],
        on='trip_id',
        how='left'
    )

    # Merges from stops file
    merged_df = pd.merge(
        merged_df,
        stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'wheelchair_boarding']],
        on='stop_id',
        how='left'
    )

    # Merges from routes file
    merged_df = pd.merge(
        merged_df, 
        routes_df[['route_id', 'route_long_name']], 
        on='route_id',
        how='left'
    )

    # Create the route_info column
    merged_df['route_info'] = merged_df.apply(create_route_info, axis=1)
    
    # Select required columns
    final_df = merged_df[['route_id', 'route_info', 'trip_id', 'shape_id', 'wheelchair_accessible', 
                          'arrival_time_unix', 'stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'wheelchair_boarding']]

    # Write and upload the DataFrame to S3
    output_file_path = f'{folder_name}/daily_stops_info/daily_stops_info_{file_name}.parquet'
    local_output_path = f"/tmp/{os.path.basename(output_file_path)}"
    write_df_to_parquet_to_tmp(final_df, local_output_path)
    upload_file_from_tmp(output_bucket, output_file_path, local_output_path)

    # Clean up the /tmp directory if needed
    os.remove(stops_local_path)
    os.remove(filtered_trips_local_path)
    os.remove(filtered_stop_times_local_path)
    os.remove(routes_local_path)

    return {
        'statusCode': 200,
        'body': f"File saved to {output_bucket}/{output_file_path}"
    }

def download_file_to_tmp(bucket, key):
    local_path = f"/tmp/{os.path.basename(key)}"
    s3_client.download_file(Bucket=bucket, Key=key, Filename=local_path)
    return local_path

def upload_file_from_tmp(bucket, key, local_path):
    s3_client.upload_file(Filename=local_path, Bucket=bucket, Key=key)

# We need to force "fastparquet" as an engine, otherwise there is an error in the reading and creation.
def read_parquet_from_tmp(local_path):
    return pd.read_parquet(local_path,engine='fastparquet')

def write_df_to_parquet_to_tmp(df, local_path):
    df.to_parquet(local_path, engine='fastparquet', compression='gzip', index=False)


def convert_to_unix(time_str, base_date, timezone_str):
    time_parts = [int(part) for part in time_str.split(':')]
    days_to_add = 0
    if time_parts[0] >= 24:
        time_parts[0] -= 24
        days_to_add = 1
    naive_base_date = base_date.replace(tzinfo=None)
    time_obj = naive_base_date.replace(hour=time_parts[0], minute=time_parts[1], second=time_parts[2])
    time_obj += timedelta(days=days_to_add)
    local_timezone = pytz.timezone(timezone_str)
    local_dt = local_timezone.localize(time_obj, is_dst=None)
    return int(local_dt.timestamp())


def create_route_info(row):
    direction_mapping = {'E': 'EST', 'O': 'OUEST', 'S': 'SUD', 'N': 'NORD'}
    trip_headsign = row['trip_headsign']
    if isinstance(trip_headsign, str):
        direction = trip_headsign[-1]
        translated_direction = direction_mapping.get(direction, direction)
        route_info = f"{row['route_id']} {row['route_long_name']} dir. {translated_direction}"
    else:
        route_info = f"{row['route_id']} {row['route_long_name']}"
    return route_info