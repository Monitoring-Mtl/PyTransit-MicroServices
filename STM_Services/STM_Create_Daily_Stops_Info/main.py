import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import pytz

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    static_bucket = event['static_bucket']
    daily_static_bucket = event['daily_static_bucket']
    output_bucket = event['output_bucket']
    local_timezone = event['timezone']
    #'America/Montreal'

    eastern = pytz.timezone(local_timezone)
    now = datetime.now(eastern)
    folder_name = now.strftime('%Y-%m-%d')

    filtered_trips_path = f'{folder_name}/filtered_trips/filtered_trips.csv'
    filtered_stop_times_path = f'{folder_name}/filtered_stop_times/filtered_stop_times.csv'

    # Read the necessary files from S3
    stops_df = read_csv_from_s3(static_bucket, 'stops/stops.csv')
    filtered_trips_df = read_csv_from_s3(daily_static_bucket, filtered_trips_path)
    filtered_stop_times_df = read_csv_from_s3(daily_static_bucket, filtered_stop_times_path)
    routes_df = read_csv_from_s3(static_bucket, 'routes/routes.csv')

    # Process the stop times into UNIX timestamp
    filtered_stop_times_df['arrival_time_unix'] = filtered_stop_times_df['arrival_time'].apply(lambda x: convert_to_unix(x, now, local_timezone))

    # Ensure data types for 'stop_id' match
    stops_df['stop_id'] = stops_df['stop_id'].astype(str)
    filtered_stop_times_df['stop_id'] = filtered_stop_times_df['stop_id'].astype(str)

    # Merge DataFrames and select specific columns to merge from filtered_trips_df
    merged_df = pd.merge(
        filtered_stop_times_df,
        filtered_trips_df[['trip_id', 'route_id', 'trip_headsign', 'direction_id', 'shape_id', 'wheelchair_accessible']],
        on='trip_id',
        how='left'
    )

    print(f'merged_df columns: {merged_df.columns}')
    print(f'stops_df columns: {stops_df.columns}')

    # Merge without creating duplicate columns
    merged_df = pd.merge(
        merged_df,
        stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'wheelchair_boarding']],
        on='stop_id',
        how='left'
    )
    merged_df = pd.merge(
        merged_df, 
        routes_df[['route_id', 'route_long_name']], 
        on='route_id',
        how='left'
    )

    # Create the route_info column (Ex : 16 Griffith / St-François dir. E)
    merged_df['route_info'] = merged_df.apply(create_route_info, axis=1)
    
    # Select required columns
    final_df = merged_df[['route_id', 'route_info', 'trip_id', 'shape_id', 'wheelchair_accessible', 
                          'arrival_time_unix', 'stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'wheelchair_boarding']]

    # Write the DataFrame to a new CSV file in S3
    output_file_path = f'{folder_name}/daily_stops_info/daily_stops_info.csv'
    write_df_to_csv_on_s3(final_df, output_bucket, output_file_path)

    return {
        'statusCode': 200,
        'body': f"File saved to {output_bucket}/{output_file_path}"
    }

def read_csv_from_s3(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    except s3_client.exceptions.NoSuchKey:
        print(f"No such key: {key} in bucket: {bucket}")
        # Handle the exception or re-raise it
        raise

def write_df_to_csv_on_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

def convert_to_unix(time_str, base_date, timezone_str):
    time_parts = [int(part) for part in time_str.split(':')]
    days_to_add = 0
    if time_parts[0] >= 24:
        time_parts[0] -= 24
        days_to_add = 1

    # Ensure base_date is naive before replacing its components
    naive_base_date = base_date.replace(tzinfo=None)

    time_obj = naive_base_date.replace(hour=time_parts[0], minute=time_parts[1], second=time_parts[2])
    time_obj += timedelta(days=days_to_add)  # Add extra day if needed

    local_timezone = pytz.timezone(timezone_str)
    local_dt = local_timezone.localize(time_obj, is_dst=None)

    return int(local_dt.timestamp())


def create_route_info(row):
    # Mapping for direction translation
    direction_mapping = {'E': 'EST', 'O': 'OUEST', 'S': 'SUD', 'N': 'NORD'}

    # Extract the last character from trip_headsign for direction
    direction = row['trip_headsign'][-1]

    # Translate direction and create route_info string
    translated_direction = direction_mapping.get(direction, direction)
    route_info = f"{row['route_id']} {row['route_long_name']} dir. {translated_direction}"

    return route_info