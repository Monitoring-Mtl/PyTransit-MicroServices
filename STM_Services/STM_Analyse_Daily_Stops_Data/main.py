import boto3
import gzip
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from io import StringIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Define some variables based on the event.
    json_bucket_name = event['json_bucket_name']
    csv_bucket_name = event['csv_bucket_name']
    updated_files_bucket_name = event['updated_files_bucket_name']
    event_date = event['date']  # e.g., '20231113'
    timezone = event['timezone']  # e.g., 'Canada/Eastern'

    # We get the date to evaluate and also determine the next day for the GTFS Json files
    event_datetime = datetime.strptime(event_date, '%Y%m%d')
    next_day = event_datetime + timedelta(days=1)

    # Determine the folders name in format # e.g. '2023-11-09'
    event_date_folder = event_datetime.strftime('%Y-%m-%d')
    next_day_folder = next_day.strftime('%Y-%m-%d')

    # Create the dictionary for the GTFS Live data
    json_dict = {}
    json_dict.update(process_json_files_s3(json_bucket_name, f'{event_date_folder}/'))
    json_dict.update(process_json_files_s3(json_bucket_name, f'{next_day_folder}/'))

    event_date_csv = event_datetime.strftime('%Y-%m-%d')
    csv_file_name = f'{event_date_csv}/filtered_stop_times.csv'
    csv_obj = s3.get_object(Bucket=csv_bucket_name, Key=csv_file_name)
    csv_data = pd.read_csv(csv_obj['Body'])

    # Convert 'arrival_time' to UNIX timestamps
    csv_data['arrival_time_unix'] = csv_data['arrival_time'].apply(lambda x: time_to_unix(x, event_datetime, timezone))

    # Prepare a new column for the offset
    csv_data['offset'] = None

    # Iterate through the rows of the CSV data
    for index, row in csv_data.iterrows():
        trip_id = str(row['trip_id'])
        stop_sequence = row['stop_sequence']
        if trip_id in json_dict:
            for item in json_dict[trip_id]:
                if item['vehicle']['currentStopSequence'] == stop_sequence:
                    arrival_time_unix = row['arrival_time_unix'] #CSV_file GTFS STATIC
                    timestamp_unix = int(item['vehicle']['timestamp']) #GTFS VehiclePositions
                    # Calculate the offset
                    offset = timestamp_unix - arrival_time_unix
                    # Update the offset column
                    csv_data.at[index, 'offset'] = offset
                    break  # Break as soon as a match is found to avoid overwriting with subsequent JSON files

    # Save CSV back to S3
    csv_buffer = StringIO()
    csv_data.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=updated_files_bucket_name, Body=csv_buffer.getvalue(), Key=f'{event_date_csv}'
                                                                                    f'/updated_filtered_stop_times.csv')


def process_json_files_s3(bucket_name, prefix):
    json_dict = {}
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    for item in response.get('Contents', []):
        key = item['Key']
        if key.endswith('.json.gz'):
            json_obj = s3.get_object(Bucket=bucket_name, Key=key)
            with gzip.GzipFile(fileobj=json_obj['Body']) as gzipfile:
                json_data = json.load(gzipfile)
                # Access the 'entity' field from json_data
                entities = json_data.get('entity', [])
                for entity_item in entities:
                    trip_id = entity_item['vehicle']['trip']['tripId']
                    if trip_id not in json_dict:
                        json_dict[trip_id] = []
                    json_dict[trip_id].append(entity_item)
    return json_dict


# Function to convert UNIX timestamp to seconds of the day
def unix_to_seconds(unix_timestamp, timezone):
    dt = datetime.fromtimestamp(unix_timestamp, tz=pytz.timezone(timezone))
    seconds = (dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    return seconds


def time_to_unix(time_str, base_date, timezone_str):
    time_parts = [int(part) for part in time_str.split(':')]
    if time_parts[0] >= 24:
        time_parts[0] -= 24
        base_date += timedelta(days=1)
    time_obj = base_date.replace(hour=time_parts[0], minute=time_parts[1], second=time_parts[2])
    local_timezone = pytz.timezone(timezone_str)
    local_dt = local_timezone.localize(time_obj, is_dst=None)
    return int(local_dt.timestamp())