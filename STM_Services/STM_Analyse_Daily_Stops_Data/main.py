import boto3
import gzip
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from io import StringIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # NEEDS TO HAVE 8192MB of memory
    # Define some variables based on the event.
    json_bucket_name = event['json_bucket_name']
    csv_bucket_name = event['csv_bucket_name']
    updated_files_bucket_name = event['updated_files_bucket_name']
    event_date = event['date']  # e.g., '20231113'
    timezone = event['timezone']  # e.g., 'Canada/Eastern'

    eastern_tz = pytz.timezone('Canada/Eastern')

    TIME_THRESHOLD = 18000  # 5 hours in seconds

    # We get the date to evaluate and also determine the next day for the GTFS Json files
    event_datetime = datetime.strptime(event_date, '%Y%m%d')
    next_day = event_datetime + timedelta(days=1)

    # Calculate UNIX timestamp for 7:20 AM on the next day to use a Threshold when fetching from S3 bucket
    seven_twenty_am_unix = time_to_unix("07:20:00", next_day, timezone)
    
    # Determine the folders name in format # e.g. '2023/11/09' & '2023/11/10'
    event_date_folder = event_datetime.strftime('%Y/%m/%d')
    next_day_folder = next_day.strftime('%Y/%m/%d')

    # Create the dictionary for the GTFS Live data
    json_dict = {}

    event_date_csv = event_datetime.strftime('%Y/%m/%d')
    file_date = event_datetime.strftime('%Y-%m-%d')

    csv_file_name = f'{event_date_csv}/filtered_stop_times/filtered_stop_times_{file_date}.csv'
    csv_obj = s3.get_object(Bucket=csv_bucket_name, Key=csv_file_name)
    csv_data = pd.read_csv(csv_obj['Body'])

    # Convert 'arrival_time' to UNIX timestamps
    csv_data['arrival_time_unix'] = csv_data['arrival_time'].apply(lambda x: time_to_unix(x, file_date, timezone))

    # Prepare a new column for the offset
    csv_data['offset'] = None
    csv_data['Current_Occupancy'] = None

    # To store the last stop sequence for each trip
    last_stop_sequences = csv_data.groupby('trip_id')['stop_sequence'].max().to_dict()

    # To keep track of the previous trip and stop sequence for each vehicle
    previous_trip_stop = {}

    # Process data from event_date_folder and calculate offsets
    json_dict = process_json_files_s3(json_bucket_name, f'{event_date_folder}/', event_datetime, eastern_tz)
    process_offsets(csv_data, json_dict, previous_trip_stop, last_stop_sequences, TIME_THRESHOLD)

    # Print the number of items (keys) in json_dict
    print(f"Number of items in json_dict: {len(json_dict)}")

    # Process data from next_day_folder, skipping already processed trip_ids
    json_dict_next_day = process_json_files_s3(json_bucket_name, f'{next_day_folder}/', event_datetime, eastern_tz, seven_twenty_am_unix)
    process_offsets(csv_data, json_dict_next_day, previous_trip_stop, last_stop_sequences, TIME_THRESHOLD, skip_processed=True)
    
    # Print the number of items (keys) in json_dict_next_day
    print(f"Number of items in json_dict: {len(json_dict_next_day)}")
    
    # Save CSV back to S3
    csv_buffer = StringIO()
    csv_data.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=updated_files_bucket_name, Body=csv_buffer.getvalue(), Key=f'{event_date_csv}'
                                                                                    f'/updated_filtered_stop_times_{file_date}.csv')


def process_json_files_s3(bucket_name, prefix, event_datetime, timezone, cutoff_timestamp=None):
    json_dict = {}
    file_count = 0
    continuation_token = None
    next_day = event_datetime + timedelta(days=1)

    while True:
        # List objects with pagination
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        for item in response.get('Contents', []):
            key = item['Key']
            if key.endswith('.json.gz'):
                # Extract timestamp from file name
                file_timestamp = int(key.split('_')[-1].split('.')[0])
                
                # Check if file_timestamp is for the next day
                file_datetime = datetime.fromtimestamp(file_timestamp, timezone)
                file_date = file_datetime.date()
                
                # Process the file only if its timestamp is less than or equal to the cutoff timestamp
                if cutoff_timestamp is None or file_timestamp <= cutoff_timestamp:
                    file_count += 1
                    json_obj = s3.get_object(Bucket=bucket_name, Key=key)
                    with gzip.GzipFile(fileobj=json_obj['Body']) as gzipfile:
                        json_data = json.load(gzipfile)
                        entities = json_data.get('entity', [])
                        for entity_item in entities:
                            trip_id = entity_item['vehicle']['trip']['tripId']
                            if trip_id not in json_dict:
                                json_dict[trip_id] = []
                            json_dict[trip_id].append(entity_item)

        # Check if there are more files to fetch
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    print(f"Number of '.json.gz' files in bucket '{bucket_name}' with prefix '{prefix}': {file_count}")
    return json_dict



# Iterate through the rows of the CSV data and calculate the offset value and occupation_level
def process_offsets(csv_data, json_dict, previous_trip_stop, last_stop_sequences, TIME_THRESHOLD, skip_processed=False):
    for index, row in csv_data.iterrows():
        trip_id = str(row['trip_id'])
        stop_sequence = row['stop_sequence']
        if trip_id in json_dict:

            # Skip processing this trip_id if 'offset' already exists and skip_processed is True
            if skip_processed and csv_data.at[index, 'offset'] is not None:
                continue

            for item in json_dict[trip_id]:
                # Data from GTFS
                #vehicle_id = item['vehicle']['vehicle']['id']
                #vehicle_trip_id = item['vehicle']['trip']['tripId']
                try:
                    vehicle_stop_sequence = item['vehicle']['currentStopSequence']
                    vehicle_status = item['vehicle']['currentStatus']
                    
                except (TypeError, ValueError) as e:
                    print(f"Error with trip_id: {trip_id} when processing the offset calculation")
                # Normal processing for current trip 
                if vehicle_stop_sequence >= stop_sequence and vehicle_status in ["IN_TRANSIT_TO", "STOPPED_AT"]:
                    try:
                        vehicle_current_occupancy = item['vehicle'].get('occupancyStatus', 'Unknown')  # Default value as 'Unknown'
                        #vehicle_current_occupancy = item['vehicle']['occupancyStatus']
                        arrival_time_unix = int(row['arrival_time_unix']) 
                        timestamp_unix = int(item['vehicle']['timestamp'])  
                        offset = timestamp_unix - arrival_time_unix
                        if not isinstance(vehicle_current_occupancy, str):
                            raise TypeError(f"Vehicle_Occupancy is not an integer. Current Occupancy: {vehicle_current_occupancy}, Type: {type(offset)}, trip_id: {trip_id}") 

                        if not isinstance(offset, int):
                            raise TypeError(f"Offset is not an integer. Offset: {offset}, Type: {type(offset)}")

                        if abs(offset) <= TIME_THRESHOLD:
                            csv_data.at[index, 'Stop_Sequence_Found'] = vehicle_stop_sequence
                            csv_data.at[index, 'offset'] = offset
                            csv_data.at[index, 'Current_Occupancy'] = vehicle_current_occupancy
                            break  # Break as soon as a match is found

                    except (TypeError, ValueError) as e:
                        print(f"Error in offset calculation: {e}")
                        print(f"trip_id: {trip_id}, timestamp_unix: {timestamp_unix}, arrival_time_unix: {arrival_time_unix}")


def time_to_unix(time_str, base_date, timezone_str):
    time_parts = [int(part) for part in time_str.split(':')]
    if time_parts[0] >= 24:
        time_parts[0] -= 24
        base_date += timedelta(days=1)
    time_obj = base_date.replace(hour=time_parts[0], minute=time_parts[1], second=time_parts[2])
    local_timezone = pytz.timezone(timezone_str)
    local_dt = local_timezone.localize(time_obj, is_dst=None)
    return int(local_dt.timestamp())
