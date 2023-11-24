import boto3
from boto3.dynamodb.conditions import Key
import pytz
from datetime import datetime
import time
import json
from decimal import Decimal

########## FYI ################# VA être fait dans l'autre REPO
def get_current_unix_time(timezone):
    tz = pytz.timezone(timezone)
    now = datetime.now(tz)
    return int(now.timestamp())

def query_unique_route_info(route_id, dynamodb=None):
    if dynamodb is None:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('STM_Daily_Stops_Info')

    route_id_int = int(route_id)

    # Start timing for query
    start_query_time = time.time()

    response = table.query(
        IndexName='route_id-index',
        KeyConditionExpression=Key('route_id').eq(route_id_int)
    )

    end_query_time = time.time()

    items = response['Items']
    unique_route_info = set(item['route_info'] for item in items)

    return list(unique_route_info), end_query_time - start_query_time

def query_all_trips(route_info, dynamodb=None):
    if dynamodb is None:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('STM_Daily_Stops_Info')

    response = table.query(
        KeyConditionExpression=Key('route_info').eq(route_info)
    )

    return response['Items']

def find_current_trip(trips, current_time_unix):
    current_trip_id = None
    for trip in trips:
        if 'arrival_time_unix' in trip and trip['arrival_time_unix'] <= current_time_unix:
            current_trip_id = trip['trip_id']
            break
    return current_trip_id

def query_rows_by_trip_id(trip_id, dynamodb=None):
    if dynamodb is None:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('STM_Daily_Stops_Info')

    # Start timing for query
    start_query_time = time.time()

    response = table.query(
        IndexName='trip_id-arrival_time_unix-index',
        KeyConditionExpression=Key('trip_id').eq(trip_id)
    )

    end_query_time = time.time()

    return response['Items'], end_query_time - start_query_time

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)

def save_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, cls=DecimalEncoder, indent=4)

# Start timing for the entire process
start_process_time = time.time()

# Example usage
route_id = 90  # Example route_id
unique_routes, query_duration = query_unique_route_info(route_id)
print(f"Unique Routes: {unique_routes}")
print(f"Query Duration: {query_duration} seconds")

selected_route_info = unique_routes[0] if unique_routes else None

if selected_route_info:
    timezone = 'America/Montreal'
    current_time_unix = get_current_unix_time(timezone)

    trips = query_all_trips(selected_route_info)
    current_trip_id = find_current_trip(trips, current_time_unix)

    if current_trip_id:
        trip_rows, trip_query_duration = query_rows_by_trip_id(current_trip_id)
        print(f"Trip Query Duration: {trip_query_duration} seconds")
        json_filename = 'current_trip_info.json'
        save_to_json(trip_rows, json_filename)
        print(f"Trip information saved to {json_filename}")
    else:
        print("No current trip found for the selected route.")
else:
    print("No routes found for the given route_id.")

end_process_time = time.time()
total_process_duration = end_process_time - start_process_time
print(f"Total Process Duration: {total_process_duration} seconds")
