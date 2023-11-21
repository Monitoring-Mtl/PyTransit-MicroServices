from google.protobuf.json_format import MessageToDict
import json
import boto3
from google.transit import gtfs_realtime_pb2
from urllib.request import Request, urlopen
from datetime import datetime, timedelta
import gzip
import pytz
import os

api_url = os.environ.get('API_URL_STM_TRIP')
api_key = os.environ.get('API_KEY_STM')

# Set up S3
s3 = boto3.client('s3')
bucket_name = ""


def lambda_handler(event, context): 
    global bucket_name 
    bucket_name = event['bucket_name']
    fetch_data()


def fetch_data():
    eastern = pytz.timezone('America/Toronto')
    now = datetime.now(eastern)
    fetch_time_unix = int(now.timestamp())
    folder_name = now.strftime('%Y-%m-%d')

    feed = gtfs_realtime_pb2.FeedMessage()
    request = Request(api_url)
    request.add_header('apikey', api_key)
    try:
        response = urlopen(request)
    except Exception as e:
        print(f"Failed to make the request: {e}")
        return
    feed.ParseFromString(response.read())

    # Convert the feed object to a JSON-serializable dictionary
    json_data_dict = MessageToDict(feed)

    # Serialize the dictionary to a JSON string
    json_data = json.dumps(json_data_dict)

    # Compress the JSON data using gzip
    json_data_gzipped = gzip.compress(json_data.encode('utf-8'))

    # Define S3 object name
    object_name = f'{folder_name}/gtfs_data_{fetch_time_unix}.json.gz'  
    try:
        # Store the gzipped JSON object in S3
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=json_data_gzipped)
        print(f'Successfully stored {object_name} in S3.')
    except Exception as e:
        print(f'Failed to store {object_name} in S3 due to an exception: {e}')
        raise
