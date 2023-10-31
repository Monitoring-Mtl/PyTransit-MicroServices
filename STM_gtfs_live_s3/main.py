from google.protobuf.json_format import MessageToDict
import json
import boto3
from urllib.request import Request, urlopen
from google.transit import gtfs_realtime_pb2
import datetime

# Define the URL of the API
api_url = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions"
api_key = "MyKey"

# Set up the request headers with the API key
headers = {
    "apikey": api_key
}

# Set up S3
s3 = boto3.client('s3')
bucket_name = 'your-bucket-name'

def lambda_handler(event, context):
    fetch_data()
    return {"status": "success"}

def fetch_data():
    fetch_time_unix = int(datetime.datetime.now().timestamp())
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

    # Define S3 object name
    object_name = f'gtfs_data_{fetch_time_unix}.json'

    try:
        # Store the JSON object in S3
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=json_data)
        print(f'Successfully stored {object_name} in S3.')
    except Exception as e:
        print(f'Failed to store {object_name} in S3 due to an exception: {e}')
