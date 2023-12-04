from google.protobuf.json_format import MessageToDict
import pandas as pd
import boto3
from google.transit import gtfs_realtime_pb2
from urllib.request import Request, urlopen
from datetime import datetime
import pytz
import os

api_url = os.environ.get('API_URL_STM_TRIP')
api_key = os.environ.get('API_KEY_STM')

# Set up S3
s3 = boto3.client('s3')


def lambda_handler(event, context): 

    bucket_name = event['bucket_name']
    timezone = event.get('timezone', 'America/Montreal')  # Default to 'America/Montreal' if not specified

    eastern = pytz.timezone(timezone)
    now = datetime.now(eastern)
    fetch_time_unix = int(now.timestamp())
    folder_name = now.strftime('%Y/%m/%d')

    feed = gtfs_realtime_pb2.FeedMessage()
    request = Request(api_url, headers={'apikey': api_key})

    try:
        response = urlopen(request)
    except Exception as e:
        print(f"Failed to make the request: {e}")
        return

    feed.ParseFromString(response.read())

    # Convert the feed object to a JSON-serializable dictionary
    json_data_dict = MessageToDict(feed)

    # Check if 'entity' is in data, if so normalize it  A VALIDER !!!!! DF['entity'] pour accéder aux DATAs
    if 'entity' in json_data_dict:
        df = pd.json_normalize(json_data_dict['entity'], sep='_')
    else:
        df = pd.DataFrame()

    # Sort the columns in the DataFrame
    df = df.sort_index(axis=1)

    # Save Dataframe 
    temp_file_path = '/tmp/file.parquet'
    df.to_parquet(temp_file_path, compression='gzip')

    # Define S3 key
    s3_file_key = f'{folder_name}/STM_GTFS_TripUpdates_{fetch_time_unix}.parquet'

    # Upload the file to S3 with try-except for error handling
    try:
        s3.upload_file(temp_file_path, bucket_name, s3_file_key)
        print(f'Successfully stored {s3_file_key} in S3.')
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        return
    finally:
        # Always try to remove the file from /tmp, even if upload fails
        try:
            os.remove(temp_file_path)
        except Exception as e:
            print(f"Failed to delete temporary file: {e}")