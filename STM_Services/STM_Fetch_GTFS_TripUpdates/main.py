from google.protobuf.json_format import MessageToDict
import pandas as pd
import boto3
import fastparquet
from google.transit import gtfs_realtime_pb2
from urllib.request import Request, urlopen
from datetime import datetime
import pytz
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection

# Set up S3
s3 = boto3.client('s3')


def lambda_handler(event, context): 
    api_url = os.environ.get('API_URL_STM_TRIP')
    api_key = os.environ.get('API_KEY_STM')

    bucket_name = event['bucket_name']
    mongo_collection = event['collection_name']
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

        # Write to mongo
        trip_updates = [{
            'tripUpdateId': api_trip_update.get('id', None),
            'timestamp': api_trip_update.get('tripUpdate', None).get('timestamp', None),
            'trip': api_trip_update.get('tripUpdate', None).get('trip', None),
            'stopTimeUpdate': api_trip_update.get('tripUpdate', None).get('stopTimeUpdate', None),
        } for api_trip_update in json_data_dict['entity']]
        write_trip_updates_to_mongo(mongo_collection, trip_updates)

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

def get_mongo_collection(collection_name) -> Collection:
    try:
        atlas_uri = os.environ.get('ATLAS_URI')
        db_name = os.environ.get('MONGO_DATABASE_NAME')
        mongoClient = MongoClient(atlas_uri, server_api=ServerApi('1'), tls=True, tlsAllowInvalidCertificates=True)
        db = mongoClient[db_name]
        return db[collection_name]
    except Exception as e:
        print(e)

def write_trip_updates_to_mongo(collection: Collection, trip_updates):
    collection.insert_many(trip_updates)
