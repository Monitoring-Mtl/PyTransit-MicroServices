import json
import boto3
import os
import pandas as pd
import pytz
from datetime import datetime
from urllib.request import Request, urlopen
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    atlas_uri = os.environ.get('ATLAS_URI')
    db_name = os.environ.get('MONGO_DATABASE_NAME')
    collection_name = event['collection_name']
    bucket_name = event['bucket_name']
    temp_file_path = event.get('file_path', '/tmp/file.parquet')

    eastern = pytz.timezone('America/Toronto')
    now = datetime.now(eastern)
    fetch_time_unix = int(now.timestamp())
    folder_name = now.strftime('%Y-%m-%d')

    # Fetch data from the API
    request = Request(event['url'])
    try:
        response = urlopen(request)
        data = response.read()
    except Exception as e:
        print(f'Error during data fetch and store: {e}')
        raise

    #Transform to .parquet
    # Deserialize JSON data
    data_dict=json.loads(data)
    #Create dataframe with only the stations status
    stations = data_dict["data"]["stations"]
    df = pd.DataFrame(stations)

    # Save Dataframe 
    df.to_parquet(temp_file_path, compression="gzip")

    # Define S3 object name
    s3_file = f'{folder_name}/gbfs_data_station_status_{fetch_time_unix}.parquet'

    try:
        s3.upload_file(temp_file_path, bucket_name, s3_file)
        print(f'Successfully stored {s3_file} in S3.')
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        return
    finally:
        # Always try to remove the file from /tmp, even if upload fails
        try:
            os.remove(temp_file_path)
        except Exception as e:
            print(f"Failed to delete temporary file: {e}")
    

    # Write to Mongo
    mongoClient = MongoClient(atlas_uri, server_api=ServerApi('1'), tls=True, tlsAllowInvalidCertificates=True)
    try:
        db = mongoClient[db_name]
        collection = db[collection_name]
        collection.insert_many(stations)
        print("inserted documents")
    except Exception as e:
        print(f"Mongo insert returned an error: {e}")

# You can set the environment variables 'API_URL' and 'bucket_name' accordingly.
# Example usage: lambda_handler({'bucket_name': 'my-s3-bucket'}, None)
