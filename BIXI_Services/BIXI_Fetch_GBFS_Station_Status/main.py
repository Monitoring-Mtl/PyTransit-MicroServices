import json
import boto3
import os
import pandas as pd
import pytz
import fastparquet
from datetime import datetime
from urllib.request import Request, urlopen

# Set up S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = event['bucket_name']
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
    df = pd.DataFrame(data_dict["data"]["stations"])

    # Save Dataframe 
    temp_file_path = '/tmp/file.parquet'
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
    

# You can set the environment variables 'API_URL' and 'bucket_name' accordingly.
# Example usage: lambda_handler({'bucket_name': 'my-s3-bucket'}, None)
