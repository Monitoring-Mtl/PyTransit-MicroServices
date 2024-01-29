import json
import boto3
import gzip
import os
import pytz
from datetime import datetime
from urllib.request import Request, urlopen

# Set the environment variable for the API URL
#api_url = os.environ.get('API_URL')
#api_url = 'https://gbfs.velobixi.com/gbfs/en/station_status.json'

# Set up S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = event['bucket_name']
    fetch_and_store_data(bucket_name, event['url'])

def fetch_and_store_data(bucket_name, api_url):
    eastern = pytz.timezone('America/Toronto')
    now = datetime.now(eastern)
    fetch_time_unix = int(now.timestamp())
    folder_name = now.strftime('%Y-%m-%d')

    try:
        # Fetch data from the API
        request = Request(api_url)
        response = urlopen(request)
        data = response.read()

        #AJOUTER TRANSFORMATION À .parquet QUELQUE PART ICI
        #ON PEUT SE FIER À LEUR FONCTION STM_Fetch_GTFS_VehiclePositions

        # Deserialize JSON data
        data_dict = json.loads(data)

        # Serialize the data back to a JSON string
        json_data = json.dumps(data_dict)

        # Compress the JSON data using gzip
        json_data_gzipped = gzip.compress(json_data.encode('utf-8'))

        # Define S3 object name
        object_name = f'{folder_name}/gbfs_data_station_status_{fetch_time_unix}.json.gz'

        # Store the gzipped JSON in the specified S3 bucket
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=json_data_gzipped)
        print(f'Successfully stored {object_name} in S3.')
    except Exception as e:
        print(f'Error during data fetch and store: {e}')
        raise

# You can set the environment variables 'API_URL' and 'bucket_name' accordingly.
# Example usage: lambda_handler({'bucket_name': 'my-s3-bucket'}, None)
