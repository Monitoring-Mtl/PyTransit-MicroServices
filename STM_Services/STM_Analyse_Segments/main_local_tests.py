import json
import boto3
import os
import time
import pytz
import io
import fastparquet
import polars as pl
import pandas as pd
import polars.selectors as cs

from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from gridfs import GridFS

# Set up S3 client
s3 = boto3.client('s3', aws_access_key_id=os.environ['aws_access_key_id'], aws_secret_access_key=os.environ['aws_secret_access_key'])
    
def download_file_to_tmp(bucket, key):
    local_path = f"/tmp/{os.path.basename(key)}"
    s3.download_file(Bucket=bucket, Key=key, Filename=local_path)
    return local_path

def download_from_s3(bucket_name, file_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        return pl.read_parquet(io.BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"Error downloading file {file_key} from S3: {e}")
        return None

def get_daily_parquet_file(bucket_name, prefix):

    # List objects in the bucket with the specified prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    # If there are objects in 'Contents', get the first one
    if 'Contents' in response and len(response['Contents']) > 0:
        first_object = response['Contents'][0]
        
        # Check if it's a Parquet file
        if first_object['Key'].endswith('.parquet'):
            # Download the Parquet file
            print(bucket_name, first_object[('Key')])
            return download_from_s3(bucket_name, first_object['Key'])
        else:
            print('The first object is not a Parquet file.')

    else:
        print('No objects found in the specified prefix.')
    return None

def save_dataframe_to_db(event, df): 
    atlas_uri = os.environ['ATLAS_URI']
    db_name = os.environ['MONGO_DATABASE_NAME']
    mongoClient = MongoClient(atlas_uri)
    db = mongoClient[db_name]
    collection_name = event['collection_name']
    collection = db[collection_name]
    df = df.to_pandas()
    df.reset_index(inplace=True)
    collection.insert_many(df.to_dict('records'))
    

def lambda_handler(event, context):
    # Define the buckets and file paths
    stm_analytics_bucket = event['input_bucket']
    timezone_str = event.get('timezone', 'America/Montreal')  # Default to 'America/Montreal' if not specified
    eastern = pytz.timezone(timezone_str)
    date_str = event.get('date', datetime.now(eastern).strftime('%Y%m%d'))
    #test pour 2024/01/01 et 2023/11/02
    daily_data_01 = get_daily_parquet_file(stm_analytics_bucket, '2024/01/01')
    daily_data_01 = daily_data_01.filter(pl.col('offset').is_not_null())
    daily_data_02 = get_daily_parquet_file(stm_analytics_bucket, '2023/11/02')
    daily_data_02 = daily_data_02.filter(pl.col('offset').is_not_null())
    analytics_data = pl.concat([daily_data_01, daily_data_02], rechunk=True)

    # Filter out null offsets, if any
    daily_data = analytics_data.filter(pl.col('offset').is_not_null())

    # Sort the DataFrame by trip_id, stop_sequence, and arrival_time_unix for sequential analysis in time
    daily_data = daily_data.sort(by=['trip_id', 'stop_sequence', 'arrival_time_unix'])

    # Add a column with the previous stop's id
    daily_data = daily_data.with_columns(
        pl.col('stop_id').shift(1).alias('previous_stop_id')
    )

    # Calculate the time differences between consecutive stops
    daily_data = daily_data.with_columns(
        (pl.col('offset') - pl.col('offset').shift(1)).alias('offset_difference')
    )

    # Add a column for the previous trip's id
    daily_data = daily_data.with_columns(pl.col('trip_id').shift(1).alias('previous_trip_id'))

    # Filter out rows where trip_id changes, as we are interested only in consecutive stops during the same trip
    # This prevents events where a stop is skipped and the data would be wrong (for example: shadow buses)
    daily_data = daily_data.filter(pl.col("trip_id") == pl.col("previous_trip_id"))

    # Filter out rows with null time_difference (sometimes first/last row)
    daily_data = daily_data.filter(pl.col('offset_difference').is_not_null())

    # Cast offset_difference to integer
    daily_data = daily_data.cast({'offset_difference': pl.Int64})

    # Select only relevant columns before saving to database
    daily_data = daily_data.select(['stop_id', 'routeId', 'previous_stop_id', 'offset_difference', 'Current_Occupancy', 'arrival_time_unix', 'trip_id'])
    save_dataframe_to_db(event, daily_data, 15)

if __name__ == '__main__':
    event = {}
    event['collection_name'] = 'monitoring-mtl-stm-segments-analysis'
    event['input_bucket'] = 'monitoring-mtl-stm-analytics'
    event['output_bucket'] = 'monitoring-mtl-stm-gtfs-vehicle-positions-daily-merge'
    lambda_handler(event, None)