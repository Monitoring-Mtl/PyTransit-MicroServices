import json
import boto3
import os
import time
import pytz
import io
import fastparquet
import polars as pl
import polars.selectors as cs

from datetime import datetime, timedelta
from urllib.request import Request, urlopen

# Set up S3 client
s3 = boto3.client('s3')

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

def save_dataframe_to_database(dataframe):
    print('TODO:')

def lambda_handler(event, context):
    # Define the buckets and file paths
    stm_analytics_bucket = event['input_bucket']
    timezone_str = event.get('timezone', 'America/Montreal')  # Default to 'America/Montreal' if not specified
    eastern = pytz.timezone(timezone_str)

    # Extract the date from the event, or use the current date in the specified timezone (Format YYYYMMDD)
    date_str = event.get('date', datetime.now(eastern).strftime('%Y%m%d'))

    # Parse the date string into a datetime object
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    date_obj = eastern.localize(date_obj)
    folder_name = date_obj.strftime('%Y/%m/%d')
    
    # Get daily analytics data from parquet
    daily_data = get_daily_parquet_file(stm_analytics_bucket, folder_name)

    # Filter out null offsets, if any
    daily_data = daily_data.filter(pl.col('offset').is_not_null())

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
    daily_data = daily_data.select(['trip_id', 'stop_id', 'previous_stop_id', 'offset_difference', 'Current_Occupancy', 'arrival_time_unix'])

    save_dataframe_to_database(daily_data)