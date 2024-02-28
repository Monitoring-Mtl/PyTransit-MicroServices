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


def filter_dataframe_by_route(df, route_number):
    return df.filter(df['route_id'] == str(route_number))

def lambda_handler(event, context):
    # Define the buckets and file paths
    input_bucket = event['input_bucket']
    daily_static_bucket = event['daily_static_bucket']
    output_bucket = event['output_bucket']
    timezone = event.get('timezone', 'America/Montreal')  # Default to 'America/Montreal' if not specified

    # Setup the timezone
    eastern = pytz.timezone(timezone)

    yesterday = datetime.now() - timedelta(days=1)
    # Format as 'YYYY-MM-DD'
    #yesterday_string = yesterday.strftime('%Y-%m-%d ')
    yesterday_string = '2024-01-29 '
    yesterday_folder_name = yesterday.strftime('%Y/%m/%d/')
    yesterday_static_folder_name = yesterday.strftime('%Y/%m/%d/filtered_trips/')

    daily_static_data_df = get_daily_parquet_file(daily_static_bucket, '2024/01/29')

    daily_data_df = get_daily_parquet_file(input_bucket, '2024/01/29')
    daily_data_df = daily_data_df.select(
        pl.col("trip_id"),
        pl.col("stop_id").cast(pl.Int64),
        pl.col("arrival_time_unix")
    )

    merged_df = daily_static_data_df.join(daily_data_df, left_on=["trip_id", "stop_id"], right_on=["trip_id", "stop_id"]).select(cs.by_name('trip_id', 'stop_id','arrival_time', 'arrival_time_unix'))

    #rename for better comprehension
    merged_df = merged_df.rename({'arrival_time': 'expected_arrival_time'})

    #convert unix timestamp to datetime
    merged_df = merged_df.with_columns(
       pl.from_epoch("arrival_time_unix", time_unit="s")
   )
    merged_df = merged_df.rename({'arrival_time_unix': 'actual_arrival_time'})
    merged_df = merged_df.with_columns(yesterday_string + pl.col("expected_arrival_time"))
    merged_df = merged_df.select(
        pl.col("trip_id"),
        pl.col("stop_id"),
        pl.col("literal").alias("expected_arrival_time"),
        pl.col("actual_arrival_time"),
    )
    merged_df = merged_df.with_columns(pl.col("expected_arrival_time").str.to_datetime("%Y-%m-%d %H:%M:%S"))

    time_difference = pl.col("actual_arrival_time") - pl.col("expected_arrival_time")

    offset_unix_timestamp = time_difference.dt.seconds() + time_difference.dt.days() * 24 * 3600
    
    merged_df = merged_df.with_columns(offset_unix_timestamp.alias("offset"))

    print(merged_df.head(1000))
    merged_df.head(100).write_csv("merged_df.csv")

if __name__ == '__main__':
    event = {}
    event['daily_static_bucket'] = 'monitoring-mtl-gtfs-static-daily'
    event['input_bucket'] = 'monitoring-mtl-gtfs-daily-stops-infos'
    event['output_bucket'] = 'monitoring-mtl-stm-gtfs-vehicle-positions-daily-merge'
    lambda_handler(event, None)