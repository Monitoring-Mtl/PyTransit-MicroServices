import boto3
import os
import io
import polars as pl
import concurrent.futures
from datetime import datetime, timedelta
import pytz


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    input_bucket = event['input_bucket'] 
    output_bucket = event['output_bucket']
    timezone = event.get('timezone', 'America/Montreal')  # Default to 'America/Montreal' if not specified
    workers = event.get('workers', 1) # Default 1

    # Initialize the timezone
    eastern = pytz.timezone(timezone)

    # Extract the date from the event, or use the current date in the specified timezone
    date_str = event.get('date', datetime.now(eastern).strftime('%Y%m%d'))

    # Parse the date string into a datetime object
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    date_obj = date_obj - timedelta(days=1)
    formatted_date = date_obj.strftime('%Y-%m-%d')

    # We use "Bucket/YYYY/MM/DD/... as a folder structure to benefit from Partition since our query will be mainly with based on date
    # Folder structure for S3 keys
    folder_structure = date_obj.strftime('%Y/%m/%d')
    prefix = f"{folder_structure}/"

    all_columns = set()

    # Paginate through files in the S3 bucket
    paginator = s3.get_paginator('list_objects_v2')
    file_keys = []
    for page in paginator.paginate(Bucket=input_bucket, Prefix=prefix):
        for content in page['Contents']:
            file_key = content['Key']
            if file_key.endswith('.parquet'):
                file_keys.append(file_key)
                try:
                    df = download_from_s3(input_bucket, file_key)
                    all_columns.update(df.columns)
                except Exception as e:
                    print(f"Error processing file: {file_key}")
                    print(e)
                    continue

    all_columns.add('timefetch')
    all_columns = sorted(all_columns)  # Convert to a sorted list for consistent order

    # Process files
    processed_dfs = process_files(file_keys, all_columns, input_bucket, workers)
    merged_df = pl.concat(processed_dfs)

    # Upload merged DataFrame to S3
    output_file_key = f'{folder_structure}/Daily_GTFS_VehiclePosition_{formatted_date}.parquet'
    upload_to_s3(output_bucket, output_file_key, merged_df)

    return {
        'statusCode': 200,
        'body': 'Data processing and upload completed successfully.'
    }


def download_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        return pl.read_parquet(io.BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"Error downloading file {file_key} from S3: {e}")
        return None


def upload_to_s3(bucket_name, key, dataframe):
    s3 = boto3.client('s3')
    temp_file_path = '/tmp/file.parquet'

    try:
        # Save DataFrame to Parquet file in /tmp with gzip compression
        dataframe.write_parquet(temp_file_path, compression="gzip")

        # Upload the file to S3
        s3.upload_file(temp_file_path, bucket_name, key)
        print(f'Successfully stored {key} in S3.')
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
    finally:
        # Remove the file from /tmp
        try:
            os.remove(temp_file_path)
        except Exception as e:
            print(f"Failed to delete temporary file: {e}")


def process_file(file_key, all_columns, source_bucket_name):

    df = download_from_s3(source_bucket_name, file_key)
    if df is None:
        return None

    # Extract the UNIX timestamp from the file name
    try:
        # The UNIX timestamp is located before the '.parquet' in the file name
        unix_timefetch = int(file_key.split('_')[-1].split('.')[0])
    except Exception as e:
        print(f"Error extracting UNIX timestamp from file name: {file_key}")
        print(e)
        return None

    # Add missing columns with default values (in case there is missing columns)
    for col in all_columns:
        if col not in df.columns:
            if col == 'timefetch':
                df = df.with_columns(pl.lit(unix_timefetch).alias(col))
            else:
                df = df.with_columns(pl.lit(None).alias(col))

    # Sort the columns
    df = df.select(sorted(all_columns))

    return df


def process_files(file_keys, all_columns, source_bucket_name, workers):
    """
    Process multiple files using multithreading.(not sure that it makes a difference in Lambda...)
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_file, file_key, all_columns, source_bucket_name) for file_key in file_keys]
        dfs = []
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            if df is not None:
                dfs.append(df)
        return dfs