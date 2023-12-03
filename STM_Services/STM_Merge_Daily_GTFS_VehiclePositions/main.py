import boto3
import os
import shutil
import polars as pl
import concurrent.futures
import datetime
import pytz
import time

# Initialize Boto3 S3 client
s3 = boto3.client('s3')

def upload_to_s3(bucket, key, local_path):
    s3.upload_file(local_path, bucket, key)

def download_and_process_file(bucket, key, all_columns):
    try:
        # Download file from S3 into memory
        response = s3.get_object(Bucket=bucket, Key=key)
        file_stream = response['Body']

        # Read the file into a DataFrame
        df = pl.read_parquet(file_stream)

        # Add the timefetch column
        timefetch = int(key.split('_')[2].split('.')[0])
        df = df.with_columns(pl.lit(timefetch).alias("timefetch"))

        # Add missing columns with default values
        for col in all_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        df = df.select(list(all_columns))
        return df
    except pl.exceptions.SchemaError as e:
        print(f"Error processing file: {key}")
        print(e)
        return None


def process_files(bucket, keys, all_columns):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_df = {executor.submit(download_and_process_file, bucket, key, all_columns): key for key in keys}
        dfs = []
        for future in concurrent.futures.as_completed(future_to_df):
            df = future.result()
            if df is not None:
                dfs.append(df)
        return dfs
    

def list_files_in_s3_bucket(bucket, prefix):
    files = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        files.extend(item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.parquet'))

        # Check if more files are available
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return files

def lambda_handler(event, context):
    input_bucket = event['input_bucket']
    output_bucket = event['output_bucket']
    input_prefix = event['intput_prefix']
    output_prefix = event['output_prefix']
    timezone = event.get('timezone', 'America/Montreal')  # Default to 'America/Montreal' if not specified

    # Initialize the timezone
    eastern = pytz.timezone(timezone)

    # Extract the date from the event, or use the current date in the specified timezone
    date_str = event.get('date', datetime.now(eastern).strftime('%Y%m%d'))
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    formatted_date = date_obj.strftime('%Y-%m-%d')
    folder_structure = date_obj.strftime('%Y/%m/%d')

    s3_keys = list_files_in_s3_bucket(input_bucket, input_prefix)

    # Determine columns
    all_columns = set()
    for key in s3_keys:
        response = s3.get_object(Bucket=input_bucket, Key=key)
        file_stream = response['Body']
        df = pl.read_parquet(file_stream)
        all_columns.update(df.columns)

    all_columns.add('timefetch')
    all_columns = sorted(all_columns)  # Convert to a sorted list for consistent order

    # Process files and merge into a single DataFrame
    dfs = process_files(input_bucket, s3_keys, all_columns)
    merged_df = pl.concat(dfs)

    try:
        # Use /tmp directory for local file operations in Lambda
        local_output_file = f"/tmp/Daily_GTFS_VehiclePosition_{formatted_date}.parquet"
        merged_df.write_parquet(local_output_file)

        # S3 key includes folder structure
        s3_key = f"{output_prefix}{folder_structure}/Daily_GTFS_VehiclePosition_{formatted_date}.parquet"

        # Upload the output file to S3
        upload_to_s3(output_bucket, s3_key, local_output_file)

        print(f"Data merged and saved in S3 bucket '{output_bucket}' at '{s3_key}'")

    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': f"Error occurred: {e}"
        }

    finally:
        # Clean up /tmp directory
        try:
            os.remove(local_output_file)
        except Exception as e:
            print(f"Failed to delete temporary file: {e}")

    return {
        'statusCode': 200,
        'body': f"Process completed for date {formatted_date}"
    }
