import pandas
import polars as pl
import boto3
import fastparquet
from datetime import datetime, timedelta
import time
import pytz
import os
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):

    bucket_static_daily = event['bucket_static_daily']
    bucket_vehicle_positions_daily_merge = event['bucket_vehicle_positions_daily_merge']
    output_bucket = event['output_bucket']
    timezone_str = event.get('timezone', 'America/Montreal')  # Default to 'America/Montreal' if not specified

    eastern = pytz.timezone(timezone_str)

    # Extract the date from the event, or use the current date in the specified timezone (Format YYYYMMDD)
    date_str = event.get('date', datetime.now(eastern).strftime('%Y%m%d'))

    # Parse the date string into a datetime object
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    date_obj = eastern.localize(date_obj)

    next_day = date_obj + timedelta(days=1)

    folder_name = date_obj.strftime('%Y/%m/%d')
    folder_name_next_day = next_day.strftime('%Y/%m/%d')


    file_name = date_obj.strftime('%Y-%m-%d')
    file_name_next_day = next_day.strftime('%Y-%m-%d')

    # Define the name of the local files
    local_static_file_name = f'/tmp/filtered_stop_times_{file_name}.parquet'
    local_file_name = f'/tmp/Daily_merge_{file_name}.parquet'
    local_file_name_next_day = f'/tmp/Daily_merge_{file_name_next_day}.parquet'

    # Define the folder and file structure on S3
    key_static = f'{folder_name}/filtered_stop_times/filtered_stop_times_{file_name}.parquet'
    key = f'{folder_name}/Daily_GTFS_VehiclePosition_{file_name}.parquet'
    key_next_day = f'{folder_name_next_day}/Daily_GTFS_VehiclePosition_{file_name_next_day}.parquet'

    # Download files locally
    local_path_static = download_file_to_tmp(bucket_static_daily, key_static, local_static_file_name)
    local_day_path = download_file_to_tmp(bucket_vehicle_positions_daily_merge, key, local_file_name)
    local_next_day_path = download_file_to_tmp(bucket_vehicle_positions_daily_merge, key_next_day, local_file_name_next_day)

    df = pl.read_parquet(local_day_path)
    df_next_day = pl.read_parquet(local_next_day_path)
    df_stop_times = pl.read_parquet(local_path_static)

    # Drop useless column
    df_stop_times = df_stop_times.drop({'departure_time'})
    # We create the new column 'arrival_time_unix' converting the time in UNIX.
    df_stops_unix = adding_arrival_time_unix(df_stop_times, date_obj)

    # Merge the two DFs (current_day + next_day) of VehiclePositions
    dfs_daily_vehicle_positions_merge = pl.concat([df, df_next_day], rechunk=True)

    # We rename a column and convert the type of others
    dfs_daily_vehicle_positions_merge = rename_and_convert_columns(dfs_daily_vehicle_positions_merge)

    dfs_daily_vehicle_positions_merge.write_parquet('dfs_daily_vehicle_positions_merge.parquet') #Used to generate the map

    # We proceed with the analysis of the DATA based on the daily stop_times file provided
    df_processed = process_based_on_daily_static_files(dfs_daily_vehicle_positions_merge, df_stops_unix)

    # We remove the duplicate rows
    df_processed = (df_processed.unique(subset=['trip_id', 'vehicle_currentStopSequence'])
                    .sort(['trip_id', 'vehicle_currentStopSequence', 'timefetch']))

    df_processed = df_processed.with_columns(((pl.col('arrival_time_offset') + pl.col('departure_time_offset'))/2).alias('offset'))

    # Select only the columns you want to keep from the right DataFrame
    df_processed = df_processed.select([
        'trip_id',
        'vehicle_currentStopSequence',
        'vehicle_currentStatus',
        'id',
        'vehicle_occupancyStatus',
        'offset',
        'vehicle_trip_routeId',
        'arrival_time_offset',
        'departure_time_offset'
    ])

    # Perform the left join
    df_final = df_stops_unix.join(
        df_processed,
        how='left',
        left_on=['trip_id', 'stop_sequence'],
        right_on=['trip_id', 'vehicle_currentStopSequence']
    )

    # Drop a double column and rename some.
    df_final = df_final.drop('vehicle_currentStopSequence')
    df_final = df_final.rename({'id': 'vehicleID', 'vehicle_occupancyStatus': 'Current_Occupancy',
                                'vehicle_trip_routeId': 'routeId'})


    df_final.write_parquet(f'data_stops_{file_name}.parquet')
    df_final.write_csv('df_final.csv')

    # Upload the final file back to S3
    output_key = f'{folder_name}/data_stops_{file_name}.parquet'  # Set your output file path here
    upload_file_from_tmp(f'/tmp/data_stops_{file_name}.parquet', output_bucket, output_key)



def download_file_to_tmp(bucket, key, local_file_name):
    """
    Download a file from a S3 bucket and stores it locally
    :param bucket: Bucket name in S3
    :param key: the key of the file to retrieve
    :param local_file_name: the path where to store it locally
    :return: the local path if success, none if failed.
    """
    local_path = local_file_name
    try:
        s3.download_file(Bucket=bucket, Key=key, Filename=local_path)
        return local_path
    except Exception as e:
        print(f'Error downloading file from S3: {e}')
        return False


def upload_file_from_tmp(local_file_name, bucket, key):
    """
    Uploads a file to an S3 bucket from the local file system.
    :param local_file_name: the path of the local file
    :param bucket: Bucket name in S3
    :param key: the key under which to store the file
    :return: True if file was uploaded successfully, False otherwise.
    """
    s3 = boto3.client('s3')
    try:
        if os.path.exists(local_file_name):
            s3.upload_file(Filename=local_file_name, Bucket=bucket, Key=key)
            return True
        else:
            print(f"Error: The file {local_file_name} does not exist.")
            return False
    except Exception as e:
        print(f'Error uploading file to S3: {e}')
        return False


def adding_arrival_time_unix(df_temp, date_obj):
    """
    Create a new column "arrival_time_unix", it uses the existing column "arrival_time" and using the day passed in
    the event convert that time in a UNIX value for the timezone (also given in the event). In case where the value is
    greater than 23:59:59, we increment the date by 1. ex: (event date: 2023-12-01) 25:54:00 -> 1:54:00 AM of 2023-12-02
    and then convert that to a UNIX value
    :param df_temp: Dataframe to apply the addition to
    :return: Dataframe with the modification sorted by trip_id and arrival_time_unix
    """
    df_temp = df_temp.with_columns(df_temp['arrival_time'].str.split_exact(':', 2).struct.
                                         rename_fields(["hours", "minutes", "seconds"])
                                         .alias('time_split')).unnest('time_split')

    # Convert 'hours', 'minutes', and 'seconds' to integers
    df_temp = df_temp.with_columns([
        pl.col("hours").cast(pl.Int32),
        pl.col("minutes").cast(pl.Int32),
        pl.col("seconds").cast(pl.Int32)
    ])

    # Adjust for hours >= 24 and calculate additional days
    df_temp = df_temp.with_columns([
        (pl.col("hours") % 24).alias("adjusted_hours"),
        (pl.col("hours") / 24).floor().cast(pl.Int32).alias("additional_days")
    ])

    # Extract year, month, and day from the localized date_obj
    year, month, day = date_obj.year, date_obj.month, date_obj.day

    # Create DateTime objects and convert to UNIX timestamps
    df_temp = df_temp.with_columns(
        (pl.datetime(year, month, day, "adjusted_hours", "minutes", "seconds", time_zone='America/Montreal') +
         pl.duration(days="additional_days")).alias("datetime")
    )

    # Convert to UNIX timestamp
    df_temp = df_temp.with_columns(
        pl.col("datetime").dt.timestamp('ms').alias("arrival_time_unix")
    )

    return df_temp.select(pl.col('trip_id'), pl.col('arrival_time'), pl.col('stop_id'), pl.col('stop_sequence')
                          , (pl.col('arrival_time_unix') / 1000).cast(pl.Int64)).sort(['trip_id', 'stop_sequence'])


def rename_and_convert_columns(df):
    df_temp = df.rename({'vehicle_trip_tripId': 'trip_id'})
    try:
        df_temp = df_temp.cast({'id': pl.Int32,'timefetch': pl.Int64, 'vehicle_position_bearing': pl.Int32,
                                'vehicle_position_latitude': pl.Float64, 'vehicle_position_longitude': pl.Float64,
                               'vehicle_position_speed': pl.Float64, 'vehicle_timestamp': pl.Int64, 'trip_id': pl.Int64})
    except Exception as e:
        print(f'Error {e} converting columns type of {df}')
        raise
    return df_temp.sort(['trip_id', 'vehicle_currentStopSequence', 'timefetch'])


def filter_daily_vehicle_position(df):
    # Creating a column to identify where the vehicle_currentStopSequence changes
    df_temp = df.with_columns(
        pl.col("vehicle_currentStopSequence").diff().ne(0).alias("stop_sequence_changed")
    )

    # Filtering the DataFrame based on the specified conditions
    filtered_merged_df = df_temp.filter(
        (pl.col("stop_sequence_changed") & (pl.col("vehicle_currentStatus") == "IN_TRANSIT_TO")) |
        (pl.col("vehicle_currentStatus") == "STOPPED_AT")
    )

    return filtered_merged_df.drop('stop_sequence_changed')


def calculate_offset_for_stopped_status(df):
    """
    Calculate the offset for the stop_sequence where the vehicle_status is "stopped_at"
    :param df: Dataframe to calculate the offset on
    :return: Dataframe sorted by trip_id and vehicle_stop
    """
    df = df.with_columns([
        pl.when(pl.col('vehicle_currentStatus') == "STOPPED_AT")
        .then(pl.col('vehicle_timestamp') - pl.col('arrival_time_unix'))
        .otherwise(None)
        .alias('offset')
    ])

    # return the DF sorted by 'trip_id' and 'vehicle_currentStopSequence'
    return df.sort(['trip_id', 'vehicle_currentStopSequence'])


def calculate_offset_for_in_transit_status(df):
    # Use the `shift()` function to get the 'vehicle_timestamp' of the next 'vehicle_currentStopSequence'
    # within each 'trip_id'. We shift by -1 to get the next value.
    df = df.with_columns(
        pl.col('vehicle_timestamp').shift(-1).over('trip_id').alias('next_vehicle_timestamp')
    )

    # Calculate the offset for 'IN_TRANSIT_TO' using the 'next_vehicle_timestamp'
    # If the next row is from a different 'trip_id', we should not calculate the offset, so we also check for this
    df = df.with_columns([
        pl.when(
            (pl.col('vehicle_currentStatus') == 'IN_TRANSIT_TO') &
            (pl.col('trip_id') == pl.col('trip_id').shift(-1))
        ).then(
            pl.col('next_vehicle_timestamp') - pl.col('arrival_time_unix')
        ).otherwise(
            pl.col('offset')  # Keep the existing offset if the condition is not met
        ).alias('offset')
    ])

    return df.sort(['trip_id', 'vehicle_currentStopSequence'])


def calculate_offset_for_last_stop_sequence(df):
    try:
        # Create a column with the 'vehicle_timestamp' of the next row with the same 'vehicle_vehicle_id'
        df = df.with_columns(
            pl.when(pl.col('vehicle_vehicle_id') == pl.col('vehicle_vehicle_id').shift(-1))
            .then(pl.col('vehicle_timestamp').shift(-1))
            .otherwise(pl.lit(None))
            .alias('next_vehicle_timestamp')
        )

        # Calculate the offset using the new 'next_vehicle_timestamp' column
        # Make sure to compare the vehicle IDs to ensure they are the same before using the next timestamp
        df = df.with_columns([
            pl.when(
                (pl.col('vehicle_currentStatus') != 'STOPPED_AT') &
                (pl.col('vehicle_vehicle_id') == pl.col('vehicle_vehicle_id').shift(-1)) &
                (pl.col('vehicle_currentStopSequence') == pl.col('vehicle_currentStopSequence').max().over('trip_id'))
            ).then(
                pl.col('next_vehicle_timestamp') - pl.col('arrival_time_unix')
            ).otherwise(
                pl.col('offset')
            ).alias('offset')
        ])
        return df.sort(['trip_id', 'vehicle_currentStopSequence'])
    except Exception as e:
        print(f'Failed to calculate offset_for_the_last_stop_sequence {df}')
        print(f'Error: {e}')


def calculate_arrival_departure_offset(df):
    """
    Calculate the arrival and departure time offset based on how many value there is for a stop_sequence
    If we have more than one value of offsets for a stop_sequence, we have the information of arrival and departure
    to that stop, we then take the lowest has arrival and highest as departure. If we have 1 value we assign that one
    for both (arrival and departure offset)
    :param df: Dataframe to calculate
    :return: Dataframe with values added
    """
    try:
        # Calculate the first and last offset for each group
        first_offset = df.group_by(['trip_id', 'vehicle_currentStopSequence']).agg(
            pl.first('offset').alias('arrival_time_offset')
        ).with_columns(pl.col('vehicle_currentStopSequence').cast(pl.Int64))  # Cast if necessary

        last_offset = df.group_by(['trip_id', 'vehicle_currentStopSequence']).agg(
            pl.last('offset').alias('departure_time_offset')
        ).with_columns(pl.col('vehicle_currentStopSequence').cast(pl.Int64))  # Cast if necessary

        # Join the first and last offsets back to the original DataFrame
        df = df.join(first_offset, on=['trip_id', 'vehicle_currentStopSequence'], how='left')
        df = df.join(last_offset, on=['trip_id', 'vehicle_currentStopSequence'], how='left')
        return df
    except Exception as e:
        print(f'Failed to calculate arrival_departure_offset of {df}')
        print(f'Error: {e}')
        raise


def process_based_on_daily_static_files(dfs_daily_vehicle_positions_merge, df_stops_unix):
    try:
        # We filter to only keep the positions(rows) we need to process (remove duplicate)
        df_filtered_vehicle_positions = filter_daily_vehicle_position(dfs_daily_vehicle_positions_merge)
        df_filtered_vehicle_positions.write_parquet('df_filtered_vehicle_positions.parquet')  # Used to generate the map

        # We then reduce the number of rows to keep, only the one with value for a stop_sequence
        df_merge = df_filtered_vehicle_positions.join(df_stops_unix, how='outer',
                                                      left_on=['trip_id', 'vehicle_currentStopSequence'],
                                                      right_on=['trip_id', 'stop_sequence'])

        # We remove the rows of data that doesn't have an arrival_time_unix (see the documentation for why that would happen)
        df_merge = df_merge.filter(pl.col('arrival_time_unix').is_not_null())
        df_merge = df_merge.filter(pl.col('vehicle_timestamp').is_not_null())

        # We create a column 'offset' and calculate a value to be able to the remove the vehiclePosition of the next day.
        df_merge = df_merge.with_columns((pl.col('timefetch') - pl.col('arrival_time_unix')).alias('offset'))
        # Filter out rows where the absolute value of the offset is greater than 7200 seconds(2 hours)(trip_id of the next_day)
        df_time_difference = df_merge.filter(pl.col('offset').abs() <= 7200)
        df_time_difference = df_time_difference.with_columns(pl.lit(None).alias('offset'))

        # Calculate offset for "STOPPED_AT" VehicleStatus
        df_time_difference = calculate_offset_for_stopped_status(df_time_difference)

        # Calculate offset for "IN_TRANSIT_TO" VehicleStatus
        df_time_difference = calculate_offset_for_in_transit_status(df_time_difference)

        # Calculate offset for the last stop of a trip
        df_time_difference = calculate_offset_for_last_stop_sequence(df_time_difference)

        # Determine the arrival and departure offset for each stop if possible
        df_time_difference = calculate_arrival_departure_offset(df_time_difference)

        # Remove the stops with an offset of more than 1800 seconds (30 minutes)
        df_time_difference = df_time_difference.filter(pl.col('offset').abs() <= 1800)

        return df_time_difference.sort(['trip_id', 'vehicle_currentStopSequence', 'timefetch'])

    except Exception as e:
        print(f'Failed to process {dfs_daily_vehicle_positions_merge} and {df_stops_unix} for daily static file')
        print(f'Error: {e}')
        raise