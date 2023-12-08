import pandas
import polars as pl
import boto3
from datetime import datetime, timedelta
import time
import pytz
import os
import io


def download_file_to_tmp(bucket, key, local_file_name):
    local_path = local_file_name
    try:
        s3.download_file(Bucket=bucket, Key=key, Filename=local_path)
        return local_path
    except Exception as e:
        print(f'Error downloading file from S3: {e}')
        return None


def adding_arrival_time_unix(df_temp):
    df_temp = df_stop_times.with_columns(df_stop_times['arrival_time'].str.split_exact(':', 2).struct.
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
                          , (pl.col('arrival_time_unix') / 1000).cast(pl.Int64)).sort(['trip_id', 'arrival_time_unix'])


def rename_and_convert_columns(df):
    df_temp = df.rename({'vehicle_trip_tripId': 'trip_id'})
    try:
        df_temp = df_temp.cast({'id': pl.Int32,'timefetch': pl.Int64, 'vehicle_position_bearing': pl.Int32,
                                'vehicle_position_latitude': pl.Float64, 'vehicle_position_longitude': pl.Float64,
                               'vehicle_position_speed': pl.Float64, 'vehicle_timestamp': pl.Int64, 'trip_id': pl.Int64})
    except Exception as e:
        print(f'Error {e} converting columns type of {df}')
        raise
    return df_temp.sort(['trip_id', 'timefetch'])


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

    return filtered_merged_df.drop("stop_sequence_changed")

def calculate_offset_vehicleid_occupancy():
    return None


start_time = time.time()

s3 = boto3.client('s3')

timezone_str = 'America/Montreal'
eastern = pytz.timezone(timezone_str)

# Extract the date from the event, or use the current date in the specified timezone (Format YYYYMMDD)
date_str = '20231019'

# Parse the date string into a datetime object
date_obj = datetime.strptime(date_str, '%Y%m%d')
date_obj = eastern.localize(date_obj)

TIME_THRESHOLD = 18000  # 5 hours in seconds

next_day = date_obj + timedelta(days=1)

bucket_static_daily = 'pfe014-stm-data-static-daily'
bucket_name = 'pfe014-stm-gtfs-vehicleposition-daily'
folder_name = date_obj.strftime('%Y/%m/%d')
folder_name_next_day = next_day.strftime('%Y/%m/%d')

file_name = date_obj.strftime('%Y-%m-%d')
file_name_next_day = next_day.strftime('%Y-%m-%d')

local_static_file_name = (f'C:/Users/bou52/PycharmProjects/pythonProject2/'
                          f'tmp_file/filtered_stop_times_{file_name}.parquet')

local_file_name = f'C:/Users/bou52/PycharmProjects/pythonProject2/tmp_file/Daily_merge_{file_name}.parquet'
local_file_name_next_day = (f'C:/Users/bou52/PycharmProjects/pythonProject2/tmp_file/'
                            f'Daily_merge_{file_name_next_day}.parquet')


key_static = f'{folder_name}/filtered_stop_times/filtered_stop_times_{file_name}.parquet'
key = f'{folder_name}/Daily_GTFS_VehiclePosition_{file_name}.parquet'
key_next_day = f'{folder_name_next_day}/Daily_GTFS_VehiclePosition_{file_name_next_day}.parquet'

local_path_static = download_file_to_tmp(bucket_static_daily, key_static, local_static_file_name)
local_day_path = download_file_to_tmp(bucket_name, key, local_file_name)
local_next_day_path = download_file_to_tmp(bucket_name, key_next_day, local_file_name_next_day)

df = pl.read_parquet(local_day_path)
df_next_day = pl.read_parquet(local_next_day_path)
df_stop_times = pl.read_parquet(local_path_static)

time_to_fetch = time.time()

df_stop_times = df_stop_times.drop({'departure_time'})
# We create the new column 'arrival_time_unix' converting the time in UNIX.
df_stops_unix = adding_arrival_time_unix(df_stop_times)

# Merge the two DFs (current_day + next_day) of VehiclePositions
dfs_daily_vehicle_positions_merge = pl.concat([df, df_next_day], rechunk=True)

# We rename a column and convert the type of others
dfs_daily_vehicle_positions_merge = rename_and_convert_columns(dfs_daily_vehicle_positions_merge)
dfs_daily_vehicle_positions_merge.write_parquet('dfs_daily_vehicle_positions_merge.parquet') #For the MAP


# We filter to only keep the positions(rows) we need to evaluate the offset, occupancy and wheelchair info
df_filtered_vehicle_positions = filter_daily_vehicle_position(dfs_daily_vehicle_positions_merge)
df_filtered_vehicle_positions.write_parquet('df_filtered_vehicle_positions.parquet') #For the MAP


# We create the new column 'arrival_time_unix' converting the time in UNIX.
df_stops_unix = adding_arrival_time_unix(df_stop_times)

# We then select reduce the number of rows to keep only one value for a stop_sequence
df_merge = df_filtered_vehicle_positions.join(df_stops_unix, how='outer',
                                              left_on=['trip_id', 'vehicle_currentStopSequence'],
                                              right_on=['trip_id', 'stop_sequence'])
# We remove the rows of data that doesn't have an arrival_time_unix (see the documentation for why that would happen)
df_merge = df_merge.filter(pl.col('arrival_time_unix').is_not_null())

print(df_merge.columns)
print(df_merge.dtypes)
print(df_merge.head(5))

df_merge.write_csv('merge_csv.csv')


print(f'Duration for Download:{time_to_fetch - start_time}')
print(f'Duration to process:{time.time() - time_to_fetch}')
print(f'Duration total:{time.time() - start_time}')



#df_stops_unix.write_csv(f'stop_times_{file_name}.csv')
#dfs.write_csv('merged_dfs.csv')