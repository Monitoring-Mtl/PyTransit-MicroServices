import pandas as pd
import boto3
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):
    #Init S3 client
    s3 = boto3.client('s3')

    #Define the bucket and folder names
    input_bucket = 'pfe014-stm-data-static'
    output_bucket = 'pfe014-stm-data-static-daily'
    calendar_folder = 'calendar/'
    trips_folder = 'trips/'
    stop_times_folder = 'stop_times/'

    #Load calendar.csv from S3
    calendar_key = calendar_folder + 'calendar.csv'
    response = s3.get_object(Bucket=input_bucket, Key=calendar_key)
    calendar_df = pd.read_csv(response['Body'])

    
