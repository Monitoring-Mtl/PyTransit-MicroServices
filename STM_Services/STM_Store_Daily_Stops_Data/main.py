import boto3
import gzip


s3 = boto3.client('s3')


def lambda_handler(event, context):
    analysis_bucket_name = event['analysis_bucket_name']
    
    print('allo')