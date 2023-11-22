import boto3
import gzip


s3 = boto3.client('s3')


def lambda_handler(event, context):
    print('allo')