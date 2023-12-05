import requests
import os
import zipfile
import boto3
from io import BytesIO
import polars as pl
import datetime

s3 = boto3.client('s3')

LAST_MODIFIED_KEY = "Last_modified.txt"


def lambda_handler(event, context):
    bucket_name = event['bucket_name']
    url = event['url']

    # Step 1: On récupère la valeur du header du ZIP pour les dernières modifications
    response = requests.head(url)
    last_modified = response.headers.get('Last-Modified')
    last_modified_date = datetime.datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT')

    # Step 2: On valide la dernière modification dans le bucket S3 si elle existe
    try:
        last_modified_file = s3.get_object(Bucket=bucket_name, Key=LAST_MODIFIED_KEY)
        last_modified_in_s3 = last_modified_file['Body'].read().decode('utf-8')
        last_modified_in_s3_date = datetime.datetime.strptime(last_modified_in_s3, '%a, %d %b %Y %H:%M:%S GMT')

        # Step 3: On compare les deux dates si le fichier existe dans le bucket S3
        if last_modified_date <= last_modified_in_s3_date:
            print("Files are up to date.")
            return

    except s3.exceptions.NoSuchKey:
        print("Last_modified.txt does not exist. Processing new file.")

    # Step 4: Si le fichier "Last_modified.txt" n'existe pas on télécharge les fichiers et upload dans S3
    # Step 4: Download ZIP to /tmp directory
    response = requests.get(url, stream=True)
    zip_tmp_path = '/tmp/tempfile.zip'
    with open(zip_tmp_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=128):
            f.write(chunk)

    # Process files from /tmp
    with zipfile.ZipFile(zip_tmp_path) as z:
        for filename in z.namelist():
            if filename.endswith('.txt'):
                # Extract file to /tmp
                z.extract(filename, '/tmp')
                txt_file_path = os.path.join('/tmp', filename)

                # Read content into Polars DataFrame
                df = pl.read_csv(txt_file_path)

                # Convert DataFrame to Parquet
                parquet_buffer = BytesIO()
                df.write_parquet(parquet_buffer)

                # Define key for S3 (change file extension to .parquet)
                parquet_filename = filename.replace('.txt', '.parquet')
                folder_name = filename.replace('.txt', '')
                parquet_key = f'{folder_name}/{parquet_filename}'

                # Upload Parquet file to S3
                s3.put_object(Bucket=bucket_name, Key=parquet_key, Body=parquet_buffer.getvalue())

    # Clean up /tmp
    os.remove(zip_tmp_path)
    for filename in z.namelist():
        os.remove(os.path.join('/tmp', filename))

    # Step 5: Mettre à jour le fichier "Last_modified.txt" avec la nouvelle date. 
    s3.put_object(Bucket=bucket_name, Key=LAST_MODIFIED_KEY, Body=last_modified)