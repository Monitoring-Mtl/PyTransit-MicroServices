import boto3
import requests
import os
import datetime
import zipfile
import csv
from io import StringIO, BytesIO

s3 = boto3.client('s3')
bucket_name = 'pfe014-stm-data-static'
url = "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"
last_modified_key = "Last_modified.txt"


def lambda_handler(event, context):
    # Step 1: On récupère la valeur du header du ZIP pour les dernières modifications
    response = requests.head(url)
    last_modified = response.headers.get('Last-Modified')
    last_modified_date = datetime.datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT')

    # Step 2: On valide la dernière modification dans le bucket S3 si elle existe
    try:
        last_modified_file = s3.get_object(Bucket=bucket_name, Key=last_modified_key)
        last_modified_in_s3 = last_modified_file['Body'].read().decode('utf-8')
        last_modified_in_s3_date = datetime.datetime.strptime(last_modified_in_s3, '%a, %d %b %Y %H:%M:%S GMT')

        # Step 3: On compare les deux dates si le fichier existe dans le bucket S3
        if last_modified_date <= last_modified_in_s3_date:
            print("Files are up to date.")
            return

    except s3.exceptions.NoSuchKey:
        print("Last_modified.txt does not exist. Processing new file.")

    # Step 4: Si le fichier "Last_modified.txt" n'existe pas on télécharge les fichiers et upload dans S3
    zip_file_request = requests.get(url)
    zip_file_content = BytesIO(zip_file_request.content)
    with zipfile.ZipFile(zip_file_content) as z:
        for filename in z.namelist():
            if filename.endswith('.txt'):
                # On lit le CSV en Utf-8 et converti en csv. 
                with z.open(filename) as f:
                    txt_content = f.read().decode('utf-8')

                    # Modifie l'extension pour CSV (le fichier est déjà en format interne CSV)
                    csv_filename = filename.replace('.txt', '.csv')
                    folder_name = filename.replace('.txt', '')

                    # On sauvegarde le fichier dans un répertoire du même nom
                    csv_key = f'{folder_name}/{csv_filename}'
                    s3.put_object(Bucket=bucket_name, Key=csv_key, Body=txt_content)

    # Step 5: Mettre à jour le fichier "Last_modified.txt" avec la nouvelle date. 
    s3.put_object(Bucket=bucket_name, Key=last_modified_key, Body=last_modified)


# Entry point for the Lambda function execution
def main():
    lambda_handler(None, None)


if __name__ == "__main__":
    main()
