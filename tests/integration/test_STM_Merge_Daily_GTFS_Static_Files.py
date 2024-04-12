import unittest
import polars as pl
from unittest.mock import patch, MagicMock

from STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main import download_from_s3, upload_to_s3, process_file, process_files, lambda_handler

class TestS3DataProcessing(unittest.TestCase):
    
    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.boto3.client')
    def test_download_from_s3_success(self, mock_boto3_client):
        mock_s3 = mock_boto3_client.return_value
        mock_response = {'Body': MagicMock(read=MagicMock(return_value=b'content'))}
        mock_s3.get_object.return_value = mock_response

        expected_df = pl.DataFrame({'test': [1, 2, 3]})

        with patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.pl.read_parquet', return_value=expected_df) as mock_read_parquet:
            df = download_from_s3('bucket-name', 'file-key')

            mock_read_parquet.assert_called_once()
            self.assertEqual(df.frame_equal(expected_df), True)

    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.boto3.client')
    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.os.remove')
    def test_upload_to_s3_success(self, mock_remove, mock_boto3_client):
        mock_s3 = mock_boto3_client.return_value

        df_to_upload = pl.DataFrame({'test': [1, 2, 3]})

        with patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.pl.DataFrame.write_parquet', return_value=None) as mock_write_parquet:
            upload_to_s3('bucket-name', 'file-key', df_to_upload)

            # Verify write_parquet and upload_file are called as expected
            mock_write_parquet.assert_called_once()
            mock_s3.upload_file.assert_called_once_with('/tmp/file.parquet', 'bucket-name', 'file-key')
            mock_remove.assert_called_once_with('/tmp/file.parquet')
    
    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.download_from_s3')
    def test_process_file_success(self, mock_download):
        mock_df = pl.DataFrame({'a': [1], 'b': [2]})
        mock_download.return_value = mock_df

        file_key = '20200101_123456.parquet'
        all_columns = ['a', 'b', 'c', 'timefetch']

        processed_df = process_file(file_key, all_columns, 'bucket-name')

        expected_columns = sorted(all_columns)
        self.assertEqual(processed_df.columns, expected_columns)
        self.assertIn('c', processed_df.columns)  
        self.assertIn('timefetch', processed_df.columns) 

    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.process_file')
    def test_process_files_success(self, mock_process_file):
        mock_df = pl.DataFrame({'a': [1], 'b': [2], 'c': [None], 'timefetch': [123456789]})
        mock_process_file.return_value = mock_df

        file_keys = ['20200101_123456.parquet', '20200102_123456.parquet']
        all_columns = ['a', 'b', 'c', 'timefetch']

        processed_dfs = process_files(file_keys, all_columns, 'bucket-name', 2)  

        self.assertEqual(len(processed_dfs), 2) 
        for df in processed_dfs:
            self.assertEqual(df.columns, sorted(all_columns)) 

    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.upload_to_s3')
    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.process_files')
    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.download_from_s3')
    @patch('STM_Services.STM_Merge_Daily_GTFS_VehiclePositions.main.boto3.client')
    def test_lambda_handler_success(self, mock_boto3, mock_download, mock_process_files, mock_upload):
        mock_s3_client = MagicMock()
        mock_boto3.return_value = mock_s3_client
        mock_s3_client.get_paginator.return_value.paginate.return_value = [{
            'Contents': [{'Key': '20200101_123456.parquet'}]
        }]

        mock_download.return_value = pl.DataFrame({'a': [1], 'b': [2]})
        mock_process_files.return_value = [pl.DataFrame({'a': [1], 'b': [2], 'c': [None], 'timefetch': [123456789]})]
        mock_upload.return_value = None

        event = {
            'input_bucket': 'input-bucket',
            'output_bucket': 'output-bucket',
            'timezone': 'UTC',
            'date': '20200101',
        }

        response = lambda_handler(event, None)

        self.assertEqual(response['statusCode'], 200)
        self.assertIn('Data processing and upload completed successfully.', response['body'])

        # Verify mock calls
        mock_boto3.assert_called_with('s3')
        mock_download.assert_called()
        mock_process_files.assert_called()
        mock_upload.assert_called()

if __name__ == '__main__':
    unittest.main()
