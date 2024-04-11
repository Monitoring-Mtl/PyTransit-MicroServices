from datetime import datetime, timezone
import unittest
import polars as pl
import pytz
from unittest.mock import patch
from STM_Services.STM_Analyse_Daily_Stops_Data.main import download_file_to_tmp, upload_file_from_tmp, lambda_handler, adding_arrival_time_unix

class TestLambdaFunction(unittest.TestCase):
    
    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.upload_file_from_tmp', return_value=True)
    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.download_file_to_tmp')
    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.pl.read_parquet')
    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.pl.DataFrame.write_parquet')
    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.clean_tmp_folder')
    def test_lambda_handler_success(self, mock_clean, mock_write_parquet, mock_read_parquet, mock_download, mock_upload):
        
        timestamp1 = int(datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())
        timestamp2 = int(datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc).timestamp())

        mock_df_daily_merge = pl.DataFrame({
            'id': [1, 2],
            'timefetch': [timestamp1, timestamp2],
            'vehicle_position_bearing': [10, 20],
            'vehicle_trip_tripId': [1, 2],
            'vehicle_position_longitude': [-10, -20],
            'vehicle_position_latitude': [10, 20],
            'vehicle_timestamp': [timestamp1, timestamp2],
            'vehicle_position_speed': [100, 200],
            'vehicle_currentStopSequence': [2, 1],
            'vehicle_currentStatus': ['STOPPED_AT', 'STOPPED_AT'],
            'vehicle_vehicle_id': [1, 2],
            'vehicle_occupancyStatus': ['MANY_SEATS_AVAIBLE', 'MANY_SEATS_AVAIBLE'],
            'vehicle_trip_routeId': [1, 2]
        })

        timestamp3 = int(datetime(2023, 1, 2, 12, 0, 0, tzinfo=timezone.utc).timestamp())
        timestamp4 = int(datetime(2023, 1, 2, 13, 0, 0, tzinfo=timezone.utc).timestamp())

        mock_df_next_day = pl.DataFrame({
            'id': [3, 4],
            'timefetch': [timestamp3, timestamp4],
            'vehicle_position_bearing': [30, 40],
            'vehicle_trip_tripId': [3, 4],
            'vehicle_position_longitude': [-30, -40],
            'vehicle_position_latitude': [10, 20],
            'vehicle_timestamp': [timestamp3, timestamp4],
            'vehicle_position_speed': [300, 400],
            'vehicle_currentStopSequence': [4, 3] ,
            'vehicle_currentStatus': ['STOPPED_AT', 'STOPPED_AT'],
            'vehicle_vehicle_id': [3, 4],
            'vehicle_occupancyStatus': ['MANY_SEATS_AVAIBLE', 'MANY_SEATS_AVAIBLE'],
            'vehicle_trip_routeId': [3, 4] 
        })

        mock_df_static_file = pl.DataFrame({
            'arrival_time': ['23:59:59', '24:30:00'],
            'departure_time': ['23:59:59', '24:30:00'],
            'trip_id': [1, 2],  
            'stop_id': [101, 102],
            'stop_sequence': [1, 2]
        })

        mock_read_parquet.side_effect = [mock_df_daily_merge, mock_df_next_day, mock_df_static_file]
        mock_write_parquet.return_value = None

        event = {
            'daily_static_bucket': 'test-bucket',
            'bucket_vehicle_positions_daily_merge': 'vehicle-positions-bucket',
            'output_bucket': 'output-bucket',
            'timezone': 'America/Montreal',
            'date': '20230101',
        }

        lambda_handler(event, None)

        # Assertions to ensure the mocked functions were called as expected
        mock_download.assert_called()
        mock_upload.assert_called_with('/tmp/data_stops_2022-12-30.parquet', 'output-bucket', '2022/12/30/data_stops_2022-12-30.parquet')
        mock_clean.assert_called()
        mock_read_parquet.assert_called()
        mock_write_parquet.assert_called_once()
        call_args, call_kwargs = mock_write_parquet.call_args
        expected_path = f'/tmp/data_stops_2022-12-30.parquet'
        self.assertIn(expected_path, call_args[0])


    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.boto3.client')
    def test_download_file_to_tmp_success(self, mock_boto3_client):
        mock_s3_client = mock_boto3_client.return_value

        bucket = 'test-bucket'
        key = 'test-key'
        local_file_name = '/tmp/test-file.parquet'

        result = download_file_to_tmp(bucket, key, local_file_name)
        self.assertEqual(result, local_file_name)

        # Verify download_file was called with the correct parameters
        mock_s3_client.download_file.assert_called_once_with(Bucket=bucket, Key=key, Filename=local_file_name)
    
    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.boto3.client')
    def test_download_file_to_tmp_failure(self, mock_boto3_client):
        mock_s3 = mock_boto3_client.return_value
        mock_s3.download_file.side_effect = Exception("Download failure")
        
        result = download_file_to_tmp('test-bucket', 'test-key', '/tmp/local_file.parquet')
        self.assertFalse(result)
        
        # Verify download_file was attempted with the correct parameters
        mock_s3.download_file.assert_called_once_with(Bucket='test-bucket', Key='test-key', Filename='/tmp/local_file.parquet')
    
    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.boto3.client')
    def test_upload_file_from_tmp_success(self, mock_boto3_client):
        mock_s3_client = mock_boto3_client.return_value

        with patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.os.path.exists', return_value=True):
            result = upload_file_from_tmp('/tmp/test-file.parquet', 'test-bucket', 'test-key')
            self.assertTrue(result) 

        # Verify upload_file was called with the correct parameters in the correct order
        mock_s3_client.upload_file.assert_called_once_with(Filename='/tmp/test-file.parquet', Bucket='test-bucket', Key='test-key')
    
    @patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.boto3.client')
    def test_upload_file_from_tmp_failure_upload(self, mock_boto3_client):
        mock_s3 = mock_boto3_client.return_value
        mock_s3.upload_file.side_effect = Exception("Upload failure")

        with patch('STM_Services.STM_Analyse_Daily_Stops_Data.main.os.path.exists', return_value=True):
            result = upload_file_from_tmp('/tmp/local_file.parquet', 'test-bucket', 'test-key')
            self.assertFalse(result) 
        
        # Verify upload_file was called with the correct parameters
        mock_s3.upload_file.assert_called_once_with(Filename='/tmp/local_file.parquet', Bucket='test-bucket', Key='test-key')
    
    def test_adding_arrival_time_unix(self):
        df = pl.DataFrame({
            'arrival_time': ['23:59:59', '24:30:00'],
            'trip_id': [1, 2],
            'stop_id': [101, 102],
            'stop_sequence': [1, 2]
        })
        date_obj = datetime(2023, 12, 1, tzinfo=pytz.timezone('America/Montreal'))
        
        # Expected UNIX timestamps
        expected_unix_timestamps = [1701493199, 1701495000]

        result_df = adding_arrival_time_unix(df, date_obj)
        actual_unix_timestamps = result_df['arrival_time_unix'].to_list()

        # Verify result
        self.assertEqual(actual_unix_timestamps, expected_unix_timestamps)

if __name__ == '__main__':
    unittest.main()
