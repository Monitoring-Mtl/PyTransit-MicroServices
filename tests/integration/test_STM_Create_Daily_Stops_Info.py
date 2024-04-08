from datetime import datetime, timezone
import unittest
from unittest.mock import patch, MagicMock
import polars as pl
import pytz
import tempfile
import os
from STM_Services.STM_Create_Daily_Stops_Info.main import lambda_handler, download_file_to_tmp, upload_file_from_tmp, read_parquet_from_tmp, write_df_to_parquet_to_tmp, convert_to_unix

class TestS3DataProcessing(unittest.TestCase):
    
    @patch('STM_Services.STM_Create_Daily_Stops_Info.main.upload_file_from_tmp')
    @patch('STM_Services.STM_Create_Daily_Stops_Info.main.download_file_to_tmp')
    @patch('STM_Services.STM_Create_Daily_Stops_Info.main.pl.read_parquet')
    @patch('STM_Services.STM_Create_Daily_Stops_Info.main.pl.DataFrame.write_parquet')
    def test_lambda_handler_success(self, mock_write_parquet, mock_read_parquet, mock_download, mock_upload):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file_1, \
             tempfile.NamedTemporaryFile(delete=False) as temp_file_2, \
             tempfile.NamedTemporaryFile(delete=False) as temp_file_3, \
             tempfile.NamedTemporaryFile(delete=False) as temp_file_4:
            mock_download.side_effect = [temp_file_1.name, temp_file_2.name, temp_file_3.name, temp_file_4.name]
        
        # Mocking read_parquet to return dataframes
        mock_stops_df = pl.DataFrame({'stop_id': ['1'], 'stop_name': ['Stop A'], 'stop_lat': [40.7128], 'stop_lon': [-74.0060], 'wheelchair_boarding': [1]})
        mock_trips_df = pl.DataFrame({ 'trip_id': ['1'],'route_id': ['101'],'trip_headsign': ['North'], 'direction_id': [0],'shape_id': ['1'],'wheelchair_accessible': [1]})
        mock_stop_times_df = pl.DataFrame({'trip_id': ['1'], 'arrival_time': ['12:00:00'], 'stop_id': ['1']})
        mock_routes_df = pl.DataFrame({'route_id': ['101'], 'route_long_name': ['Route 101']})
        
        mock_read_parquet.side_effect = [mock_stops_df, mock_trips_df, mock_stop_times_df, mock_routes_df]
        mock_write_parquet.return_value = None

        event = {
            'static_bucket': 'my-static-bucket',
            'daily_static_bucket': 'my-daily-static-bucket',
            'output_bucket': 'my-output-bucket',
            'timezone': 'America/Montreal',
            'date': '20230401',
        }

        lambda_handler(event, None)

        # Assertions to ensure the mocked functions were called as expected
        mock_download.assert_called()
        mock_upload.assert_called()
        mock_write_parquet.assert_called_once()

    @patch('STM_Services.STM_Create_Daily_Stops_Info.main.s3_client.download_file')
    def test_download_file_to_tmp(self, mock_download_file):
        mock_download_file.return_value = None
        bucket = 'test-bucket'
        key = 'stops/stops.parquet'
        expected_local_path = f"/tmp/{os.path.basename(key)}"

        result = download_file_to_tmp(bucket, key)
        self.assertEqual(result, expected_local_path)
        mock_download_file.assert_called_with(Bucket=bucket, Key=key, Filename=expected_local_path)
    
    @patch('STM_Services.STM_Create_Daily_Stops_Info.main.s3_client.upload_file')
    def test_upload_file_from_tmp(self, mock_upload_file):
        mock_upload_file.return_value = None
        bucket = 'test-bucket'
        key = 'processed/data.parquet'
        local_path = '/tmp/data.parquet'

        upload_file_from_tmp(bucket, key, local_path)
        mock_upload_file.assert_called_with(Filename=local_path, Bucket=bucket, Key=key)
    
    def test_convert_to_unix(self):
        time_str = '24:00:00'
        base_date = datetime(2023, 1, 1, tzinfo=pytz.timezone('America/Montreal'))
        timezone_str = 'America/Montreal'
        result = convert_to_unix(time_str, base_date, timezone_str)
        
        expected = int(pytz.timezone(timezone_str).localize(datetime(2023, 1, 2)).timestamp())
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
