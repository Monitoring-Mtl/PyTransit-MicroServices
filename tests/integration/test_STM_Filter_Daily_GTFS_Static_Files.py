from unittest import TestCase, mock
from unittest.mock import patch
import polars as pl
import pytz
from datetime import datetime
from STM_Services.STM_Filter_Daily_GTFS_Static_files.main import lambda_handler

class TestLambdaHandler(TestCase):
    @patch('STM_Services.STM_Filter_Daily_GTFS_Static_files.main.boto3.client')
    @patch('STM_Services.STM_Filter_Daily_GTFS_Static_files.main.os.remove')
    @patch('STM_Services.STM_Filter_Daily_GTFS_Static_files.main.pl.read_parquet')
    @patch('STM_Services.STM_Filter_Daily_GTFS_Static_files.main.datetime', wraps=datetime)
    @patch('polars.DataFrame.write_parquet')  
    def test_lambda_handler(self, mock_write_parquet, mock_datetime, mock_read_parquet, mock_os_remove, mock_boto3_client):    
        mock_s3 = mock.MagicMock()
        mock_boto3_client.return_value = mock_s3

        eastern = pytz.timezone('America/Montreal')
        test_date = datetime(2023, 1, 1, tzinfo=eastern)
        mock_datetime.now.return_value = test_date
        mock_datetime.strptime.return_value = test_date

        calendar_columns = ['start_date', 'end_date', 'service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        calendar_data  = [['20230101', '20230102', 1, 0, 0, 0, 0, 0, 0, 1]]
        expected_calendar_df = pl.DataFrame(calendar_data, schema=calendar_columns)
        
        trips_columns = ['service_id', 'trip_id']
        trips_data = [[1, 'trip_123']]
        expected_trips_df = pl.DataFrame(trips_data, schema=trips_columns)

        stop_times_columns = ['trip_id', 'stop_id', 'arrival_time']
        stop_times_data = [['trip_123', 'stop_1', '08:00:00']]
        expected_stop_times_df = pl.DataFrame(stop_times_data, schema=stop_times_columns)
        mock_read_parquet.side_effect = [expected_calendar_df, expected_trips_df, expected_stop_times_df]

        mock_write_parquet.return_value = None

        event = {
            'input_bucket': 'input-bucket',
            'output_bucket': 'output-bucket',
            'date': '20230101',
            'timezone': 'America/Montreal'
        }
        context = {}

        lambda_handler(event, context)

        mock_boto3_client.assert_called_with('s3')
        mock_read_parquet.assert_called()
        mock_write_parquet.assert_called()

if __name__ == '__main__':
    TestCase.main()
