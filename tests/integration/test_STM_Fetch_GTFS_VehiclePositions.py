import unittest
from unittest.mock import patch, MagicMock
from STM_Services.STM_Fetch_GTFS_VehiclePositions.main import lambda_handler
from google.transit import gtfs_realtime_pb2
from datetime import datetime 

class TestLambdaHandler(unittest.TestCase):

    @patch('STM_Services.STM_Fetch_GTFS_VehiclePositions.main.os.environ.get')
    @patch('STM_Services.STM_Fetch_GTFS_VehiclePositions.main.urlopen')
    @patch('STM_Services.STM_Fetch_GTFS_VehiclePositions.main.boto3.client')
    @patch('STM_Services.STM_Fetch_GTFS_VehiclePositions.main.os.remove')
    @patch('STM_Services.STM_Fetch_GTFS_VehiclePositions.main.pd.DataFrame.to_parquet')
    def test_lambda_handler(self, mock_to_parquet, mock_remove, mock_boto3_client, mock_urlopen, mock_get_env):
        mock_get_env.side_effect = lambda k: {'API_URL_STM_VEHICLE': 'https://api_url', 'API_KEY_STM': 'api_key'}.get(k)
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = '2.0'
        feed.header.incrementality = feed.header.DIFFERENTIAL
        feed.header.timestamp = int(datetime.now().timestamp())

        mock_response = MagicMock()
        mock_response.read.return_value = feed.SerializeToString()
        mock_urlopen.return_value = mock_response

        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        lambda_handler({'bucket_name': 'test-bucket'}, None)

        # Check if everything has been called correctly
        mock_urlopen.assert_called()  
        mock_to_parquet.assert_called_once()
        mock_remove.assert_called_once()

if __name__ == '__main__':
    unittest.main()
