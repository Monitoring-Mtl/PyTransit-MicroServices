import unittest
from unittest.mock import patch, MagicMock
from STM_Services.STM_Fetch_GTFS_TripUpdates.main import lambda_handler  # Adjust this import according to your module's structure

class TestLambdaHandler(unittest.TestCase):

    @patch('STM_Services.STM_Fetch_GTFS_TripUpdates.main.os.environ.get')
    @patch('STM_Services.STM_Fetch_GTFS_TripUpdates.main.urlopen')
    @patch('STM_Services.STM_Fetch_GTFS_TripUpdates.main.s3')
    @patch('STM_Services.STM_Fetch_GTFS_TripUpdates.main.os.remove')
    @patch('STM_Services.STM_Fetch_GTFS_TripUpdates.main.pd.DataFrame.to_parquet')
    def test_lambda_handler(self, mock_to_parquet, mock_remove, mock_s3, mock_urlopen, mock_get_env):
        mock_get_env.side_effect = lambda k: {'API_URL_STM_TRIP': 'https://api_url', 'API_KEY_STM': 'api_key'}.get(k)

        mock_response = MagicMock()
        mock_response.read.return_value = b'' 
        mock_urlopen.return_value = mock_response

        mock_s3.upload_file.return_value = None

        event = {
            'bucket_name': 'test-bucket',
            'collection_name': 'test-collection'
        }

        lambda_handler(event, None)

        #Check if everything has been called
        mock_urlopen.assert_called()  
        mock_to_parquet.assert_called_once()  
        mock_s3.upload_file.assert_called_once()  
        mock_remove.assert_called_once() 

if __name__ == '__main__':
    unittest.main()
