import json
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from BIXI_Services.BIXI_Fetch_GBFS_Station_Status.main import lambda_handler

class TestLambdaFunction(unittest.TestCase):

    @patch('BIXI_Services.BIXI_Fetch_GBFS_Station_Status.main.pd.DataFrame')
    @patch('BIXI_Services.BIXI_Fetch_GBFS_Station_Status.main.boto3.client')
    @patch('BIXI_Services.BIXI_Fetch_GBFS_Station_Status.main.urlopen')
    def test_success_path(self, mock_urlopen, mock_boto3, mock_dataframe):
        response_data = json.dumps({
            "data": {"stations": [{"id": "1", "name": "Test Station"}]}
        }).encode('utf-8')
        
        mock_response = MagicMock()
        mock_response.read.return_value = response_data
        mock_urlopen.return_value = mock_response

        mock_s3_client = MagicMock()
        mock_boto3.return_value = mock_s3_client

        with tempfile.TemporaryDirectory() as tmpdirname:
            temp_file_path = os.path.join(tmpdirname, 'file.parquet')
            
            lambda_handler({
                'bucket_name': 'test-bucket', 
                'url': 'http://test.url', 
                'file_path': temp_file_path
            }, None)

        # Verify that urlopen was called with the expected URL
        assert mock_urlopen.call_args[0][0].get_full_url() == 'http://test.url'

        # Verify DataFrame was created with expected data
        mock_dataframe.assert_called_once_with([{"id": "1", "name": "Test Station"}])
    
    @patch('BIXI_Services.BIXI_Fetch_GBFS_Station_Status.main.pd.DataFrame')
    @patch('BIXI_Services.BIXI_Fetch_GBFS_Station_Status.main.boto3.client')
    @patch('BIXI_Services.BIXI_Fetch_GBFS_Station_Status.main.urlopen')
    def test_data_fetch_failure(self, mock_urlopen, mock_boto3, mock_dataframe):
        mock_urlopen.side_effect = Exception("Simulated fetch failure")

        with tempfile.TemporaryDirectory() as tmpdirname:
            temp_file_path = os.path.join(tmpdirname, 'file.parquet')

            with self.assertRaises(Exception) as context:
                lambda_handler({
                    'bucket_name': 'test-bucket', 
                    'url': 'http://test.url', 
                    'file_path': temp_file_path
                }, None)

            self.assertTrue("Simulated fetch failure" in str(context.exception))
        
        # Verify that the DataFrame creation and S3 upload were not attempted due to the fetch failure
        mock_dataframe.assert_not_called()
        mock_boto3.upload_file.assert_not_called()

if __name__ == '__main__':
    unittest.main()
