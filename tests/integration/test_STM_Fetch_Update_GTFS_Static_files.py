from unittest import TestCase, mock
from unittest.mock import MagicMock, patch
from io import BytesIO
import datetime

from STM_Services.STM_Fetch_Update_GTFS_Static_files.main import lambda_handler

class TestLambdaHandler(TestCase):
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.requests.get')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.requests.head')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.boto3.client')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.zipfile.ZipFile')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.os.remove')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.pl.read_csv')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.datetime.datetime', wraps=datetime.datetime)
    def test_files_are_the_same(self, mock_datetime, mock_read_csv, mock_remove, mock_zipfile, mock_boto3_client, mock_requests_head, mock_requests_get):    
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.get_object.side_effect = None

        mock_response_head = MagicMock()
        mock_response_head.headers = {'Last-Modified': 'Wed, 21 Oct 2020 07:28:00 GMT'}
        mock_requests_head.return_value = mock_response_head

        mock_response_get = MagicMock()
        mock_response_get.iter_content.return_value = [b'data']
        mock_requests_get.return_value = mock_response_get

        mock_datetime.strptime.return_value = datetime.datetime(2020, 10, 21, 7, 28)

        # Simulate the zipfile and its contents
        mock_zipfile_instance = MagicMock()
        mock_zipfile.return_value = mock_zipfile_instance
        mock_zipfile_instance.__enter__.return_value.namelist.return_value = ['test.txt']

        df_mock = MagicMock()
        mock_read_csv.return_value = df_mock

        event = {'bucket_name': 'test-bucket', 'url': 'http://example.com/data.zip'}
        lambda_handler(event, None)

        # Assertions
        mock_requests_head.assert_called_once_with('http://example.com/data.zip')
        mock_boto3_client.assert_called_with('s3')
        mock_datetime.strptime.assert_called()  
    
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.zipfile.ZipFile')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.open', new_callable=mock.mock_open, create=True)
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.requests.get')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.requests.head')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.boto3.client')
    @patch('STM_Services.STM_Fetch_Update_GTFS_Static_files.main.os.remove')
    def test_file_needs_update(self, mock_os_remove, mock_boto_client, mock_requests_head, mock_requests_get, mock_file_open, mock_zip):
        s3_mock = MagicMock()
        mock_boto_client.return_value = s3_mock
        mock_last_modified_in_s3 = MagicMock()
        mock_last_modified_in_s3.read.return_value = b"Wed, 01 Jan 2020 00:00:00 GMT"
        s3_mock.get_object.return_value = {'Body': mock_last_modified_in_s3}
        
        mock_response_head = MagicMock()
        mock_response_head.headers = {'Last-Modified': datetime.datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')}
        mock_requests_head.return_value = mock_response_head

        mock_response_get = MagicMock()
        mock_response_get.iter_content = MagicMock(return_value=[b'test data'])
        mock_requests_get.return_value = mock_response_get

        mock_os_remove = MagicMock()
        mock_os_remove.return_value = None

        event = {'bucket_name': 'test_bucket', 'url': 'https://example.com/test.zip'}

        lambda_handler(event, None)

        # Verify that the new file was uploaded
        s3_mock.put_object.assert_called()

if __name__ == '__main__':
    TestCase.main()
