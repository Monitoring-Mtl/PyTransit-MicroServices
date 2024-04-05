import os
from unittest import TestCase
from unittest.mock import Mock, call, patch

from BIXI_Services.BIXI_Historical_Data_Processor.main import Config, handler

MOCK_ENVIRON = {
    "ATLAS_URI": "test_mongodb_uri",
    "MONGO_DATABASE_NAME": "test_db",
    "BIXI_DATA_URL": "test_data_url",
    "BIXI_CDN": "test_cdn",
    "BIXI_URL_COLLECTION": "test_url_collection",
    "BIXI_LOCATION_COLLECTION": "location_collection",
    "BIXI_TRIP_COLLECTION": "trip_collection",
    "BIXI_CHUNK_SIZE": "1000",
    "BIXI_DEFAULT_EXTRACT_PATH": "/tmp/",
}


@patch.dict("os.environ", MOCK_ENVIRON)
class TestHandler(TestCase):

    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.etl")
    def test_handler(self, mock_etl: Mock):
        event = {
            "urls": {
                2021: "http://example.com/data2021.zip",
                2020: "http://example.com/data2020.zip",
            }
        }
        with patch(
            "BIXI_Services.BIXI_Historical_Data_Processor.main.etl",
            new_callable=Mock,
        ) as mock_etl:
            mock_etl.side_effect = [
                ["/tmp/file2020_1.csv", "/tmp/file2020_2.csv"],
                ["/tmp/file2021_1.csv", "/tmp/file2021_2.csv"],
            ]
            handler(event, None)
            config = Config(**os.environ)
            calls = [
                call("http://example.com/data2020.zip", 2020, config),
                call("http://example.com/data2021.zip", 2021, config),
            ]
            mock_etl.assert_has_calls(calls, any_order=False)
            self.assertEqual(mock_etl.call_count, 2)

    def test_handler_empty_urls(self):
        event = {"urls": {}}
        result = handler(event, None)
        self.assertIsNone(result["filenames"])
        result = handler({}, None)
        self.assertEqual(result["status"], "Error")
