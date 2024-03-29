import unittest
from unittest.mock import MagicMock, patch

from BIXI_Services.BIXI_Historical_Data_Checker.main import (
    check_new_data,
    handler,
    scrape_bixi_historic_data_urls,
)


class TestBixiDataScraper(unittest.TestCase):

    @patch("BIXI_Services.BIXI_Historical_Data_Checker.main.requests.get")
    def test_scrape_bixi_historic_data_urls(self, mock_get):
        test_cdn = "https://cdn.bixi.com/"
        mock_response = MagicMock()
        mock_response.content = (
            b'<html><body>'
            b'<a href="https://cdn.bixi.com/data1">Data1</a>'
            b'<a href="https://other.com/data2">Data2</a>'
            b'</body></html>'
        )
        mock_get.return_value = mock_response
        expected_urls = ["https://cdn.bixi.com/data1"]
        urls = scrape_bixi_historic_data_urls(bixi_cdn=test_cdn)
        self.assertEqual(urls, expected_urls)

    @patch("BIXI_Services.BIXI_Historical_Data_Checker.main.MongoClient")
    def test_check_new_data(self, mock_mongo_client):
        mock_collection = (
            mock_mongo_client.return_value.__getitem__.return_value.__getitem__.return_value
        )
        mock_collection.find.return_value = [{"_id": "https://cdn.bixi.com/data1"}]
        expected_new_urls = ["https://cdn.bixi.com/data2"]
        scraped_urls = ["https://cdn.bixi.com/data1", "https://cdn.bixi.com/data2"]
        new_urls = check_new_data(
            scraped_urls,
            mongo_uri="mock_mongo_uri",
            bixi_db_name="mock_db_name",
            url_collection="mock_collection_name",
        )
        self.assertEqual(new_urls, expected_new_urls)

    @patch("BIXI_Services.BIXI_Historical_Data_Checker.main.check_new_data")
    @patch(
        "BIXI_Services.BIXI_Historical_Data_Checker.main.scrape_bixi_historic_data_urls"
    )
    def test_handler_success(self, mock_scrape, mock_check_new_data):
        mock_scrape.return_value = ["https://cdn.bixi.com/data1"]
        mock_check_new_data.return_value = ["https://cdn.bixi.com/data1"]
        expected_result = {"process": True, "urls": ["https://cdn.bixi.com/data1"]}
        result = handler(None, None)
        self.assertEqual(result, expected_result)

    @patch("BIXI_Services.BIXI_Historical_Data_Checker.main.check_new_data")
    @patch(
        "BIXI_Services.BIXI_Historical_Data_Checker.main.scrape_bixi_historic_data_urls"
    )
    def test_handler_failure(self, mock_scrape, mock_check_new_data):
        mock_scrape.side_effect = Exception("Test exception")
        expected_result = {"process": False, "urls": [], "error": "Test exception"}
        result = handler(None, None)
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
