import unittest
from unittest.mock import MagicMock, patch

from BIXI_Services.BIXI_Historical_Data_Checker.main import (
    check_new_data,
    handler,
    scrape_bixi_historic_data_urls,
)

MOCK_ENVIRON = {
    "ATLAS_URI": "your_mongodb_uri",
    "MONGO_DATABASE_NAME": "your_db_name",
    "BIXI_DATA_URL": "your_data_url",
    "BIXI_CDN": "your_cdn",
    "BIXI_URL_COLLECTION": "your_url_collection",
}


class TestBixiDataScraper(unittest.TestCase):

    @patch("requests.get")
    def test_scrape_bixi_historic_data_urls(self, mock_get):
        mock_response = MagicMock()
        mock_response.content = b"""
            <div class="row">
                <div class="col-md-12 document-col">
                    <div class="row document-row">
                        <h3 class="document-title title-4">2024</h3>
                        <a href="https://cdn.bixi.com/data/2024_data.zip" download="">Download (ZIP)</a>
                    </div>
                    <div class="row document-row">
                        <h3 class="document-title title-4">2022</h3>
                        <a href="https://cdn.bixi.com/data/2022_data.zip" download="">Download (ZIP)</a>
                    </div>
                    <div class="row document-row">
                        <h3 class="document-title title-4">2019</h3>
                        <a href="https://cdn.bixi.com/data/2018_data.zip" download="">Download (ZIP)</a>
                    </div>
                    <div class="row document-row">
                        <h3 class="document-title title-4">2026</h3>
                        <a href="https://cdn.bixi.com/data/2026_data.zip" download="">Download (ZIP)</a>
                    </div>
                </div>
            </div>
            """
        mock_get.return_value = mock_response
        expected_result = {
            2022: "https://cdn.bixi.com/data/2022_data.zip",
            2024: "https://cdn.bixi.com/data/2024_data.zip",
        }
        result = scrape_bixi_historic_data_urls(
            url="dummy_url", bixi_cdn="cdn.bixi.com", low=2020, top=2025
        )
        self.assertDictEqual(result, expected_result)
        self.assertEqual(list(result.keys()), [2022, 2024])

    @patch("BIXI_Services.BIXI_Historical_Data_Checker.main.MongoClient")
    def test_check_new_data(self, mock_mongo_client):
        mock_collection = (
            mock_mongo_client.return_value.__getitem__.return_value.__getitem__.return_value
        )
        mock_collection.find.return_value = [{"_id": "https://cdn.bixi.com/data1"}]
        expected_new_data = {2022: "https://cdn.bixi.com/data2"}
        scraped_data_dict = {
            2021: "https://cdn.bixi.com/data1",
            2022: "https://cdn.bixi.com/data2",
        }
        new_data = check_new_data(
            scraped_data_dict,
            mongo_uri="mock_mongo_uri",
            bixi_db_name="mock_db_name",
            url_collection="mock_collection_name",
        )
        self.assertEqual(new_data, expected_new_data)

    @patch.dict("os.environ", MOCK_ENVIRON)
    @patch("BIXI_Services.BIXI_Historical_Data_Checker.main.check_new_data")
    @patch(
        "BIXI_Services.BIXI_Historical_Data_Checker.main.scrape_bixi_historic_data_urls"
    )
    def test_handler_success(self, mock_scrape, mock_check_new_data):
        mock_scrape.return_value = {2022: "https://cdn.bixi.com/data2"}
        mock_check_new_data.return_value = {2022: "https://cdn.bixi.com/data2"}
        expected_result = {
            "process": True,
            "urls": {2022: "https://cdn.bixi.com/data2"},
        }
        result = handler(None, None)
        self.assertEqual(result, expected_result)

    @patch.dict("os.environ", MOCK_ENVIRON)
    @patch("BIXI_Services.BIXI_Historical_Data_Checker.main.check_new_data")
    @patch(
        "BIXI_Services.BIXI_Historical_Data_Checker.main.scrape_bixi_historic_data_urls"
    )
    def test_handler_failure(self, mock_scrape, mock_check_new_data):
        mock_scrape.side_effect = Exception("Test exception")
        expected_result = {"process": False, "urls": {}, "error": "Test exception"}
        result = handler(None, None)
        self.assertEqual(result, expected_result)
