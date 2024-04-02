import os
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pymongo import InsertOne

from BIXI_Services.BIXI_Historical_Data_Processor.main import (
    Config,
    etl,
    extract,
    save_url,
)


class TestExtract(TestCase):
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.requests.get")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.zipfile.ZipFile")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.os.makedirs")
    def test_extract(self, mock_makedirs, mock_zipfile, mock_get):
        mock_response = MagicMock()
        mock_response.content = b"zip content"
        mock_get.return_value.__enter__.return_value = mock_response

        mock_zip_file_instance = MagicMock()
        mock_zip_file_instance.namelist.return_value = ["file1.csv", "file2.txt"]
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file_instance

        url = "https://example.com/data.zip"
        bixi_cdn = "https://cdn.example.com"
        path = "extract/path"
        result = extract(url, bixi_cdn, path)

        mock_get.assert_called_once()
        mock_zipfile.assert_called_once()
        mock_makedirs.assert_called_once_with(path, exist_ok=True)

        expected_paths = [os.path.abspath(os.path.join(path, "file1.csv"))]
        self.assertTrue(all(item in result for item in expected_paths))


class TestAsyncOperations(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.config = Config(
            ATLAS_URI="mongodb+srv://test",
            MONGO_DATABASE_NAME="test_db",
            BIXI_URL_COLLECTION="test_col",
            BIXI_TRIP_COLLECTION="trips",
            BIXI_LOCATION_COLLECTION="locations",
            BIXI_CDN="https://cdn.example.com",
            BIXI_DEFAULT_EXTRACT_PATH="/path/to/extract",
            BIXI_CHUNK_SIZE=25000,
            BIXI_QUEUE_SIZE=1,
            BIXI_CONCURRENCY=4,
        )

    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.AsyncIOMotorClient")
    async def test_save_url(self, mock_client):
        url = "https://example.com/data.zip"
        year = 2021
        mock_collection = MagicMock()
        mock_collection.bulk_write = AsyncMock()
        mock_client.return_value.__getitem__.return_value.__getitem__.return_value = (
            mock_collection
        )
        await save_url(url, year, self.config)
        mock_collection.bulk_write.assert_awaited_once()
        args, _ = mock_collection.bulk_write.call_args
        operations = args[0]
        self.assertEqual(len(operations), 1)
        self.assertIsInstance(operations[0], InsertOne)
        self.assertEqual(operations[0]._doc, {"_id": url, "year": year})

    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.extract")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.TransformLoadContext")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.save_url")
    async def test_etl(self, mock_save_url, mock_context, mock_extract):
        url = "https://example.com/data.zip"
        year = 2021
        mock_extract.return_value = ["/path/to/extract/file1.csv"]
        mock_context_instance = MagicMock()
        mock_context.return_value = mock_context_instance
        mock_context_instance.execute_transform_load = AsyncMock()

        await etl(url, year, self.config)

        mock_extract.assert_called_once_with(
            url, self.config.BIXI_CDN, self.config.BIXI_DEFAULT_EXTRACT_PATH
        )
        mock_context_instance.execute_transform_load.assert_awaited_once_with(
            ["/path/to/extract/file1.csv"], self.config
        )
        mock_save_url.assert_awaited_once_with(url, year, self.config)
