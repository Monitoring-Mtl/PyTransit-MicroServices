import os
from unittest import TestCase
from unittest.mock import MagicMock, Mock, call, patch

import polars as pl
import pytest
from pydantic import ValidationError
from pymongo import InsertOne

from BIXI_Services.BIXI_Historical_Data_Processor.main import (
    Config,
    etl,
    extract,
    handler,
    map,
    save_url,
    validate_columns,
)

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


class TestOperations(TestCase):
    def setUp(self):
        self.config = Config(
            ATLAS_URI="mongodb+srv://test",
            MONGO_DATABASE_NAME="test_db",
            BIXI_URL_COLLECTION="test_col",
            BIXI_TRIP_COLLECTION="trips",
            BIXI_LOCATION_COLLECTION="locations",
            BIXI_CDN="https://cdn.example.com",
            BIXI_DEFAULT_EXTRACT_PATH="/path/to/extract",
            BIXI_CHUNK_SIZE=25000,
        )

    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.MongoClient")
    def test_save_url(self, mock_client):
        url = "https://example.com/data.zip"
        year = 2021
        mock_collection = MagicMock()
        mock_client.return_value.__getitem__.return_value.__getitem__.return_value = (
            mock_collection
        )
        save_url(url, year, self.config)
        mock_collection.bulk_write.assert_called_once()
        args, _ = mock_collection.bulk_write.call_args
        operations = args[0]
        self.assertEqual(len(operations), 1)
        self.assertIsInstance(operations[0], InsertOne)
        self.assertEqual(operations[0]._doc, {"_id": url, "year": year})

    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.extract")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.TransformLoadContext")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.save_url")
    def test_etl(self, mock_save_url, mock_context, mock_extract):
        url = "https://example.com/data.zip"
        year = 2021
        mock_extract.return_value = ["/path/to/extract/file1.csv"]
        mock_context_instance = MagicMock()
        mock_context.return_value = mock_context_instance
        mock_context_instance.execute_transform_load = MagicMock()

        etl(url, year, self.config)

        mock_extract.assert_called_once_with(
            url, self.config.BIXI_CDN, self.config.BIXI_DEFAULT_EXTRACT_PATH
        )
        mock_context_instance.execute_transform_load.assert_called_once_with(
            ["/path/to/extract/file1.csv"], self.config
        )
        mock_save_url.assert_called_once_with(url, year, self.config)


class TestConfigModel(TestCase):
    def test_config_model_creation(self):
        config_data = {
            "ATLAS_URI": "mongodb+srv://yourcluster",
            "MONGO_DATABASE_NAME": "exampleDB",
            "BIXI_CDN": "https://cdn.example.com",
            "BIXI_DEFAULT_EXTRACT_PATH": "/path/to/extract",
            "BIXI_LOCATION_COLLECTION": "locations",
            "BIXI_TRIP_COLLECTION": "trips",
            "BIXI_CHUNK_SIZE": 5,
            "BIXI_URL_COLLECTION": "urls",
            "additional": "whatever",
        }
        config = Config(**config_data)
        self.assertIsInstance(config, Config)

    def test_config_model_validation_error(self):
        config_data = {
            # "ATLAS_URI": "mongodb+srv://yourcluster",
            "MONGO_DATABASE_NAME": "exampleDB",
            "BIXI_CDN": "https://cdn.example.com",
            "BIXI_DEFAULT_EXTRACT_PATH": "/path/to/extract",
            "BIXI_LOCATION_COLLECTION": "locations",
            "BIXI_TRIP_COLLECTION": "trips",
            "BIXI_CHUNK_SIZE": 5,
            "BIXI_URL_COLLECTION": "urls",
        }
        with self.assertRaises(ValidationError):
            Config(**config_data)


@pytest.mark.parametrize(
    "left, right, expected",
    [
        (["a", "b"], ["1", "2"], {"a": "1", "b": "2"}),
        ([], [], {}),
        (["a", "b"], ["1"], {"a": "1"}),
        (["a"], ["1", "2"], {"a": "1"}),
    ],
)
def test_map(left, right, expected):
    assert map(left, right) == expected


@pytest.mark.parametrize(
    "df_columns, columns, expected",
    [
        (["A", "B"], ["a", "b"], True),
        (["A", "B"], ["a"], True),
        (["A"], ["a", "b"], False),
        ([], [], True),
    ],
)
def test_validate_columns(df_columns, columns, expected):
    df = pl.DataFrame({col: [1] for col in df_columns})
    assert validate_columns(df, columns) == expected
