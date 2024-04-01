import os
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pymongo import InsertOne, UpdateOne

from BIXI_Services.BIXI_Historical_Data_Processor.main import (
    create_update_operations,
    etl,
    handler,
    main,
    prepare_location_data,
    process_locations,
    process_trips,
)

CHUNK = DataFrame(
    {
        "STARTSTATIONNAME": ["Station A", "Station B"],
        "STARTSTATIONARRONDISSEMENT": ["Arrondissement 1", "Arrondissement 2"],
        "STARTSTATIONLATITUDE": [45.1, 45.2],
        "STARTSTATIONLONGITUDE": [-73.1, -73.2],
        "ENDSTATIONNAME": ["Station B", "Station C"],
        "ENDSTATIONARRONDISSEMENT": ["Arrondissement 2", "Arrondissement 3"],
        "ENDSTATIONLATITUDE": [45.2, 45.3],
        "ENDSTATIONLONGITUDE": [-73.2, -73.3],
        "STARTTIMEMS": [1000, 2000],
        "ENDTIMEMS": [1500, 2500],
    }
)
EMPTY_CHUNK = DataFrame(
    columns=[
        "STARTSTATIONNAME",
        "STARTSTATIONARRONDISSEMENT",
        "STARTSTATIONLATITUDE",
        "STARTSTATIONLONGITUDE",
        "ENDSTATIONNAME",
        "ENDSTATIONARRONDISSEMENT",
        "ENDSTATIONLATITUDE",
        "ENDSTATIONLONGITUDE",
        "STARTTIMEMS",
        "ENDTIMEMS",
    ]
)
MOCK_ENVIRON = {
    "ATLAS_URI": "test_mongodb_uri",
    "MONGO_DATABASE_NAME": "test_db",
    "BIXI_DATA_URL": "test_data_url",
    "BIXI_CDN": "test_cdn",
    "BIXI_URL_COLLECTION": "test_url_collection",
    "BIXI_LOCATION_COLLECTION": "location_collection",
    "BIXI_TRIP_COLLECTION": "trip_collection",
    "BIXI_QUEUE_SIZE": "1",
    "BIXI_CHUNK_SIZE": "1000",
    "BIXI_CONCURRENCY": "4",
    "BIXI_DEFAULT_EXTRACT_PATH": "/tmp/",
}


class TestBixiHistoricalDataProcessor(TestCase):
    @patch("os.environ", {})
    def test_handler_returns_error_for_missing_urls_parameter(self):
        assert os.getenv("ATLAS_URI") is None
        result = handler({}, {})
        self.assertEqual(result["status"], "Error")

    def test_prepare_station_location_unique_stations(self):
        expected_df = DataFrame(
            {
                "name": ["Station A", "Station B", "Station C"],
                "arrondissement": [
                    "Arrondissement 1",
                    "Arrondissement 2",
                    "Arrondissement 3",
                ],
                "latitude": [45.1, 45.2, 45.3],
                "longitude": [-73.1, -73.2, -73.3],
            }
        )
        result_df = prepare_location_data(CHUNK)
        assert_frame_equal(
            result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
        )

    def test_prepare_location_data_empty_chunk(self):
        expected_df = DataFrame(
            columns=["name", "arrondissement", "latitude", "longitude"]
        )
        result_df = prepare_location_data(EMPTY_CHUNK)
        assert_frame_equal(
            result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
        )

    def test_create_update_operations_types_and_values(self):
        stations = DataFrame(
            {
                "name": ["Station A", "Station B"],
                "arrondissement": ["Arrondissement 1", "Arrondissement 2"],
                "latitude": [45.1, 45.2],
                "longitude": [-73.1, -73.2],
            }
        )
        operations = create_update_operations(stations)
        for op in operations:
            self.assertIsInstance(op, UpdateOne)


class TestBixiHistoricalDataProcessorAsync(IsolatedAsyncioTestCase):
    async def test_process_locations_with_valid_data(self):
        mock_collection = MagicMock()
        mock_collection.bulk_write = AsyncMock()
        await process_locations(CHUNK, mock_collection)
        mock_collection.bulk_write.assert_awaited_once()
        args, _ = mock_collection.bulk_write.call_args
        operations = args[0]
        self.assertTrue(len(operations) > 0)
        for op in operations:
            self.assertIsInstance(op, UpdateOne)

    async def test_process_locations_with_empty_data(self):
        mock_collection = MagicMock()
        mock_collection.bulk_write = AsyncMock()
        await process_locations(EMPTY_CHUNK, mock_collection)
        mock_collection.bulk_write.assert_not_awaited()

    async def test_process_trips_with_valid_data(self):
        mock_collection = MagicMock()
        mock_collection.bulk_write = AsyncMock()
        await process_trips(CHUNK, mock_collection)
        mock_collection.bulk_write.assert_awaited_once()
        args, _ = mock_collection.bulk_write.call_args
        operations = args[0]
        self.assertTrue(len(operations) > 0)
        for op in operations:
            self.assertIsInstance(op, InsertOne)
            self.assertSetEqual(
                set(op._doc.keys()),
                {
                    "STARTSTATIONNAME",
                    "ENDSTATIONNAME",
                    "STARTTIMEMS",
                    "ENDTIMEMS",
                    "DURATION",
                },
            )

    async def test_process_trips_with_empty_data(self):
        mock_collection = MagicMock()
        mock_collection.bulk_write = AsyncMock()
        await process_trips(EMPTY_CHUNK, mock_collection)
        mock_collection.bulk_write.assert_not_awaited()


class TestETL(IsolatedAsyncioTestCase):
    @patch.dict(os.environ, MOCK_ENVIRON)
    @patch("os.remove")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.save_url")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.transform_load")
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.extract")
    async def test_etl(
        self,
        mock_extract,
        mock_transform_load,
        mock_save_url,
        mock_remove,
    ):
        assert os.environ["ATLAS_URI"] == "test_mongodb_uri"
        url_item = ("http://example.com/data.zip", "2020")

        mock_extract.return_value = ["/tmp/file1.csv", "/tmp/file2.csv"]

        with patch(
            "BIXI_Services.BIXI_Historical_Data_Processor.main.AsyncIOMotorClient"
        ) as mock_client:
            mock_db = MagicMock()
            mock_client.return_value.__getitem__.return_value = mock_db

            await etl(url_item)

            mock_extract.assert_called_once_with("http://example.com/data.zip", "/tmp/")
            mock_transform_load.assert_called_once_with(
                ["/tmp/file1.csv", "/tmp/file2.csv"],
                mock_db["location_collection"],
                mock_db["trip_collection"],
                1,
                1000,
                4,
            )
            mock_save_url.assert_called_once_with(
                mock_db["BIXI_URL_COLLECTION"], "http://example.com/data.zip", "2020"
            )
            mock_remove.assert_has_calls(
                [call("/tmp/file1.csv"), call("/tmp/file2.csv")], any_order=True
            )


class TestMain(IsolatedAsyncioTestCase):
    @patch.dict(os.environ, MOCK_ENVIRON)
    @patch("BIXI_Services.BIXI_Historical_Data_Processor.main.etl")
    async def test_main(
        self,
        mock_etl,
    ):
        assert os.environ["ATLAS_URI"] == "test_mongodb_uri"
        event = {
            "urls": {
                2021: "http://example.com/data2021.zip",
                2020: "http://example.com/data2020.zip",
            }
        }
        context = {}
        with patch(
            "BIXI_Services.BIXI_Historical_Data_Processor.main.etl",
            new_callable=AsyncMock,
        ) as mock_etl:
            mock_etl.side_effect = [
                ["/tmp/file2020_1.csv", "/tmp/file2020_2.csv"],
                ["/tmp/file2021_1.csv", "/tmp/file2021_2.csv"],
            ]
            await main(event, context)
            calls = [
                call(("http://example.com/data2020.zip", 2020)),
                call(("http://example.com/data2021.zip", 2021)),
            ]
            mock_etl.assert_has_calls(calls, any_order=False)
            self.assertEqual(mock_etl.call_count, 2)
