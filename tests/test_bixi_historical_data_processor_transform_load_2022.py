from unittest.mock import MagicMock

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pymongo import InsertOne, UpdateOne

from BIXI_Services.BIXI_Historical_Data_Processor.main import (
    Config,
    TransformLoad2014,
    TransformLoad2022,
    TransformLoadContext,
)

CHUNK = pl.DataFrame(
    {
        "startStationName": ["station a", "station b"],
        "startStationArrondissement": ["arrondissement 1", "arrondissement 2"],
        "startStationLatitude": [45.1, 45.2],
        "startStationLongitude": [-73.1, -73.2],
        "endStationName": ["station b", "station c"],
        "endStationArrondissement": ["arrondissement 2", "arrondissement 3"],
        "endStationLatitude": [45.2, 45.3],
        "endStationLongitude": [-73.2, -73.3],
        "startTimeMs": [1000, 2000],
        "endTimeMs": [1500, 2500],
    }
)
EMPTY_CHUNK = pl.DataFrame(
    schema=[
        "startStationName",
        "startStationArrondissement",
        "startStationLatitude",
        "startStationLongitude",
        "endStationName",
        "endStationArrondissement",
        "endStationLatitude",
        "endStationLongitude",
        "startTimeMs",
        "endTimeMs",
    ]
)


@pytest.fixture
def config():
    return Config(
        ATLAS_URI="mongodb+srv://test",
        MONGO_DATABASE_NAME="test_db",
        BIXI_URL_COLLECTION="test_col",
        BIXI_TRIP_COLLECTION="trips",
        BIXI_LOCATION_COLLECTION="locations",
        BIXI_CDN="https://cdn.example.com",
        BIXI_DEFAULT_EXTRACT_PATH="/path/to/extract",
        BIXI_CHUNK_SIZE=25000,
    )


def test_prepare_trip_data():
    transform_load = TransformLoad2022()
    data = {
        "startStationName": ["stationa", "stationb"],
        "endStationName": ["stationc", "stationd"],
        "startTimeMs": [1000, 2000],
        "endTimeMs": [1500, 2500],
    }
    chunk = pl.DataFrame(data)
    expected_data = {
        "startStationName": ["stationa", "stationb"],
        "endStationName": ["stationc", "stationd"],
        "startTimeMs": [1000, 2000],
        "endTimeMs": [1500, 2500],
        "durationMs": [500, 500],
    }
    expected = pl.DataFrame(expected_data)[transform_load.trip_columns_db]
    result = transform_load.prepare_trip_data(chunk)
    assert_frame_equal(result, expected, check_row_order=False)


def test_process_trips_with_valid_data():
    mock_collection = MagicMock()
    transform_load = TransformLoad2022()
    transform_load.process_trips(CHUNK, mock_collection)
    mock_collection.bulk_write.assert_called_once()
    args, _ = mock_collection.bulk_write.call_args
    operations = args[0]
    assert len(operations) > 0
    for op in operations:
        assert isinstance(op, InsertOne)
        assert set(op._doc.keys()) == {
            "startStationName",
            "endStationName",
            "startTimeMs",
            "endTimeMs",
            "durationMs",
        }


def test_process_trips_with_empty_data():
    mock_collection = MagicMock()
    transform_load = TransformLoad2022()
    empty_chunk = pl.DataFrame(EMPTY_CHUNK)
    transform_load.process_trips(empty_chunk, mock_collection)
    mock_collection.bulk_write.assert_not_called()


def test_process_locations_with_valid_data():
    mock_collection = MagicMock()
    transform_load = TransformLoad2022()
    transform_load.process_locations(CHUNK, mock_collection)
    mock_collection.bulk_write.assert_called_once()
    args, _ = mock_collection.bulk_write.call_args
    operations = args[0]
    assert len(operations) > 0
    for op in operations:
        assert isinstance(op, UpdateOne)


def test_process_locations_with_empty_data():
    mock_collection = MagicMock()
    transform_load = TransformLoad2022()
    transform_load.process_locations(EMPTY_CHUNK, mock_collection)
    mock_collection.bulk_write.assert_not_called()


def test_prepare_location_data_unique_stations():
    transform_load = TransformLoad2022()
    expected_df = pl.DataFrame(
        {
            "name": ["station a", "station b", "station c"],
            "arrondissement": [
                "arrondissement 1",
                "arrondissement 2",
                "arrondissement 3",
            ],
            "latitude": [45.1, 45.2, 45.3],
            "longitude": [-73.1, -73.2, -73.3],
        }
    )
    result_df = transform_load.prepare_location_data(CHUNK)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_prepare_location_data_empty_chunk():
    transform_load = TransformLoad2022()
    expected_df = pl.DataFrame(
        schema=["name", "arrondissement", "latitude", "longitude"]
    )
    result_df = transform_load.prepare_location_data(EMPTY_CHUNK)
    assert_frame_equal(result_df, expected_df, check_row_order=False)


def test_create_location_update_operations_types_and_values():
    transform_load = TransformLoad2022()
    stations = pl.DataFrame(
        {
            "name": ["station a", "station b"],
            "arrondissement": ["arrondissement 1", "arrondissement 2"],
            "latitude": [45.1, 45.2],
            "longitude": [-73.1, -73.2],
        }
    )
    operations = transform_load.create_location_update_operations(stations)
    for op in operations:
        assert isinstance(op, UpdateOne)


def test_get_highest_starttimems():
    db_collection = MagicMock()
    db_collection.find_one = MagicMock(return_value={"startTimeMs": 1000})
    transform_load = TransformLoad2022()
    result = transform_load.get_highest_starttimems(db_collection)
    assert result == 1000
    db_collection.find_one.assert_called_with(
        sort=[("startTimeMs", -1)], projection={"startTimeMs": 1}
    )


def test_execute_transform_load_2014(config):
    context = TransformLoadContext(2014)
    context.strategy.transform_load = MagicMock(return_value=None)
    context.execute_transform_load(["file.csv"], config)
    context.strategy.transform_load.assert_called_once_with(["file.csv"], config)


def test_execute_transform_load_2022(config):
    context = TransformLoadContext(2022)
    context.strategy.transform_load = MagicMock(return_value=None)
    context.execute_transform_load(["file.csv"], config)
    context.strategy.transform_load.assert_called_once_with(["file.csv"], config)


def test_choose_strategy_2014():
    context = TransformLoadContext(2014)
    assert isinstance(context.strategy, TransformLoad2014)


def test_choose_strategy_2022():
    context = TransformLoadContext(2022)
    assert isinstance(context.strategy, TransformLoad2022)


def test_choose_strategy_invalid_year():
    with pytest.raises(ValueError):
        TransformLoadContext(2010)
