from unittest.mock import MagicMock

import polars as pl
from polars.testing import assert_frame_equal
from pymongo import InsertOne, UpdateOne

from BIXI_Services.BIXI_Historical_Data_Processor.main import TransformLoad2022

CHUNK = pl.DataFrame(
    {
        "startstationname": ["station a", "station b"],
        "startstationarrondissement": ["arrondissement 1", "arrondissement 2"],
        "startstationlatitude": [45.1, 45.2],
        "startstationlongitude": [-73.1, -73.2],
        "endstationname": ["station b", "station c"],
        "endstationarrondissement": ["arrondissement 2", "arrondissement 3"],
        "endstationlatitude": [45.2, 45.3],
        "endstationlongitude": [-73.2, -73.3],
        "starttimems": [1000, 2000],
        "endtimems": [1500, 2500],
    }
)
EMPTY_CHUNK = pl.DataFrame(
    schema=[
        "startstationname",
        "startstationarrondissement",
        "startstationlatitude",
        "startstationlongitude",
        "endstationname",
        "endstationarrondissement",
        "endstationlatitude",
        "endstationlongitude",
        "starttimems",
        "endtimems",
    ]
)


def test_prepare_trip_data():
    transform_load = TransformLoad2022()
    data = {
        "startstationname": ["stationa", "stationb"],
        "endstationname": ["stationc", "stationd"],
        "starttimems": [1000, 2000],
        "endtimems": [1500, 2500],
    }
    chunk = pl.DataFrame(data)
    expected_data = {
        "startstationname": ["stationa", "stationb"],
        "endstationname": ["stationc", "stationd"],
        "starttimems": [1000, 2000],
        "endtimems": [1500, 2500],
        "durationms": [500, 500],
    }
    expected = pl.DataFrame(expected_data)[transform_load.trip_columns]
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
            "startstationname",
            "endstationname",
            "starttimems",
            "endtimems",
            "durationms",
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
    db_collection.find_one = MagicMock(return_value={"starttimems": 1000})
    transform_load = TransformLoad2022()
    result = transform_load.get_highest_starttimems(db_collection)
    assert result == 1000
    db_collection.find_one.assert_called_with(
        sort=[("starttimems", -1)], projection={"starttimems": 1}
    )


def test_map():
    transform_load = TransformLoad2022()
    columns = [
        "startstationname",
        "startstationarrondissement",
        "startstationlatitude",
        "startstationlongitude",
    ]
    db_columns = ["name", "arrondissement", "latitude", "longitude"]
    expected_result = {
        "startstationname": "name",
        "startstationarrondissement": "arrondissement",
        "startstationlatitude": "latitude",
        "startstationlongitude": "longitude",
    }
    result = transform_load.map(columns, db_columns)
    assert result == expected_result
