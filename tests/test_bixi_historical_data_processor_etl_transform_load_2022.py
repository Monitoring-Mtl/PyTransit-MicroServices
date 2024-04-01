from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from pymongo import InsertOne, UpdateOne

from BIXI_Services.BIXI_Historical_Data_Processor.main import TransformLoad2022

CHUNK = pd.DataFrame(
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
EMPTY_CHUNK = pd.DataFrame(
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


def test_prepare_trip_data():
    transform_load = TransformLoad2022()
    data = {
        "STARTSTATIONNAME": ["StationA", "StationB"],
        "ENDSTATIONNAME": ["StationC", "StationD"],
        "STARTTIMEMS": [1000, 2000],
        "ENDTIMEMS": [1500, 2500],
    }
    chunk = pd.DataFrame(data)
    expected_data = {
        "STARTSTATIONNAME": ["StationA", "StationB"],
        "ENDSTATIONNAME": ["StationC", "StationD"],
        "STARTTIMEMS": [1000, 2000],
        "ENDTIMEMS": [1500, 2500],
        "DURATIONMS": [500, 500],
    }
    expected = pd.DataFrame(expected_data)[transform_load.trip_columns]
    result = transform_load.prepare_trip_data(chunk)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )


@pytest.mark.asyncio
async def test_process_trips_with_valid_data():
    mock_collection = MagicMock()
    mock_collection.bulk_write = AsyncMock()
    transform_load = TransformLoad2022()
    await transform_load.process_trips(CHUNK, mock_collection)
    mock_collection.bulk_write.assert_awaited_once()
    args, _ = mock_collection.bulk_write.call_args
    operations = args[0]
    assert len(operations) > 0
    for op in operations:
        assert isinstance(op, InsertOne)
        assert set(op._doc.keys()) == {
            "STARTSTATIONNAME",
            "ENDSTATIONNAME",
            "STARTTIMEMS",
            "ENDTIMEMS",
            "DURATIONMS",
        }


@pytest.mark.asyncio
async def test_process_trips_with_empty_data():
    mock_collection = MagicMock()
    mock_collection.bulk_write = AsyncMock()
    transform_load = TransformLoad2022()
    empty_chunk = pd.DataFrame(EMPTY_CHUNK)
    await transform_load.process_trips(empty_chunk, mock_collection)
    mock_collection.bulk_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_locations_with_valid_data():
    mock_collection = MagicMock()
    mock_collection.bulk_write = AsyncMock()
    transform_load = TransformLoad2022()
    await transform_load.process_locations(CHUNK, mock_collection)
    mock_collection.bulk_write.assert_awaited_once()
    args, _ = mock_collection.bulk_write.call_args
    operations = args[0]
    assert len(operations) > 0
    for op in operations:
        assert isinstance(op, UpdateOne)


@pytest.mark.asyncio
async def test_process_locations_with_empty_data():
    mock_collection = MagicMock()
    mock_collection.bulk_write = AsyncMock()
    transform_load = TransformLoad2022()
    await transform_load.process_locations(EMPTY_CHUNK, mock_collection)
    mock_collection.bulk_write.assert_not_awaited()


def test_prepare_location_data_unique_stations():
    transform_load = TransformLoad2022()
    expected_df = pd.DataFrame(
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
    result_df = transform_load.prepare_location_data(CHUNK)
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_prepare_location_data_empty_chunk():
    transform_load = TransformLoad2022()
    expected_df = pd.DataFrame(
        columns=["name", "arrondissement", "latitude", "longitude"]
    )
    result_df = transform_load.prepare_location_data(EMPTY_CHUNK)
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_create_location_update_operations_types_and_values():
    transform_load = TransformLoad2022()
    stations = pd.DataFrame(
        {
            "name": ["Station A", "Station B"],
            "arrondissement": ["Arrondissement 1", "Arrondissement 2"],
            "latitude": [45.1, 45.2],
            "longitude": [-73.1, -73.2],
        }
    )
    operations = transform_load.create_location_update_operations(stations)
    for op in operations:
        assert isinstance(op, UpdateOne)


@pytest.mark.asyncio
async def test_get_highest_starttimems():
    db_collection = AsyncMock()
    db_collection.find_one = AsyncMock(return_value={"STARTTIMEMS": 1000})
    transform_load = TransformLoad2022()
    result = await transform_load.get_highest_starttimems(db_collection)
    assert result == 1000
    db_collection.find_one.assert_awaited_with(
        sort=[("STARTTIMEMS", -1)], projection={"STARTTIMEMS": 1}
    )


def test_map():
    transform_load = TransformLoad2022()
    columns = [
        "STARTSTATIONNAME",
        "STARTSTATIONARRONDISSEMENT",
        "STARTSTATIONLATITUDE",
        "STARTSTATIONLONGITUDE",
    ]
    db_columns = ["name", "arrondissement", "latitude", "longitude"]
    expected_result = {
        "STARTSTATIONNAME": "name",
        "STARTSTATIONARRONDISSEMENT": "arrondissement",
        "STARTSTATIONLATITUDE": "latitude",
        "STARTSTATIONLONGITUDE": "longitude",
    }
    result = transform_load.map(columns, db_columns)
    assert result == expected_result
