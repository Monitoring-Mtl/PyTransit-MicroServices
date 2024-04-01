from unittest.mock import AsyncMock

import pytest

from BIXI_Services.BIXI_Historical_Data_Processor.main import (
    Config,
    TransformLoad2014,
    TransformLoad2022,
    TransformLoadContext,
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
        BIXI_QUEUE_SIZE=1,
        BIXI_CONCURRENCY=4,
    )


@pytest.mark.asyncio
async def test_execute_transform_load_2014(config):
    context = TransformLoadContext(2014)
    context.strategy.transform_load = AsyncMock(return_value=None)
    await context.execute_transform_load(["file.csv"], config)
    context.strategy.transform_load.assert_awaited_once_with(["file.csv"], config)


@pytest.mark.asyncio
async def test_execute_transform_load_2022(config):
    context = TransformLoadContext(2022)
    context.strategy.transform_load = AsyncMock(return_value=None)
    await context.execute_transform_load(["file.csv"], config)
    context.strategy.transform_load.assert_awaited_once_with(["file.csv"], config)


def test_choose_strategy_2014():
    context = TransformLoadContext(2014)
    assert isinstance(context.strategy, TransformLoad2014)


def test_choose_strategy_2022():
    context = TransformLoadContext(2022)
    assert isinstance(context.strategy, TransformLoad2022)


def test_choose_strategy_invalid_year():
    with pytest.raises(ValueError):
        TransformLoadContext(2010)
