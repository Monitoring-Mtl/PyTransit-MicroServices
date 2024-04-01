import os
from unittest.async_case import IsolatedAsyncioTestCase
from unittest.mock import patch

import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient

from BIXI_Services.BIXI_Historical_Data_Processor.main import (
    REQUIRED_COLUMNS,
    transform_load,
)

@patch.dict("os.environ", {"MONGO_DATABASE_NAME": "test-monitoring-mtl"}, clear=False)
class TestTransformLoadIntegration(IsolatedAsyncioTestCase):
    """Testing the transform_load function's integration with the database. Depends on 
    test csv files in ./test_files."""

    async def asyncSetUp(self):
        assert os.environ["MONGO_DATABASE_NAME"] == "test-monitoring-mtl"
        self.uri = os.environ["ATLAS_URI"]
        self.db_name = os.environ["MONGO_DATABASE_NAME"]
        self.location_collection_name = "location_collection"
        self.trip_collection_name = "trip_collection"
        self.client = AsyncIOMotorClient(self.uri)
        self.db = self.client[self.db_name]
        self.base_path = os.path.abspath(os.path.dirname(__file__))
        self.test_files = [
            os.path.join(self.base_path, "test_files", "file1.csv"),
            os.path.join(self.base_path, "test_files", "file2.csv"),
        ]
        for file in self.test_files:
            assert os.path.exists(file), f"{file} does not exist"
        self.queue_size = 1
        self.chunk_size = 250000
        self.concurrency = 4

    async def asyncTearDown(self):
        await self.db[self.location_collection_name].drop()
        await self.db[self.trip_collection_name].drop()
        self.client.close()

    async def test_transform_load(self):
        await transform_load(
            self.test_files,
            self.db[self.location_collection_name],
            self.db[self.trip_collection_name],
            self.queue_size,
            self.chunk_size,
            self.concurrency,
        )
        trip_count = await self.db[self.trip_collection_name].count_documents({})
        location_count = await self.db[self.location_collection_name].count_documents(
            {}
        )
        self.assertEqual(trip_count, 40)
        self.assertEqual(location_count, 56)

    async def test_transform_load_with_non_existing_files(self):
        non_existing_files = [
            os.path.join(self.base_path, "test_files", "non_existing_file1.csv"),
            os.path.join(self.base_path, "test_files", "non_existing_file2.csv"),
        ]
        await transform_load(
            non_existing_files,
            self.db[self.location_collection_name],
            self.db[self.trip_collection_name],
            self.queue_size,
            self.chunk_size,
            self.concurrency,
        )
        trip_count = await self.db[self.trip_collection_name].count_documents({})
        location_count = await self.db[self.location_collection_name].count_documents(
            {}
        )
        self.assertEqual(trip_count, 0)
        self.assertEqual(location_count, 0)

    async def test_transform_load_with_missing_columns(self):
        original_file = os.path.join(self.base_path, "test_files", "file1.csv")
        modified_file = os.path.join(self.base_path, "test_files", "modified_file.csv")
        # read original and remove a column
        df = pd.read_csv(original_file)
        df.drop(columns=[REQUIRED_COLUMNS[0]], inplace=True)
        df.to_csv(modified_file, index=False)
        try:
            await transform_load(
                [modified_file],
                self.db[self.location_collection_name],
                self.db[self.trip_collection_name],
                self.queue_size,
                self.chunk_size,
                self.concurrency,
            )
            trip_count = await self.db[self.trip_collection_name].count_documents({})
            location_count = await self.db[
                self.location_collection_name
            ].count_documents({})
            self.assertEqual(trip_count, 0)
            self.assertEqual(location_count, 0)
        finally:
            os.remove(modified_file)

    async def test_transform_load_with_data_duplication(self):
        await transform_load(
            [os.path.join(self.base_path, "test_files", "file1.csv")],
            self.db[self.location_collection_name],
            self.db[self.trip_collection_name],
            self.queue_size,
            self.chunk_size,
            self.concurrency,
        )

        # create a file with old + new data
        combined_file = os.path.join(
            self.base_path, "test_files", "file1_file2_combined.csv"
        )
        df1 = pd.read_csv(os.path.join(self.base_path, "test_files", "file1.csv"))
        df2 = pd.read_csv(os.path.join(self.base_path, "test_files", "file2.csv"))
        combined_df = pd.concat([df1, df2])
        assert len(combined_df) == 40, "Combined file does not contain 40 rows"
        combined_df.to_csv(combined_file, index=False)

        # should save only the new data
        try:
            await transform_load(
                [combined_file],
                self.db[self.location_collection_name],
                self.db[self.trip_collection_name],
                self.queue_size,
                self.chunk_size,
                self.concurrency,
            )
            trip_count = await self.db[self.trip_collection_name].count_documents({})
            self.assertLessEqual(trip_count, 40)
        finally:
            os.remove(combined_file)
