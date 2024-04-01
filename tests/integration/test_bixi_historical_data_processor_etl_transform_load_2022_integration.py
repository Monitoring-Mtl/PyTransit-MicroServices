import os
from unittest import IsolatedAsyncioTestCase

import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient

from BIXI_Services.BIXI_Historical_Data_Processor.main import Config, TransformLoad2022


class TestTransformLoad2022(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.config = Config(**os.environ)
        self.client = AsyncIOMotorClient(self.config.ATLAS_URI)
        self.db = self.client[self.config.MONGO_DATABASE_NAME]
        self.transform_load = TransformLoad2022()
        self.base_path = os.path.abspath(os.path.dirname(__file__))
        self.test_files = [
            os.path.join(self.base_path, "test_files", "file1.csv"),
            os.path.join(self.base_path, "test_files", "file2.csv"),
        ]

    async def asyncTearDown(self):
        await self.db[self.config.BIXI_LOCATION_COLLECTION].drop()
        await self.db[self.config.BIXI_TRIP_COLLECTION].drop()
        self.client.close()

    async def test_transform_load(self):
        await self.transform_load.transform_load(self.test_files, self.config)
        trip_count = await self.db[self.config.BIXI_TRIP_COLLECTION].count_documents({})
        location_count = await self.db[
            self.config.BIXI_LOCATION_COLLECTION
        ].count_documents({})
        self.assertGreater(trip_count, 0)
        self.assertGreater(location_count, 0)

    async def test_transform_load_with_non_existing_files(self):
        non_existing_files = [
            os.path.join(self.base_path, "test_files", "non_existing_file1.csv"),
            os.path.join(self.base_path, "test_files", "non_existing_file2.csv"),
        ]
        await self.transform_load.transform_load(non_existing_files, self.config)
        trip_count = await self.db[self.config.BIXI_TRIP_COLLECTION].count_documents({})
        location_count = await self.db[
            self.config.BIXI_LOCATION_COLLECTION
        ].count_documents({})
        self.assertEqual(trip_count, 0)
        self.assertEqual(location_count, 0)

    async def test_transform_load_with_missing_columns(self):
        original_file = self.test_files[0]
        modified_file = os.path.join(self.base_path, "test_files", "modified_file.csv")
        df = pd.read_csv(original_file)
        df.drop(columns=["STARTSTATIONNAME"], inplace=True)
        df.to_csv(modified_file, index=False)
        try:
            await self.transform_load.transform_load([modified_file], self.config)
            trip_count = await self.db[
                self.config.BIXI_TRIP_COLLECTION
            ].count_documents({})
            location_count = await self.db[
                self.config.BIXI_LOCATION_COLLECTION
            ].count_documents({})
            self.assertEqual(trip_count, 0)
            self.assertEqual(location_count, 0)
        finally:
            os.remove(modified_file)

    async def test_transform_load_with_data_duplication(self):
        file1 = self.test_files[0]
        await self.transform_load.transform_load([file1], self.config)
        combined_file = os.path.join(
            self.base_path, "test_files", "file1_file2_combined.csv"
        )
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(self.test_files[1])
        combined_df = pd.concat([df1, df2])
        combined_df.to_csv(combined_file, index=False)
        try:
            await self.transform_load.transform_load([combined_file], self.config)
            trip_count = await self.db[
                self.config.BIXI_TRIP_COLLECTION
            ].count_documents({})
            self.assertLessEqual(trip_count, len(combined_df))
        finally:
            os.remove(combined_file)
