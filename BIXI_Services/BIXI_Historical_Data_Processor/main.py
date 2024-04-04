import asyncio
import os
import traceback
import zipfile
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse, urlunparse

import pandas as pd
import requests
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from pymongo import InsertOne, UpdateOne


class Config(BaseModel):
    ATLAS_URI: str
    MONGO_DATABASE_NAME: str
    BIXI_CDN: str
    BIXI_DEFAULT_EXTRACT_PATH: str
    BIXI_LOCATION_COLLECTION: str
    BIXI_TRIP_COLLECTION: str
    BIXI_QUEUE_SIZE: int
    BIXI_CHUNK_SIZE: int
    BIXI_CONCURRENCY: int
    BIXI_URL_COLLECTION: str


class TransformLoadStrategy(ABC):
    @abstractmethod
    async def transform_load(self, csv_files: list[str], config: Config):
        pass


class TransformLoadContext:
    def __init__(self, year: int):
        self.strategy = self._choose_strategy(year)

    def _choose_strategy(self, year: int):
        if 2014 <= year < 2022:
            return TransformLoad2014()
        elif year >= 2022:
            return TransformLoad2022()
        else:
            raise ValueError("Unsupported year")

    async def execute_transform_load(self, csv_files: list[str], config: Config):
        await self.strategy.transform_load(csv_files, config)


class TransformLoad2014(TransformLoadStrategy):
    async def transform_load(self, csv_files, config):
        raise NotImplementedError("TransformLoad2014 transform_load not implemented.")


class TransformLoad2022(TransformLoadStrategy):
    required_columns = [
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

    start_location_columns = [
        "STARTSTATIONNAME",
        "STARTSTATIONARRONDISSEMENT",
        "STARTSTATIONLATITUDE",
        "STARTSTATIONLONGITUDE",
    ]

    end_location_columns = [
        "ENDSTATIONNAME",
        "ENDSTATIONARRONDISSEMENT",
        "ENDSTATIONLATITUDE",
        "ENDSTATIONLONGITUDE",
    ]

    location_columns = ["name", "arrondissement", "latitude", "longitude"]

    trip_columns = [
        "STARTSTATIONNAME",
        "ENDSTATIONNAME",
        "STARTTIMEMS",
        "ENDTIMEMS",
        "DURATIONMS",
    ]

    def map(self, columns, db_colums=location_columns):
        return {column: name for column, name in zip(columns, db_colums)}

    async def transform_load(self, csv_files, config):
        # db objects
        db = AsyncIOMotorClient(config.ATLAS_URI)[config.MONGO_DATABASE_NAME]
        col_trips = db[config.BIXI_TRIP_COLLECTION]
        col_locations = db[config.BIXI_LOCATION_COLLECTION]
        # queue & workers
        q = asyncio.Queue(config.BIXI_QUEUE_SIZE)
        workers = [
            asyncio.create_task(self.worker(q)) for _ in range(config.BIXI_CONCURRENCY)
        ]
        # start
        max_starttimems = await self.get_highest_starttimems(col_trips)
        for file in csv_files:
            file = os.path.abspath(file)
            # ensure file exists
            if not os.path.exists(file):
                print(file, "doesn't exist.")
                continue
            # ensure the csv has the right columns
            df = pd.read_csv(file, nrows=0)
            if not all(column in df.columns for column in self.required_columns):
                print(f"CSV file {file} does not contain required columns.")
                continue
            # enqueue tasks for processing each chunk
            print("Starting processing:", file)
            for chunk in pd.read_csv(file, chunksize=config.BIXI_CHUNK_SIZE):
                if max_starttimems is not None:
                    chunk = chunk[chunk["STARTTIMEMS"] > max_starttimems]
                await q.put(
                    asyncio.create_task(self.process_locations(chunk, col_locations))
                )
                await q.put(asyncio.create_task(self.process_trips(chunk, col_trips)))
        await q.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    async def get_highest_starttimems(self, col):
        doc = await col.find_one(
            sort=[("STARTTIMEMS", -1)], projection={"STARTTIMEMS": 1}
        )
        return doc["STARTTIMEMS"] if doc else None

    async def process_locations(self, chunk, collection):
        locations = self.prepare_location_data(chunk)
        if operations := self.create_location_update_operations(locations):
            await collection.bulk_write(operations, ordered=False)

    def prepare_location_data(self, chunk: pd.DataFrame):
        return (
            pd.concat(
                [
                    chunk[self.start_location_columns].rename(
                        columns=self.map(self.start_location_columns)
                    ),
                    chunk[self.end_location_columns].rename(
                        columns=self.map(self.end_location_columns)
                    ),
                ]
            )
            .drop_duplicates(subset=["name"])
            .reset_index(drop=True)
        )

    def create_location_update_operations(self, locations: pd.DataFrame):
        return [
            UpdateOne(
                {"_id": row["name"]},
                {
                    "$setOnInsert": {
                        "arrondissement": row["arrondissement"],
                        "latitude": row["latitude"],
                        "longitude": row["longitude"],
                    }
                },
                upsert=True,
            )
            for _, row in locations.iterrows()
        ]

    async def process_trips(self, chunk: pd.DataFrame, collection):
        trip_docs = self.prepare_trip_data(chunk).to_dict("records")
        if trip_docs:
            operations = [InsertOne(doc) for doc in trip_docs]
            await collection.bulk_write(operations, ordered=False)

    def prepare_trip_data(self, chunk: pd.DataFrame):
        chunk = chunk.copy()
        chunk["DURATIONMS"] = chunk["ENDTIMEMS"] - chunk["STARTTIMEMS"]
        return chunk[self.trip_columns]

    async def worker(self, queue: asyncio.Queue):
        while True:
            task = await queue.get()
            try:
                await task
            finally:
                queue.task_done()


def handler(event, context):
    return asyncio.run(async_handler(event, context))


async def async_handler(event, context):
    print("historic data processing started.")
    try:
        urls = event["urls"]
        if not urls:
            print("no new data to process.")
            return {"status": "Success", "filenames": None}
        sorted_urls = dict(sorted(urls.items()))
        config = Config(**os.environ)
        files = []
        for year, url in sorted_urls.items():
            files.append(await etl(url, int(year), config))
        print(f"historic data processed successfully at {datetime.now().isoformat()}.")
        return {"status": "Success", "filenames": str(files)}
    except Exception as e:
        traceback.print_exc()
        return {"status": "Error", "error": str(e), "processedUrls": []}


async def etl(url: str, year: int, config: Config):
    print("ETL process started for URL:", url)
    # extract
    files = extract(url, config.BIXI_CDN, config.BIXI_DEFAULT_EXTRACT_PATH)
    # transform and load
    context = TransformLoadContext(year)
    result = []
    try:
        await context.execute_transform_load(files, config)
        await save_url(url, year, config)
        result = files
    except NotImplementedError:
        print("strategy not implemented for year", year)
    # cleaning up
    for file_path in files:
        file_path = os.path.abspath(file_path)
        try:
            os.remove(file_path)
        except Exception:
            print("delete failed for", file_path)
    return result


def extract(url: str, bixi_cdn: str, path):
    # fix for "Full server-side request forgery" security warning
    url_parsed = urlparse(url)
    bixi_cdn_parsed = urlparse(bixi_cdn)
    url = urlunparse(
        (bixi_cdn_parsed.scheme, bixi_cdn_parsed.netloc, url_parsed.path, "", "", "")
    )
    print("start download and extract", url)
    with requests.get(url) as r:
        r.raise_for_status()
        with zipfile.ZipFile(BytesIO(r.content)) as z:
            os.makedirs(path, exist_ok=True)
            z.extractall(path)
            extracted = z.namelist()
    if not extracted:
        raise Exception("No file extracted.")
    extracted = [
        os.path.abspath(os.path.join(path, file))
        for file in extracted
        if file.endswith(".csv")
    ]
    print("extracted files:", extracted)
    return extracted


async def save_url(url, year, config: Config):
    client = AsyncIOMotorClient(config.ATLAS_URI)
    db = client[config.MONGO_DATABASE_NAME]
    collection = db[config.BIXI_URL_COLLECTION]
    operation = InsertOne({"_id": url, "year": year})
    await collection.bulk_write([operation], ordered=False)
    print(f"saved: {url}")
