import asyncio
import os
import traceback
import zipfile
from io import BytesIO
from urllib.parse import urlparse, urlunparse

import pandas as pd
import requests
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from pymongo import InsertOne, UpdateOne

REQUIRED_COLUMNS = [
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


async def save_url(collection, url, year=None):
    operation = InsertOne({"_id": url, "year": year})
    await collection.bulk_write([operation], ordered=False)
    print(f"saved: {url}")


def prepare_location_data(chunk):
    return (
        pd.concat(
            [
                chunk[
                    [
                        "STARTSTATIONNAME",
                        "STARTSTATIONARRONDISSEMENT",
                        "STARTSTATIONLATITUDE",
                        "STARTSTATIONLONGITUDE",
                    ]
                ].rename(
                    columns={
                        "STARTSTATIONNAME": "name",
                        "STARTSTATIONARRONDISSEMENT": "arrondissement",
                        "STARTSTATIONLATITUDE": "latitude",
                        "STARTSTATIONLONGITUDE": "longitude",
                    }
                ),
                chunk[
                    [
                        "ENDSTATIONNAME",
                        "ENDSTATIONARRONDISSEMENT",
                        "ENDSTATIONLATITUDE",
                        "ENDSTATIONLONGITUDE",
                    ]
                ].rename(
                    columns={
                        "ENDSTATIONNAME": "name",
                        "ENDSTATIONARRONDISSEMENT": "arrondissement",
                        "ENDSTATIONLATITUDE": "latitude",
                        "ENDSTATIONLONGITUDE": "longitude",
                    }
                ),
            ]
        )
        .drop_duplicates(subset=["name"])
        .reset_index(drop=True)
    )


def create_update_operations(locations):
    operations = [
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
    return operations


async def process_locations(chunk, collection):
    locations = prepare_location_data(chunk)
    if operations := create_update_operations(locations):
        await collection.bulk_write(operations, ordered=False)


async def process_trips(chunk, collection):
    chunk["DURATION"] = chunk["ENDTIMEMS"] - chunk["STARTTIMEMS"]
    trip_docs = chunk[
        ["STARTSTATIONNAME", "ENDSTATIONNAME", "STARTTIMEMS", "ENDTIMEMS", "DURATION"]
    ].to_dict("records")
    if trip_docs:
        operations = [InsertOne(doc) for doc in trip_docs]
        await collection.bulk_write(operations, ordered=False)


async def worker(queue: asyncio.Queue):
    while True:
        task = await queue.get()
        try:
            await task
        finally:
            queue.task_done()


async def get_highest_starttimems(col):
    doc = await col.find_one(sort=[("STARTTIMEMS", -1)], projection={"STARTTIMEMS": 1})
    return doc["STARTTIMEMS"] if doc else None


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
            z.extractall(path=path)
            extracted = z.namelist()
            print("extracted files:", extracted)
            print("download and extract completed successfully.")
    if not extracted:
        raise Exception("No file extracted.")
    return [os.path.abspath(file) for file in extracted if file.endswith(".csv")]


async def transform_load(
    csv_files,
    col_locations,
    col_trips,
    q_size,
    chunk_size,
    concurrency,
):
    max_starttimems = await get_highest_starttimems(col_trips)
    q = asyncio.Queue(q_size)
    workers = [asyncio.create_task(worker(q)) for _ in range(concurrency)]
    for file in csv_files:
        file = os.path.abspath(file)
        if not file.startswith(os.getcwd()):
            print(f"Access denied: {file}")
            continue
        # ensure file exists
        if not os.path.exists(file):
            print(file, "doesn't exist.")
            continue
        print("Starting processing:", file)
        # ensure the csv has the right columns
        df = pd.read_csv(file, nrows=0)
        if not all(column in df.columns for column in REQUIRED_COLUMNS):
            print(f"CSV file {file} does not contain required columns.")
            continue
        # enqueue tasks for processing each chunk
        for chunk in pd.read_csv(file, chunksize=chunk_size):
            # ensure we don't save data we already have
            if max_starttimems is not None:
                chunk = chunk[chunk["STARTTIMEMS"] > max_starttimems]
            await q.put(asyncio.create_task(process_locations(chunk, col_locations)))
            await q.put(asyncio.create_task(process_trips(chunk, col_trips)))
    await q.join()
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


async def etl(url: str, year: int, config: Config):
    print("ETL process started for URL:", url)
    # db object
    client = AsyncIOMotorClient(config.ATLAS_URI)
    db = client[config.MONGO_DATABASE_NAME]
    # extract
    files = extract(url, config.BIXI_CDN, config.BIXI_DEFAULT_EXTRACT_PATH)
    # transform and load
    await transform_load(
        files,
        db[config.BIXI_LOCATION_COLLECTION],
        db[config.BIXI_TRIP_COLLECTION],
        config.BIXI_QUEUE_SIZE,
        config.BIXI_CHUNK_SIZE,
        config.BIXI_CONCURRENCY,
    )
    # cleaning up
    await save_url(db[config.BIXI_URL_COLLECTION], url, year)
    for file_path in files:
        file_path = os.path.abspath(file_path)
        if not file_path.startswith(os.getcwd()):
            print(f"Access denied: {file_path}")
            continue
        os.remove(file_path)
    return file_path


async def main(event, context):
    print("historic data processing started.")
    try:
        urls: dict[int, str] = event["urls"]
        if not urls:
            print("no new data to process.")
            return {"status": "Success", "filenames": None}
        sorted_urls = dict(sorted(urls.items()))
        config = Config(**os.environ)
        files = []
        for year, url in sorted_urls.items():
            files.extend(await etl(url, year, config))
        print("historic data processed successfully.")
        return {"status": "Success", "filenames": files}
    except Exception as e:
        traceback.print_exc()
        return {"status": "Error", "error": str(e), "processedUrls": []}


def handler(event, context):
    return asyncio.run(main(event, context))
