import os
import traceback
import zipfile
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse, urlunparse

import polars as pl
import requests
from pydantic import BaseModel
from pymongo import InsertOne, MongoClient, UpdateOne


class Config(BaseModel):
    ATLAS_URI: str
    MONGO_DATABASE_NAME: str
    BIXI_CDN: str
    BIXI_DEFAULT_EXTRACT_PATH: str
    BIXI_LOCATION_COLLECTION: str
    BIXI_TRIP_COLLECTION: str
    BIXI_CHUNK_SIZE: int
    BIXI_URL_COLLECTION: str


class TransformLoadStrategy(ABC):
    @abstractmethod
    def transform_load(self, csv_files: list[str], config: Config):
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

    def execute_transform_load(self, csv_files: list[str], config: Config):
        self.strategy.transform_load(csv_files, config)


class TransformLoad2014(TransformLoadStrategy):
    def transform_load(self, csv_files, config):
        raise NotImplementedError("TransformLoad2014 transform_load not implemented.")


class TransformLoad2022(TransformLoadStrategy):
    required_columns = [
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

    start_location_columns = [
        "startstationname",
        "startstationarrondissement",
        "startstationlatitude",
        "startstationlongitude",
    ]

    end_location_columns = [
        "endstationname",
        "endstationarrondissement",
        "endstationlatitude",
        "endstationlongitude",
    ]

    location_columns = ["name", "arrondissement", "latitude", "longitude"]

    trip_columns = [
        "startstationname",
        "endstationname",
        "starttimems",
        "endtimems",
        "durationms",
    ]

    def map(self, columns, db_colums=location_columns):
        return {column: name for column, name in zip(columns, db_colums)}

    def transform_load(self, csv_files, config):
        # db objects
        db = MongoClient(config.ATLAS_URI)[config.MONGO_DATABASE_NAME]
        col_trips = db[config.BIXI_TRIP_COLLECTION]
        col_locations = db[config.BIXI_LOCATION_COLLECTION]
        # start
        max_starttimems = self.get_highest_starttimems(col_trips)
        for file in csv_files:
            file = os.path.abspath(file)
            # ensure file exists
            if not os.path.exists(file):
                print(file, "doesn't exist.")
                continue
            # ensure the csv has the right columns
            header = pl.read_csv(file, n_rows=0)
            columns = [c.lower() for c in header.columns]
            if not all(column in columns for column in self.required_columns):
                print(f"CSV file {file} does not contain required columns.")
                continue
            # read & process
            print("Starting processing:", file)
            reader = pl.read_csv_batched(file, batch_size=config.BIXI_CHUNK_SIZE)
            while True:
                chunks = reader.next_batches(1)
                if not chunks:
                    break
                chunk = chunks[0]
                chunk = chunk.rename(self.map(chunk.columns, self.required_columns))
                if max_starttimems is not None:
                    chunk = chunk.filter(pl.col("starttimems") > int(max_starttimems))
                self.process_locations(chunk, col_locations)
                self.process_trips(chunk, col_trips)

    def get_highest_starttimems(self, col):
        doc = col.find_one(sort=[("starttimems", -1)], projection={"starttimems": 1})
        return doc["starttimems"] if doc else None

    def process_locations(self, chunk, collection):
        locations = self.prepare_location_data(chunk)
        if operations := self.create_location_update_operations(locations):
            collection.bulk_write(operations, ordered=False)

    def prepare_location_data(self, chunk: pl.DataFrame):
        return (
            chunk[self.start_location_columns]
            .rename(self.map(self.start_location_columns))
            .vstack(
                chunk[self.end_location_columns].rename(
                    self.map(self.end_location_columns)
                )
            )
        ).unique(subset=["name"])

    def create_location_update_operations(self, locations: pl.DataFrame):
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
            for row in locations.iter_rows(named=True)
        ]

    def process_trips(self, chunk: pl.DataFrame, collection):
        trip_docs = self.prepare_trip_data(chunk).to_dicts()
        if trip_docs:
            operations = [InsertOne(doc) for doc in trip_docs]
            collection.bulk_write(operations, ordered=False)

    def prepare_trip_data(self, chunk: pl.DataFrame):
        chunk = chunk.with_columns(
            (pl.col("endtimems") - pl.col("starttimems")).alias("durationms")
        )
        return chunk[self.trip_columns]


def handler(event, context):
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
            files.append(etl(url, int(year), config))
        print(f"historic data processed successfully at {datetime.now().isoformat()}.")
        return {"status": "Success", "filenames": str(files)}
    except Exception as e:
        traceback.print_exc()
        return {"status": "Error", "error": str(e), "processedUrls": []}


def etl(url: str, year: int, config: Config):
    print("ETL process started for URL:", url)
    # extract
    files = extract(url, config.BIXI_CDN, config.BIXI_DEFAULT_EXTRACT_PATH)
    # transform and load
    context = TransformLoadContext(year)
    result = []
    try:
        context.execute_transform_load(files, config)
        save_url(url, year, config)
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


def save_url(url, year, config: Config):
    client = MongoClient(config.ATLAS_URI)
    db = client[config.MONGO_DATABASE_NAME]
    collection = db[config.BIXI_URL_COLLECTION]
    operation = InsertOne({"_id": url, "year": year})
    collection.bulk_write([operation], ordered=False)
    print(f"saved: {url}")
