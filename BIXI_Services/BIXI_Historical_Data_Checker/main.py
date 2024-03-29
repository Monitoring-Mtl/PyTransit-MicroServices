import os

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

BIXI_DATA_URL = os.getenv("BIXI_DATA_URL", "https://bixi.com/en/open-data")
BIXI_CDN = os.getenv("BIXI_CDN", "https://s3.ca-central-1.amazonaws.com/cdn.bixi.com/")
MONGODB_URI = os.getenv("MONGODB_URI", "")
BIXI_DB_NAME = os.getenv("BIXI_DB_NAME", "")
BIXI_URL_COLLECTION = os.getenv("BIXI_HISTORIC_URLS_COLLECTION", "bixi_historic_urls")


def scrape_bixi_historic_data_urls(url=BIXI_DATA_URL, bixi_cdn=BIXI_CDN):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    urls = [a["href"] for a in soup.find_all("a", href=True) if bixi_cdn in a["href"]]
    return urls


def check_new_data(
    scraped_urls,
    mongo_uri=MONGODB_URI,
    bixi_db_name=BIXI_DB_NAME,
    url_collection=BIXI_URL_COLLECTION,
):
    client = MongoClient(mongo_uri)
    db = client[bixi_db_name]
    collection = db[url_collection]
    saved_urls = {document["_id"] for document in collection.find({}, {"_id": 1})}
    new_urls = [url for url in scraped_urls if url not in saved_urls]
    return new_urls


def handler(event, context):
    try:
        print("checking for new historic data..")
        scraped_urls = scrape_bixi_historic_data_urls()
        new_urls = check_new_data(scraped_urls)
        result = {"process": bool(new_urls), "urls": new_urls}
        print("finished checking for new historic data successfully.")
        return result
    except Exception as e:
        print(e.__str__())
        return {"process": False, "urls": [], "error": str(e)}
