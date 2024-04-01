import os
import traceback

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

def scrape_bixi_historic_data_urls(url, bixi_cdn, low=2014, top=2099):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    urls = {
        int(row.find("h3", class_="document-title title-4").text.strip()): row.find(
            "a", href=True
        )["href"]
        for row in soup.find_all("div", class_="document-row")
        if bixi_cdn in row.find("a", href=True)["href"]
        and low
        <= int(row.find("h3", class_="document-title title-4").text.strip())
        <= top
    }
    return dict(sorted(urls.items()))


def check_new_data(urls: dict, mongo_uri, bixi_db_name, url_collection):
    client = MongoClient(mongo_uri)
    db = client[bixi_db_name]
    collection = db[url_collection]
    saved_urls = {document["_id"] for document in collection.find({}, {"_id": 1})}
    new_data = {year: url for year, url in urls.items() if url not in saved_urls}
    return new_data


def handler(event, context):
    try:
        print("checking for new historic data..")
        atlas_uri = os.environ["ATLAS_URI"]
        data_url = os.environ["BIXI_DATA_URL"]
        cdn = os.environ["BIXI_CDN"]
        db_name = os.environ["MONGO_DATABASE_NAME"]
        url_collection = os.environ["BIXI_URL_COLLECTION"]
        scraped_urls = scrape_bixi_historic_data_urls(data_url, cdn)
        print("scraped_urls", scraped_urls)
        new_urls = check_new_data(scraped_urls, atlas_uri, db_name, url_collection)
        print("finished checking for new historic data successfully.")
        print("new urls:", new_urls)
        return {"process": bool(new_urls), "urls": new_urls}
    except Exception as e:
        traceback.print_exc()
        return {"process": False, "urls": {}, "error": str(e)}
