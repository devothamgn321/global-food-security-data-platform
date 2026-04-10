#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


DATASET_URL = "https://data.humdata.org/dataset/global-wfp-food-prices"
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "latest_food_prices.csv"


def find_latest_csv_link() -> str:
    response = requests.get(DATASET_URL, timeout=60)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    candidate_links = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        href_lower = href.lower()

        # Skip metadata/export links
        if "download_metadata" in href_lower:
            continue

        # Look for actual downloadable CSV resource links
        if ".csv" in href_lower or "download" in href_lower:
            full_url = urljoin(DATASET_URL, href)
            candidate_links.append(full_url)

    # Prefer links that look like actual resource/download links
    for url in candidate_links:
        url_lower = url.lower()
        if "resource" in url_lower or "download" in url_lower:
            return url

    if candidate_links:
        return candidate_links[0]

    raise RuntimeError("Could not find a usable CSV download link on the WFP dataset page.")


def download_latest_food_file() -> Path:
    csv_url = find_latest_csv_link()
    print(f"Found CSV link: {csv_url}")

    response = requests.get(csv_url, timeout=120)
    response.raise_for_status()

    with open(OUTPUT_FILE, "wb") as f:
        f.write(response.content)

    print(f"Downloaded latest food file to: {OUTPUT_FILE}")
    return OUTPUT_FILE


def run() -> Path:
    print("Starting food download step...")
    return download_latest_food_file()


if __name__ == "__main__":
    run()