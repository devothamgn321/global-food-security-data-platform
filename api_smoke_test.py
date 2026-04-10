#!/usr/bin/env python
# coding: utf-8

import os
import sys
import time

import pandas as pd
import requests


BASE_URL = os.getenv("API_BASE_URL", "http://app:8001")


def wait_for_api(timeout: int = 180, interval: int = 5) -> bool:
    start = time.time()

    while time.time() - start < timeout:
        try:
            response = requests.get(f"{BASE_URL}/api/health", timeout=10)
            if response.status_code == 200:
                return True
        except Exception:
            pass

        time.sleep(interval)

    return False


def main() -> None:
    print(f"Waiting for API at {BASE_URL} ...")

    if not wait_for_api():
        print("API did not become available in time.")
        sys.exit(1)

    print("API is up. Running smoke tests...")

    health = requests.get(f"{BASE_URL}/api/health", timeout=20)
    print("Health status code:", health.status_code)
    print("Health response:", health.json())

    if health.status_code != 200:
        print("Health endpoint failed.")
        sys.exit(1)

    get_all = requests.get(f"{BASE_URL}/api/get_all", timeout=60)
    print("get_all status code:", get_all.status_code)

    if get_all.status_code != 200:
        print("get_all endpoint failed.")
        sys.exit(1)

    data = get_all.json()
    df = pd.DataFrame(data)

    print("Returned rows:", len(df))
    print("Returned columns:", list(df.columns))

    if df.empty:
        print("API returned no records.")
        sys.exit(1)

    print(df.head().to_string(index=False))
    print("API smoke test passed.")


if __name__ == "__main__":
    main()