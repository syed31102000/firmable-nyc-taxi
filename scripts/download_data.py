import os
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
RAW_DIR = "data/raw"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_OUT = "dbt/seeds/taxi_zone_lookup.csv"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs("dbt/seeds", exist_ok=True)

def download_file(url: str, out_path: str):
    if os.path.exists(out_path):
        print(f"Skipping existing file: {out_path}")
        return
    print(f"Downloading {url}")
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    print(f"Saved to {out_path}")

def main():
    for month in MONTHS:
        filename = f"yellow_tripdata_2023-{month}.parquet"
        url = f"{BASE_URL}/{filename}"
        out_path = os.path.join(RAW_DIR, filename)
        download_file(url, out_path)

    download_file(ZONE_URL, ZONE_OUT)
    print("All downloads complete.")

if __name__ == "__main__":
    main()