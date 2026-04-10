"""Download WorldPop population GeoTIFF for Sudan and upload to S3.

Usage:
    python scripts/upload_population_tiff.py

Requires S3 env vars: S3_ENDPOINT, S3_BUCKET, S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
"""

import os
import sys
import tempfile

import boto3
import httpx
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file

WORLDPOP_URL = (
    "https://data.worldpop.org/GIS/Population/Global_2015_2030/"
    "R2025A/2026/SDN/v1/100m/constrained/"
    "sdn_pop_2026_CN_100m_R2025A_v1.tif"
)
S3_KEY = "population/sdn_pop_2026_CN_100m_R2025A_v1.tif"


def main():
    endpoint = os.environ.get("S3_ENDPOINT")
    bucket = os.environ.get("S3_BUCKET")
    region = os.environ.get("S3_REGION", "auto")
    access_key = os.environ.get("S3_ACCESS_KEY_ID")
    secret_key = os.environ.get("S3_SECRET_ACCESS_KEY")

    if not all([endpoint, bucket, access_key, secret_key]):
        print("Missing S3 env vars. Set S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY")
        sys.exit(1)

    # Download from WorldPop
    print(f"Downloading from {WORLDPOP_URL} ...")
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tmp_path = tmp.name
        with httpx.stream("GET", WORLDPOP_URL, timeout=300, follow_redirects=True) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            for chunk in resp.iter_bytes(chunk_size=8192):
                tmp.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"\r  {downloaded / 1e6:.1f} MB / {total / 1e6:.1f} MB ({pct:.0f}%)", end="", flush=True)
    print(f"\nDownloaded to {tmp_path} ({os.path.getsize(tmp_path) / 1e6:.1f} MB)")

    # Upload to S3
    print(f"Uploading to s3://{bucket}/{S3_KEY} ...")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3.upload_file(tmp_path, bucket, S3_KEY)
    print("Upload complete!")

    # Cleanup
    os.unlink(tmp_path)
    print("Done.")


if __name__ == "__main__":
    main()
