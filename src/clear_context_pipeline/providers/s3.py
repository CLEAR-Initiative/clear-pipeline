"""Shared S3 client for the reliefweb + knowledge-base pipeline.

One construction point so auth, endpoint, and request config don't drift
across the assets that read and write S3.

`when_required` checksum config: boto3 >= 1.36 defaults to streaming an
aws-chunked CRC32 checksum on PutObject, which the S3-compatible backend
behind ``S3_ENDPOINT`` rejects with ``NotImplemented: Transfering payloads
in multiple chunks using aws-chunked is not supported``. ``when_required``
attaches a checksum only when the operation mandates one — PutObject does
not — so uploads go as a single, unchunked payload.
"""

import os

import boto3
from botocore.config import Config


def s3_client():
    """The pipeline's S3 client, built from the ``S3_*`` environment."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
