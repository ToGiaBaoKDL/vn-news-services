from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

import boto3
import zstandard
from botocore.exceptions import ClientError


class S3PayloadStore:
    def __init__(self, *, endpoint_url: str) -> None:
        self.client = boto3.client("s3", endpoint_url=endpoint_url)

    def check_bucket(self, bucket: str) -> None:
        self.client.head_bucket(Bucket=bucket)

    def exists(self, uri: str) -> bool:
        bucket, key = split_s3_uri(uri)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if is_missing_object(error):
                return False
            raise
        return True

    def write_compressed(self, uri: str, payload: bytes, *, content_type: str) -> None:
        bucket, key = split_s3_uri(uri)
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=zstandard.compress(payload),
            ContentType=content_type,
            ContentEncoding="zstd",
        )

    def read_compressed(self, uri: str) -> bytes:
        bucket, key = split_s3_uri(uri)
        response = self.client.get_object(Bucket=bucket, Key=key)
        return zstandard.decompress(response["Body"].read())

    def write_json(self, uri: str, value: dict[str, Any]) -> None:
        bucket, key = split_s3_uri(uri)
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(value, separators=(",", ":"), sort_keys=True).encode(),
            ContentType="application/json",
        )

    def read_json(self, uri: str) -> dict[str, Any] | None:
        bucket, key = split_s3_uri(uri)
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if is_missing_object(error):
                return None
            raise
        value = json.loads(response["Body"].read())
        if not isinstance(value, dict):
            msg = f"Expected JSON object at {uri}"
            raise ValueError(msg)
        return value


def split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        msg = f"Invalid S3 URI: {uri}"
        raise ValueError(msg)
    return parsed.netloc, parsed.path.lstrip("/")


def is_missing_object(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}
