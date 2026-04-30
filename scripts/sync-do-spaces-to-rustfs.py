#!/usr/bin/env python3
"""Copy DigitalOcean Spaces assets into RustFS and verify object counts.

The script intentionally reads credentials from existing server-side .env files
and never prints secret values. Source buckets are mirrored to destination
buckets with the same name, preserving object keys.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


DEFAULT_WORKERBEE_ENV = "/home/dev/workerbee/.env"
DEFAULT_RUSTFS_ENV = "/home/dev/s3/.env"
DEFAULT_RUSTFS_ENDPOINT = "http://127.0.0.1:9100"
DEFAULT_SOURCE_REGION = "ams3"
DEFAULT_DEST_REGION = "us-east-1"
CHUNK_SIZE = 32 * 1024 * 1024


def load_env(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in Path(path).read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip().strip("\"'")
    return values


def s3_client(endpoint_url: str, access_key: str, secret_key: str, region: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 8, "mode": "standard"},
        ),
    )


def object_summary(client, bucket: str) -> dict[str, Any]:
    total_objects = 0
    total_bytes = 0
    top_prefixes: dict[str, dict[str, int]] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith("/"):
                continue
            size = int(obj.get("Size", 0))
            top_prefix = key.split("/", 1)[0] if "/" in key else "_root"
            prefix_summary = top_prefixes.setdefault(top_prefix, {"objects": 0, "bytes": 0})
            prefix_summary["objects"] += 1
            prefix_summary["bytes"] += size
            total_objects += 1
            total_bytes += size
    return {
        "objects": total_objects,
        "bytes": total_bytes,
        "top_prefixes": dict(sorted(top_prefixes.items())),
    }


def list_source_buckets(client, requested_bucket: str | None) -> list[str]:
    if requested_bucket:
        return [requested_bucket]
    try:
        buckets = [bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])]
    except ClientError:
        buckets = ["beehive"]
    return buckets


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError:
        client.create_bucket(Bucket=bucket)


def destination_matches(client, bucket: str, key: str, size: int, etag: str | None) -> bool:
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False
    if int(head.get("ContentLength", -1)) != size:
        return False
    destination_etag = str(head.get("ETag", "")).strip('"')
    source_etag = (etag or "").strip('"')
    return bool(source_etag and destination_etag == source_etag)


def copy_object(source, destination, bucket: str, key: str, size: int) -> None:
    head = source.head_object(Bucket=bucket, Key=key)
    body = source.get_object(Bucket=bucket, Key=key)["Body"]
    put_kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
    }
    if head.get("ContentType"):
        put_kwargs["ContentType"] = head["ContentType"]
    if head.get("CacheControl"):
        put_kwargs["CacheControl"] = head["CacheControl"]
    if head.get("Metadata"):
        put_kwargs["Metadata"] = head["Metadata"]
    if size >= CHUNK_SIZE:
        upload = destination.create_multipart_upload(**{
            key: value for key, value in put_kwargs.items() if key != "Body"
        })
        upload_id = upload["UploadId"]
        parts = []
        part_number = 1
        try:
            while True:
                chunk = body.read(CHUNK_SIZE)
                if not chunk:
                    break
                part = destination.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                part_number += 1
            destination.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            destination.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise
    else:
        destination.put_object(**put_kwargs)


def sync_bucket(source, destination, bucket: str, dry_run: bool) -> dict[str, Any]:
    ensure_bucket(destination, bucket)
    copied = 0
    skipped = 0
    bytes_copied = 0
    paginator = source.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith("/"):
                continue
            size = int(obj.get("Size", 0))
            etag = obj.get("ETag")
            if destination_matches(destination, bucket, key, size, etag):
                skipped += 1
                continue
            if not dry_run:
                copy_object(source, destination, bucket, key, size)
            copied += 1
            bytes_copied += size
    return {"copied": copied, "skipped": skipped, "bytes_copied": bytes_copied}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workerbee-env", default=DEFAULT_WORKERBEE_ENV)
    parser.add_argument("--rustfs-env", default=DEFAULT_RUSTFS_ENV)
    parser.add_argument("--rustfs-endpoint", default=DEFAULT_RUSTFS_ENDPOINT)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--count-only", action="store_true")
    args = parser.parse_args()

    workerbee_env = load_env(args.workerbee_env)
    rustfs_env = load_env(args.rustfs_env)
    source = s3_client(
        workerbee_env["DO_URL"],
        workerbee_env["DO_ACCESS_KEY_ID"],
        workerbee_env["DO_SECRET_ACCESS_KEY"],
        DEFAULT_SOURCE_REGION,
    )
    destination = s3_client(
        args.rustfs_endpoint,
        rustfs_env["RUSTFS_ACCESS_KEY"],
        rustfs_env["RUSTFS_SECRET_KEY"],
        DEFAULT_DEST_REGION,
    )

    buckets = list_source_buckets(source, args.bucket)
    before = {bucket: object_summary(source, bucket) for bucket in buckets}
    if args.count_only:
        print(json.dumps({"source": before}, indent=2, sort_keys=True))
        return 0

    sync = {bucket: sync_bucket(source, destination, bucket, args.dry_run) for bucket in buckets}
    after = {bucket: object_summary(destination, bucket) for bucket in buckets}
    mismatches = {
        bucket: {"source": before[bucket], "destination": after[bucket]}
        for bucket in buckets
        if before[bucket]["objects"] != after[bucket]["objects"]
        or before[bucket]["bytes"] != after[bucket]["bytes"]
    }
    result = {
        "dry_run": args.dry_run,
        "source": before,
        "destination": after,
        "sync": sync,
        "ok": not mismatches,
        "mismatches": mismatches,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not mismatches else 1


if __name__ == "__main__":
    sys.exit(main())
