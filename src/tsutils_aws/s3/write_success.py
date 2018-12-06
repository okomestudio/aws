import logging
from argparse import ArgumentParser

import boto3

from .utils import directory_exists
from .utils import S3Path


log = logging.getLogger(__name__)


s3 = boto3.client("s3")


class S3ResourceError(Exception):
    """Raised on an error with an S3 resource."""


def write_success(s3path):
    bucket, key = s3path.bucket, s3path.key
    key = key if key.endswith("/") else key + "/"
    if not directory_exists(bucket, key):
        raise S3ResourceError(f"S3 directory {s3path} does not exist")
    s3.put_object(Bucket=bucket, Key=key + "_SUCCESS", Body=b"")


def _main(*paths):
    for path in paths:
        try:
            write_success(S3Path.from_url(path))
        except S3ResourceError as exc:
            log.warning(str(exc))


def main():
    p = ArgumentParser()
    p.add_argument("path", action="append", default=[])
    args = p.parse_args()
    _main(*args.path)
