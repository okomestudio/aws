from dataclasses import dataclass
from urllib.parse import urlparse

import boto3


@dataclass
class S3Path:
    bucket: str
    path: str

    @classmethod
    def from_url(cls, url):
        p = urlparse(url)
        if p.scheme != "s3":
            raise ValueError(f"{url} is not a proper S3 URL")
        return cls(p.netloc, p.path)

    def __str__(self):
        return f"s3://{self.bucket}/{self.path}"


def directory_exists(bucket, directory):
    directory = directory if directory.endswith("/") else directory + "/"
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=directory, MaxKeys=1)
    return "Contents" in resp and len(resp["Contents"])
