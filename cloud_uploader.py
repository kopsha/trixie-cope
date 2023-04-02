"""Universal storage client"""
import re
from io import BufferedReader
from abc import ABC, abstractmethod
from functools import partial
import ujson

from google.cloud.storage import Client as GoogleCloudClient
from ftplib import FTP as ftp_client

from boto3 import client as boto3_client
from botocore.config import Config as boto3_config
from boto3.s3.transfer import TransferConfig as boto3_transfer_config


DEFAULT_TIMEOUT = 8  # in seconds
IO_CHUNKSIZE = 32_000  # should fit into one TCP packet


class UploaderInterface(ABC):
    @abstractmethod
    def __init__(self, destination: str) -> None:
        pass

    @abstractmethod
    def upload_from_stream(self, name: str, stream: BufferedReader):
        pass


class GoogleCloudUploader(UploaderInterface):
    def __init__(self, destination: str) -> None:
        """Maybe we'll need some credentials too"""
        self.client = GoogleCloudClient()
        self.bucket = self.client.bucket(destination)

    def upload_from_stream(self, name: str, stream: BufferedReader):
        blob = self.bucket.blob(name)
        blob.upload_from_file(stream, timeout=DEFAULT_TIMEOUT)


class AwsUploader(UploaderInterface):
    URI_PARSER = re.compile(r"^s3://(?P<bucket>[^/]+)/(?P<folder>[^/]+)/$")

    def __init__(self, destination: str) -> None:
        match = self.URI_PARSER.match(destination)
        if not match:
            raise ValueError("Please provide a valid S3 bucket URI")

        self.bucket_name = match.group("bucket")
        self.folder_name = match.group("folder")
        config = boto3_config(
            connect_timeout=DEFAULT_TIMEOUT,
            read_timeout=DEFAULT_TIMEOUT,
            max_pool_connections=1,
            s3=dict(use_accelerate_endpoint=False),
            retries=dict(max_attempts=1),
        )
        self.client = boto3_client("s3", config=config)
        self.transfer_config = boto3_transfer_config(use_threads=False, io_chunksize=IO_CHUNKSIZE)

    def upload_from_stream(self, name: str, stream: BufferedReader):
        self.client.upload_fileobj(stream, self.bucket_name, f"{self.folder_name}/{name}", Config=self.transfer_config)

class FtpClient(UploaderInterface):
    """The ftplib client is a mess, do not use it ever again"""

    SPLIT_CREDENTIALS = re.compile(
        r"^(?P<protocol>\w+)://(?:(?P<user>[^:]+):(?P<password>[^:]+)@)?"
        r"(?P<host>[\w\.\-]+)(?::(?P<port>\d+))?/?$"
    )

    def __init__(self, destination) -> None:
        match = self.SPLIT_CREDENTIALS.match(destination)
        if not match:
            raise ValueError("Please provide a valid upload URI")

        self.user = match.group("user") or ""
        self.password = match.group("password") or ""
        self.host = match.group("host")
        self.port = int(match.group("port"))
        uri_parts = (self.user, self.password, self.host, self.port)
        protocol = match.group("protocol")
        if None in uri_parts or protocol != "ftp":
            raise ValueError(f"Please provide a valid FTP URI ({protocol, *uri_parts})")

        self.client = ftp_client(
            user=self.user, passwd=self.password, timeout=DEFAULT_TIMEOUT
        )

    def upload_from_stream(self, name: str, stream: BufferedReader):
        with self.client as client:
            client.connect(host=self.host, port=self.port, timeout=DEFAULT_TIMEOUT)
            client.login(user=self.user, passwd=self.password)
            client.storbinary(f"STOR {name}", stream)
