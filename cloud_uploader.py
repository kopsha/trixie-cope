"""Universal storage client"""
import re
from io import BufferedReader
from abc import ABC, abstractmethod

from google.cloud.storage import Client as GoogleCloudClient
from ftplib import FTP as FTPClient

from boto3.session import Session as boto3_session
from boto3.s3.transfer import TransferConfig as boto3_transfer_config
from botocore.config import Config as boto3_config


DEFAULT_TIMEOUT = 8  # in seconds
KB = 1024
IO_CHUNKSIZE = 32 * KB  # should fit into one TCP packet


class UploaderInterface(ABC):
    @abstractmethod
    def __init__(self, destination: str):
        pass

    @abstractmethod
    def upload_from_stream(self, name: str, stream: BufferedReader):
        pass


class GoogleStorageUploader(UploaderInterface):
    URI_PARSER = re.compile(r"^gs://(?P<bucket>[^/]+)/(?P<folder>[^/]+)/?$")

    def __init__(self, destination: str) -> None:
        is_uri = self.URI_PARSER.match(destination)
        if not is_uri:
            raise ValueError(f"Provided {destination} is not a valid GS URI")

        self.client = GoogleCloudClient()
        self.bucket = self.client.bucket(is_uri.group("bucket"))
        self.folder = is_uri.group("folder")

    def upload_from_stream(self, name: str, stream: BufferedReader):
        blob = self.bucket.blob(f"{self.folder}/{name}")
        blob.upload_from_file(stream, timeout=DEFAULT_TIMEOUT)


class AmazonUploader(UploaderInterface):
    URI_PARSER = re.compile(r"^s3://(?P<bucket>[^/]+)/(?P<folder>[^/]+)/?$")
    SESSION = boto3_session()

    def __init__(self, destination: str) -> None:
        is_uri = self.URI_PARSER.match(destination)
        if not is_uri:
            raise ValueError(f"Provided {destination} is not a valid S3 URI")

        self.bucket_name = is_uri.group("bucket")
        self.folder = is_uri.group("folder")

        config = boto3_config(
            connect_timeout=DEFAULT_TIMEOUT,
            read_timeout=DEFAULT_TIMEOUT,
            max_pool_connections=1,
            s3=dict(use_accelerate_endpoint=False),
            retries=dict(max_attempts=1),
        )
        self.client = self.SESSION.client("s3", config=config)

    def upload_from_stream(self, name: str, stream: BufferedReader):
        config = boto3_transfer_config(use_threads=False, io_chunksize=IO_CHUNKSIZE)
        self.client.upload_fileobj(
            stream, self.bucket_name, f"{self.folder}/{name}", Config=config
        )


class FtpUploader(UploaderInterface):
    """The ftplib client is a mess, do not use it ever again"""

    URI_PARSER = re.compile(
        r"^(?P<protocol>\w+)://(?:(?P<user>[^:]+):(?P<password>[^:]+)@)?"
        r"(?P<host>[\w\.\-]+)(?::(?P<port>\d+))?/?$"
    )

    def __init__(self, destination) -> None:
        is_uri = self.URI_PARSER.match(destination)
        if not is_uri:
            raise ValueError("Please provide a valid upload URI")

        self.user = is_uri.group("user") or ""
        self.password = is_uri.group("password") or ""
        self.host = is_uri.group("host")
        self.port = int(is_uri.group("port") or 0)

        self.client = FTPClient(
            user=self.user, passwd=self.password, timeout=DEFAULT_TIMEOUT
        )

    def upload_from_stream(self, name: str, stream: BufferedReader):
        with self.client as client:
            client.connect(host=self.host, port=self.port, timeout=DEFAULT_TIMEOUT)
            client.login(user=self.user, passwd=self.password)
            client.storbinary(f"STOR {name}", stream)


class UploaderFactory:
    BUILDERS = dict(
        # manually managed factory registry
        s3=AmazonUploader,
        gs=GoogleStorageUploader,
        ftp=FtpUploader,
    )
    URI_PARSER = re.compile(r"^(?P<protocol>\w+)://")

    @classmethod
    def make(cls, uri: str, **kwargs):
        is_uri = cls.URI_PARSER.match(uri)
        if not is_uri:
            raise ValueError(f"Provided {uri} is not a valid URI")

        key = is_uri.group("protocol")
        if key not in cls.BUILDERS:
            raise ValueError(f"Provided {key} is not supported")

        builder = cls.BUILDERS[key]
        return builder(destination=uri, **kwargs)
