"""Universal storage client"""
import re
from io import BufferedReader
from pathlib import Path
from abc import ABC, abstractmethod

from google.cloud import storage
from ftplib import FTP


DEFAULT_TIMEOUT = 8  # in seconds


class StorageClientInterface(ABC):

    @abstractmethod
    def upload_from_stream(self, name: str, stream: BufferedReader):
        pass


class GoogleStorageClient(StorageClientInterface):
    def __init__(self, destination: str) -> None:
        """Maybe we'll need some credentials too"""
        self._client = storage.Client()
        self._bucket = self._client.bucket(destination)

    def upload_from_stream(self, name: str, stream: BufferedReader):
        blob = self._bucket.blob(name)
        blob.upload_from_file(stream, timeout=DEFAULT_TIMEOUT)


class FtpClient:
    """The ftplib client is a mess"""
    SPLIT_CREDENTIALS = re.compile(
        r"^(?P<protocol>\w+)://(?:(?P<user>[^:]+):(?P<password>[^:]+)@)?"
        r"(?P<host>[\w\.\-]+)(?::(?P<port>\d+))?/?$"
    )

    def __init__(self, destination) -> None:
        match = self.SPLIT_CREDENTIALS.match(destination)
        if not match:
            raise ValueError("Please provide a valid upload URI")
    
        protocol = match.group("protocol")
        self.user = match.group("user") or ""
        self.password = match.group("password") or ""
        self.host = match.group("host")
        self.port = int(match.group("port"))

        if None in (protocol, self.user, self.password, self.host, self.port) or protocol != "ftp":
            raise ValueError(f"Please provide a fully qualified FTP URI ({protocol, self.user, self.password, self.host, self.port})")

        self._client = FTP(user=self.user, passwd=self.password, timeout=DEFAULT_TIMEOUT)

    def upload_from_stream(self, name: str, stream: BufferedReader):
        try:
            self._client.connect(host=self.host, port=self.port, timeout=DEFAULT_TIMEOUT)
            self._client.login(user=self.user, passwd=self.password)
            self._client.storbinary(f"STOR {name}", stream)
        except Exception as error:
            print("This happened", error)
        finally:
            self._client.close()
    

