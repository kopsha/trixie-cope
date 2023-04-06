import pytest
from unittest.mock import patch, ANY
from cloud_uploader import (
    UploaderFactory,
    AmazonUploader,
    GoogleStorageUploader,
    FtpUploader,
    DEFAULT_TIMEOUT,
)


def test_UploaderFactory_make_invalid_uri():
    url = "any-junk-will-do"
    with pytest.raises(ValueError):
        UploaderFactory.make(url)


def test_UploaderFactory_make_valid_uri_without_folder():
    url = "mega://any-junk-will-do/"
    with pytest.raises(ValueError):
        UploaderFactory.make(url)


@patch("cloud_uploader.GoogleCloudClient", autospec=True)
def test_UploaderFactory_make_valid_gs_url(MockedClient):
    url = "gs://bucket-name/folder-name"
    uploader = UploaderFactory.make(url)
    assert uploader
    assert isinstance(uploader, GoogleStorageUploader)


@patch("cloud_uploader.GoogleCloudClient", autospec=True)
def test_GoogleStorageUploader_valid_uri_without_folder(MockedClient):
    url = "gs://bucket-name/"
    with pytest.raises(ValueError):
        GoogleStorageUploader(url)


@patch("cloud_uploader.GoogleCloudClient", autospec=True)
def test_GoogleStorageUploader_upload_from_stream_valid_uri(MockedClient):
    file_buffer = "any content is acceptable"
    url = "gs://bucket-name/folder"

    uploader = GoogleStorageUploader(url)
    MockedClient().bucket.assert_called_once_with("bucket-name")

    uploader.upload_from_stream("test.mp4", file_buffer)
    (
        MockedClient()
        .bucket("bucket-name")
        .blob("folder")
        .upload_from_file.assert_called_once_with(file_buffer, timeout=DEFAULT_TIMEOUT)
    )


def test_UploaderFactory_make_valid_ftp_url():
    url = "ftp://server:123/"
    uploader = UploaderFactory.make(url)
    assert uploader
    assert isinstance(uploader, FtpUploader)


def test_UploaderFactory_make_invalid_ftp_url():
    url = "ftp://server/123/"

    with pytest.raises(ValueError):
        UploaderFactory.make(url)


@patch("cloud_uploader.FTPClient", autospec=True)
def test_UploaderFactory_upload_from_stream_valid_uri(MockedClient):
    file_buffer = "any content is acceptable"
    url = "ftp://server:123/"
    uploader = FtpUploader(url)
    MockedClient.assert_called_once_with(user="", passwd="", timeout=DEFAULT_TIMEOUT)
    uploader.upload_from_stream("test.mp4", file_buffer)
    MockedClient().__enter__().connect.assert_called_once_with(
        host="server", port=123, timeout=DEFAULT_TIMEOUT
    )
    MockedClient().__enter__().login.assert_called_once_with(user="", passwd="")
    MockedClient().__enter__().storbinary.assert_called_once_with(
        "STOR test.mp4", file_buffer
    )


def test_UploaderFactory_make_valid_s3_url():
    url = "s3://bucket-name/folder-name"
    uploader = UploaderFactory.make(url)
    assert uploader
    assert isinstance(uploader, AmazonUploader)


def test_UploaderFactory_make_valid_s3_url_without_folder():
    url = "s3://bucket-name/"

    with pytest.raises(ValueError):
        UploaderFactory.make(url)


@patch("cloud_uploader.boto3_config")
@patch("cloud_uploader.boto3_session")
def test_AmazonUploader_upload_from_stream_valid_uri(MockSession, MockConfig):
    file_buffer = "any content is acceptable"
    url = "s3://bucket-name/folder"

    with patch.object(AmazonUploader, "SESSION", MockSession()):
        uploader = AmazonUploader(url)
        MockSession().client.assert_called_once_with("s3", config=MockConfig())

        uploader.upload_from_stream("test.mp4", file_buffer)
        (
            MockSession()
            .client("s3", config=MockConfig())
            .upload_fileobj.assert_called_once_with(
                file_buffer, "bucket-name", "folder/test.mp4", Config=ANY
            )
        )
