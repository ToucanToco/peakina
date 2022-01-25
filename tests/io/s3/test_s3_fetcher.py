from typing import Any, Dict

import boto3
import pytest
from s3fs import S3FileSystem

from peakina.io.s3.s3_fetcher import S3Fetcher


@pytest.fixture
def s3_fetcher(s3_endpoint_url):
    return S3Fetcher(client_kwargs={"endpoint_url": s3_endpoint_url})


def test_s3_fetcher_open(s3_fetcher):
    dirpath = "s3://accessKey1:verySecretKey1@mybucket"
    filepath = f"{dirpath}/0_0.csv"

    with s3_fetcher.open(filepath) as f:
        assert f.read() == b"a,b\n0,0\n0,1"


def test_s3_fetcher_listdir(s3_fetcher, mocker):
    s3_mtime_mock = mocker.patch("peakina.io.s3.s3_fetcher.s3_mtime")
    dirpath = "s3://accessKey1:verySecretKey1@mybucket"

    assert s3_fetcher.listdir(dirpath) == [
        "0_0.csv",
        "0_1.csv",
        "mydir",
    ]

    assert s3_fetcher.mtime(f"{dirpath}/0_0.csv") > 0
    assert s3_fetcher.mtime(f"{dirpath}/mydir") is None
    s3_mtime_mock.assert_not_called()


def test_s3_fetcher_mtime(s3_fetcher):
    dirpath = "s3://accessKey1:verySecretKey1@mybucket"
    filepath = f"{dirpath}/0_0.csv"

    assert s3_fetcher.mtime(filepath) > 0


def test_s3_fetcher_open_retry(s3_fetcher, s3_endpoint_url, mocker):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        aws_access_key_id="accessKey1",
        aws_secret_access_key="verySecretKey1",
        endpoint_url=s3_endpoint_url,
    )
    dirpath = "s3://accessKey1:verySecretKey1@mybucket"
    filepath = f"{dirpath}/for_retry_0_0.csv"
    s3_client.upload_file("tests/fixtures/for_retry_0_0.csv", "mybucket", "for_retry_0_0.csv")

    class S3FileSystemThatFailsOpen(S3FileSystem):
        def __init__(self, key: str, secret: str, client_kwargs: Dict[str, Any]) -> None:
            super().__init__(key=key, secret=secret, client_kwargs=client_kwargs)
            self.invalidated_cache = False

        def open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
            if not self.invalidated_cache:
                raise Exception()
            return super().open(path, mode, block_size, cache_options, **kwargs)

        def invalidate_cache(self, path=None):
            self.invalidated_cache = True

    mocker.patch("peakina.io.s3.s3_utils.s3fs.S3FileSystem", S3FileSystemThatFailsOpen)

    with s3_fetcher.open(filepath) as f:
        assert f.read() == b"a,b\n0,0\n0,1"
    s3_client.delete_object(Bucket="mybucket", Key="tests/fixtures/for_retry_0_0.csv")
