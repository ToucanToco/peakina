import pytest

from peakina.io.s3.s3_fetcher import S3Fetcher


@pytest.fixture
def s3_fetcher(s3_endpoint_url):
    return S3Fetcher(client_kwargs={'endpoint_url': s3_endpoint_url})


def test_s3_fetcher_open(s3_fetcher):
    dirpath = 's3://accessKey1:verySecretKey1@mybucket'
    filepath = f'{dirpath}/0_0.csv'

    with s3_fetcher.open(filepath) as f:
        assert f.read() == b'a,b\n0,0\n0,1'


def test_s3_fetcher_listdir(s3_fetcher, mocker):
    s3_mtime_mock = mocker.patch('peakina.io.s3.s3_fetcher.s3_mtime')
    dirpath = 's3://accessKey1:verySecretKey1@mybucket'

    assert s3_fetcher.listdir(dirpath) == [
        '0_0.csv',
        '0_1.csv',
    ]
    assert s3_fetcher.mtime(f'{dirpath}/0_0.csv') > 0
    s3_mtime_mock.assert_not_called()


def test_s3_fetcher_mtime(s3_fetcher):
    dirpath = 's3://accessKey1:verySecretKey1@mybucket'
    filepath = f'{dirpath}/0_0.csv'

    assert s3_fetcher.mtime(filepath) > 0
