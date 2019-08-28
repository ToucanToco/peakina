import pandas as pd
import pytest

from peakina.io.http.http_fetcher import HttpFetcher


def test_http_fetcher(http_path, mocker):
    fetcher = HttpFetcher('')
    tmpfile = fetcher.open(http_path)
    assert pd.read_csv(tmpfile).shape == (800, 13)
    with pytest.raises(KeyError):
        fetcher.mtime(http_path)

    with pytest.raises(NotImplementedError):
        fetcher.listdir(http_path)

    stub_headers = {"last-modified": "Mon, 25 Jun 1984 11:22:33 GMT"}
    mocker.patch.object(fetcher.pool_manager, 'request').return_value.headers = stub_headers
    assert fetcher.mtime(http_path) == 457010553


def test_http_mtime_error(mocker):
    """It should return None as default value in case of unexpected exception"""
    fetcher = HttpFetcher('')
    mocker.patch.object(fetcher.pool_manager, 'request').side_effect = TimeoutError
    assert fetcher.mtime('') is None
