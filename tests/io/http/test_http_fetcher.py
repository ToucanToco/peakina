import pandas as pd
import pytest
from urllib3.exceptions import SSLError

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


def test_http_fetcher_kwargs(http_path, mocker):
    """It should pass fetcher_kwargs to `pool_manager.request`"""
    fetcher = HttpFetcher('')
    request_mock = mocker.patch.object(fetcher.pool_manager, 'request')
    fetcher.open(http_path, headers={'X-Foo': 'bar'})
    request_mock.assert_called_once_with(
        'GET', http_path, preload_content=False, headers={'X-Foo': 'bar'}
    )


def test_http_fetcher_verify(http_path):
    """It should check certificate and get results"""
    fetcher = HttpFetcher('', verify=True)
    tmpfile = fetcher.open(http_path)
    assert pd.read_csv(tmpfile).shape == (800, 13)


def test_http_fetcher_verify_no_ca_certs(http_path, path, mocker):
    """Make sure certificates are checked with `verify=True`

    Provide an empty set of root CA and make sure certifcate verification fails.
    """
    mocker.patch('certifi.where', return_value=path('dummy_cert.crt'))
    fetcher = HttpFetcher('', verify=True)
    with pytest.raises(SSLError) as err:
        fetcher.open(http_path, retries=False)
    assert 'CERTIFICATE_VERIFY_FAILED' in str(err.value)


def test_http_fetcher_no_verify(http_path):
    """It should not check certificate and still get results"""
    fetcher = HttpFetcher('', verify=False)
    tmpfile = fetcher.open(http_path)
    assert pd.read_csv(tmpfile).shape == (800, 13)
