from contextlib import suppress

from pytest import raises

import peakina as pk
from peakina.io.s3.s3_fetcher import S3Fetcher


def test_s3_fetcher(mocker):
    dirpath = 's3://aws_key:aws_secret@bucketname'
    filepath = f'{dirpath}/objectname'

    # TODO: Test on real s3 like ftp
    mocker.patch('peakina.io.s3.s3_fetcher.s3_open').return_value = 'pika'
    assert S3Fetcher.open(filepath) == 'pika'

    with raises(NotImplementedError):
        S3Fetcher.listdir(dirpath)
    with raises(NotImplementedError):
        S3Fetcher.mtime(filepath)


def test_s3_fetcher_custom_endpoint(mocker):

    m = mocker.patch('peakina.io.s3.s3_utils.s3fs.S3FileSystem')

    dirpath = 's3://aws_key:aws_secret@bucketname'
    filepath = f'{dirpath}/objectname.csv'
    with suppress(TypeError):
        pk.read_pandas(
            filepath,
            fetcher_kwargs={"client_kwargs": {"endpoint_url": "https://internal.domain:8080/truc"}},
        )

    kwargs = m.call_args[1]
    assert 'key' in kwargs
    assert 'secret' in kwargs
    assert 'client_kwargs' in kwargs
    assert 'endpoint_url' in kwargs['client_kwargs']
