from contextlib import suppress

from pytest import raises

import peakina as pk
import boto3
from typing import BinaryIO
from peakina.io.s3.s3_fetcher import S3Fetcher
from peakina.io.s3.s3_utils import s3_read, parse_s3_url, s3_list_dir, s3_mtime
from peakina.io.s3.s3_utils import s3_open, create_s3_client
from tests.io.s3.test_s3_utils import s3_bucket, s3_container
from collections import namedtuple


def test_s3_read(mocker, s3_bucket):
    file = s3_read(
        url='s3://newAccessKey:newSecretKey@mybucket/0_0.csv', endpoint_url=s3_bucket._endpoint.host
    )
    file == b'a,b\n0,0\n0,1'


def test_s3_list_dir(mocker, s3_bucket):
    directory = s3_list_dir(
        url='s3://newAccessKey:newSecretKey@mybucket', endpoint_url=s3_bucket._endpoint.host
    )
    assert len(directory) >= 13


def test_s3_mtime(mocker, s3_bucket):
    file_timestamp = s3_mtime(
        url='s3://newAccessKey:newSecretKey@mybucket/0_0.csv', endpoint_url=s3_bucket._endpoint.host
    )
    assert isinstance(file_timestamp, int)


def test_s3_fetcher(mocker, s3_bucket):
    dirpath = 's3://newAccessKey:newSecretKey@mybucket'
    filepath = f'{dirpath}/0_0.csv'
    file = s3_read(
        url='s3://newAccessKey:newSecretKey@mybucket/0_0.csv', endpoint_url=s3_bucket._endpoint.host
    )
    breakpoint()
    directory = S3Fetcher.listdir(dirpath, endpoint_url=s3_bucket._endpoint.host)
    timestamp = S3Fetcher.mtime(filepath, endpoint_url=s3_bucket._endpoint.host)


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
