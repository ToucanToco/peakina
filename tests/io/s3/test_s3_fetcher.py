from contextlib import suppress

from pytest import raises

import peakina as pk
import boto3
from typing import BinaryIO
from peakina.io.s3.s3_fetcher import S3Fetcher
from peakina.io.s3.s3_utils import s3_read, parse_s3_url

from peakina.io.s3.s3_utils import s3_open, create_s3_client
from tests.io.s3.test_s3_utils import s3_bucket, s3_container
from collections import namedtuple


def test_s3_read(mocker, s3_bucket):
    bucket_name = parse_s3_url(filepath="s3://newAccessKey:newSecretKey@mybucket/0_0.csv")
    breakpoint()
    file = s3_read()
    body = file["Body"]
    return body.read()


def test_s3_fetcher(mocker, s3_bucket):
    dirpath = 's3://newAccessKey:newSecretKey@mybucket'
    filepath = f'{dirpath}/0_0.csv'

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
