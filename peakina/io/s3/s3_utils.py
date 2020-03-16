"""This module gathers misc convenience functions to handle s3 objects"""
import tempfile
from typing import BinaryIO, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import boto3
import s3fs
from botocore.exceptions import (
    ClientError,
    DataNotFoundError,
    NoCredentialsError,
    ParamValidationError,
)

S3_SCHEMES = ['s3', 's3n', 's3a', 'localhost']


def parse_s3_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """parses a s3 url and extract credentials and s3 object path.

    A S3 URL looks like s3://aws_key:aws_secret@bucketname/objectname where
    credentials are optional. Since credentials might include characters
    such as `/`, `@` or `#`, they have to be urlquoted in the url.

    Args:
        url (str): the s3 url

    Returns:
        tuple: (access_key, secret, bucketname, objectname). If credentials
        are not specified, `access_key` and `secret` are set to None.
    """
    urlchunks = urlparse(url)
    scheme = urlchunks.scheme
    assert scheme in S3_SCHEMES, f'{scheme} unsupported, use one of {S3_SCHEMES}'
    assert not urlchunks.params, f's3 url should not have params, got {urlchunks.params}'
    assert not urlchunks.query, f's3 url should not have query, got {urlchunks.query}'
    assert not urlchunks.fragment, f's3 url should not have fragment, got {urlchunks.fragment}'

    access_key: Optional[str] = None
    secret: Optional[str] = None

    # if either username or password is specified, we have credentials
    if urlchunks.username or urlchunks.password:
        # and they should both not be empty
        assert urlchunks.username, 's3 access key should not be empty'
        assert urlchunks.password, 's3 secret should not be empty'
        access_key = unquote(urlchunks.username)
        secret = unquote(urlchunks.password)
    objectname = urlchunks.path.lstrip('/')  # remove leading /, it's not part of the objectname

    return access_key, secret, urlchunks.hostname, objectname


def s3_open(url: str, **fetcher_kwargs) -> BinaryIO:
    """opens a s3 url and returns a file-like object"""
    access_key, secret, bucketname, objectname = parse_s3_url(url)
    fs = s3fs.S3FileSystem(
        anon=False, key=access_key, secret=secret, client_kwargs=fetcher_kwargs.get('client_kwargs')
    )
    ls = fs.ls(bucketname)
    ret = tempfile.NamedTemporaryFile(suffix='.s3tmp')
    file = fs.open(f'{bucketname}/{objectname}')
    ret.write(file.read())
    ret.seek(0)
    return ret


def create_s3_client(**kwargs):
    """
    Create s3_client

    Return:
        S3 client :: a low-level client representing Amazon Simple Storage Service (S3)

    More informations on how to configure client:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
    """
    try:
        session = boto3.session.Session()
        s3_client = session.client(service_name='s3', **kwargs,)
    except (NoCredentialsError, ClientError, ParamValidationError) as e:
        print(e)
    return s3_client


def s3_read(url: str, **kwargs) -> BinaryIO:
    """
    Read a file from a s3 path

    Arguments:
        url :: s3 path

    Return:
        file :: object data :: BinaryIO

    """

    aws_access_key, aws_secret, bucket_name, key = parse_s3_url(url)
    assert key, f"Cannot retrieve file without an empty path/key"
    s3 = create_s3_client(
        aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret, **kwargs
    )

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        response_body = response["Body"]
    except DataNotFoundError as e:
        print(e)
    return response_body.read()


def s3_list_dir(url: str, **kwargs) -> List:
    """
    List content on a s3 bucket

    Arguments:
        url :: s3 path

    Return:
        list_dir :: list of objects :: List
    """
    aws_access_key, aws_secret, bucket_name, object_name = parse_s3_url(url)
    s3 = create_s3_client(**kwargs)
    try:
        response = s3.list_objects(Bucket=bucket_name)
    except DataNotFoundError as e:
        print(e)
    return [file['Key'] for file in response['Contents']]


def s3_mtime(url: str, **kwargs) -> int:
    """
    Get the last modification of a S3 file

    Arguments:
        url :: s3 path :: str
        key :: Retrieve the 'keypath' or 'object' :: str

    Return:
        get object and return his last date of modification  :: int

    More informations:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
    """
    aws_access_key, aws_secret, bucket_name, key = parse_s3_url(url)
    assert key, f"Cannot retrieve file without an empty key in the url."
    s3 = create_s3_client(**kwargs)

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
    except DataNotFoundError as e:
        print(e)

    def datetime_to_timestamp(datetime):
        return datetime.timestamp()

    return int(datetime_to_timestamp(response['LastModified']))
