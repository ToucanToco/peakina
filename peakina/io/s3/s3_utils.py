"""This module gathers misc convenience functions to handle s3 objects"""
import re
import tempfile
from typing import Any, Dict, Optional, Tuple
from typing.io import BinaryIO
from urllib.parse import unquote, urlparse

import s3fs

S3_SCHEMES = ['s3', 's3n', 's3a']


def parse_s3_url(url: str, file=True) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
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
    if file:
        assert objectname, "s3 objectname can't be empty"

    return access_key, secret, urlchunks.hostname, objectname


def s3_open(url: str, *, client_kwargs: Optional[Dict[str, Any]] = None) -> BinaryIO:
    """opens a s3 url and returns a file-like object"""
    access_key, secret, bucketname, objectname = parse_s3_url(url)
    fs = s3fs.S3FileSystem(key=access_key, secret=secret, client_kwargs=client_kwargs)
    ret = tempfile.NamedTemporaryFile(suffix='.s3tmp')
    file = fs.open(f'{bucketname}/{objectname}')
    ret.write(file.read())
    ret.seek(0)
    return ret


def s3_mtime(url: str, *, client_kwargs: Optional[Dict[str, Any]] = None) -> int:
    access_key, secret, bucketname, objectname = parse_s3_url(url, file=True)
    fs = s3fs.S3FileSystem(key=access_key, secret=secret, client_kwargs=client_kwargs)
    return fs.info(f'{bucketname}/{objectname}')['LastModified'].timestamp()


def dir_mtimes(url: str, *, client_kwargs: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
    access_key, secret, bucketname, objectname = parse_s3_url(url, file=False)
    fs = s3fs.S3FileSystem(key=access_key, secret=secret, client_kwargs=client_kwargs)
    return {
        re.sub(rf'^{bucketname}/', '', x['name']): x['LastModified'].timestamp()
        for x in fs.listdir(bucketname)
    }
