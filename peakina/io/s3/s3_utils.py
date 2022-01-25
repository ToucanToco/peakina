"""This module gathers misc convenience functions to handle s3 objects"""
import logging
import re
import tempfile
from time import sleep
from typing import IO, Any, Dict, Optional, Tuple, cast
from urllib.parse import unquote, urlparse

import s3fs

S3_SCHEMES = ["s3", "s3n", "s3a"]

logger = logging.getLogger(__name__)


def parse_s3_url(
    url: str, file: bool = True
) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    """parses a s3 url and extract credentials and s3 object path.

    A S3 URL looks like s3://aws_key:aws_secret@bucketname/objectname where
    credentials are optional. Since credentials might include characters
    such as `/`, `@` or `#`, they have to be urlquoted in the url.

    Args:
        url (str): the s3 url
        file (bool): whether or not the url is a file url or a directory one

    Returns:
        tuple: (access_key, secret, bucketname, objectname). If credentials
        are not specified, `access_key` and `secret` are set to None.
    """
    urlchunks = urlparse(url)
    scheme = urlchunks.scheme
    assert scheme in S3_SCHEMES, f"{scheme} unsupported, use one of {S3_SCHEMES}"
    assert not urlchunks.params, f"s3 url should not have params, got {urlchunks.params}"
    assert not urlchunks.query, f"s3 url should not have query, got {urlchunks.query}"
    assert not urlchunks.fragment, f"s3 url should not have fragment, got {urlchunks.fragment}"

    access_key: Optional[str] = None
    secret: Optional[str] = None

    # if either username or password is specified, we have credentials
    if urlchunks.username or urlchunks.password:
        # and they should both not be empty
        assert urlchunks.username, "s3 access key should not be empty"
        assert urlchunks.password, "s3 secret should not be empty"
        access_key = unquote(urlchunks.username)
        secret = unquote(urlchunks.password)
    objectname = urlchunks.path.lstrip("/")  # remove leading /, it's not part of the objectname
    if file:
        assert objectname, "s3 objectname can't be empty"

    return access_key, secret, urlchunks.hostname, objectname


def _s3_open_file_with_retries(fs: s3fs.S3FileSystem, path: str, retries: int) -> Any:
    for _ in range(retries):
        try:
            logger.info(f"opening {path}")
            file = fs.open(path)
            return file
        except Exception as ex:
            logger.warning(f"could not open {path}: {ex}")
            # if the file has just been uploaded, then it might not be visible immediatly
            # but the fail to open has been cached by s3fs
            # so, we invalidate the cache
            fs.invalidate_cache(path)
            # and we give some time to S3 to settle the file status
            sleep(1)


def s3_open(url: str, *, client_kwargs: Optional[Dict[str, Any]] = None) -> IO[bytes]:
    """opens a s3 url and returns a file-like object"""
    access_key, secret, bucketname, objectname = parse_s3_url(url)
    fs = s3fs.S3FileSystem(key=access_key, secret=secret, client_kwargs=client_kwargs)
    path = f"{bucketname}/{objectname}"
    ret = tempfile.NamedTemporaryFile(suffix=".s3tmp")
    file = _s3_open_file_with_retries(fs, path, 3)
    ret.write(file.read())
    ret.seek(0)
    file.close()
    return ret


def _get_timestamp(obj: Dict[str, Any]) -> Optional[int]:
    try:
        return cast(int, obj["LastModified"].timestamp())
    except KeyError:
        return None


def s3_mtime(url: str, *, client_kwargs: Optional[Dict[str, Any]] = None) -> Optional[int]:
    access_key, secret, bucketname, objectname = parse_s3_url(url, file=True)
    fs = s3fs.S3FileSystem(key=access_key, secret=secret, client_kwargs=client_kwargs)
    return _get_timestamp(fs.info(f"{bucketname}/{objectname}"))


def dir_mtimes(
    dirpath: str, *, client_kwargs: Optional[Dict[str, Any]] = None
) -> Dict[str, Optional[int]]:
    access_key, secret, bucketname, objectname = parse_s3_url(dirpath, file=False)
    fs = s3fs.S3FileSystem(key=access_key, secret=secret, client_kwargs=client_kwargs)
    # objectname can be empty or not ('subdir1/subdir2')
    bucketdir = f"{bucketname}/{objectname}".rstrip("/")
    return {
        re.sub(rf"^{bucketdir}/", "", x["name"]): _get_timestamp(x) for x in fs.listdir(bucketdir)
    }
