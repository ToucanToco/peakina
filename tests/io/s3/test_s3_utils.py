import io
from unittest.mock import MagicMock

from pytest import raises
from pytest_mock import MockerFixture

from peakina.io.s3.s3_utils import (
    _s3_open_file_with_retries,
    parse_s3_url as pu,
    s3_open,
)


def test_parse_s3_url_no_credentials():
    """it should parse simple s3 urls without credentials"""
    assert pu("s3://mybucket/myobject.csv") == (None, None, "mybucket", "myobject.csv")
    assert pu("s3://mybucket/my/object.csv") == (None, None, "mybucket", "my/object.csv")


def test_parse_s3_url_with_credentials():
    """it should parse s3 url with (quoted) credentials"""
    assert pu("s3://ab:cd@mybucket/myobject.csv") == ("ab", "cd", "mybucket", "myobject.csv")
    assert pu("s3://ab:cd@mybucket/my/object.csv") == ("ab", "cd", "mybucket", "my/object.csv")
    assert pu("s3://a%3Ab:cd@mybucket/my/object.csv") == ("a:b", "cd", "mybucket", "my/object.csv")


def test_invalid_scheme_raise_exception():
    """it should raise an exception on invalid scheme"""
    with raises(AssertionError):
        # invalid scheme
        pu("file:///foo.csv")


def test_invalid_credentials_raise_exception():
    """it should raise an exception on invalid credentials"""
    with raises(AssertionError):
        # missing access key
        pu("s3://:x@b/foo.csv")
    with raises(AssertionError):
        # missing secret key
        pu("s3://a:@b/foo.csv")
    with raises(AssertionError):
        # malformed credentials
        pu("s3://a@b/foo.csv")


def test_empty_object_name_raise_exception():
    """it should raise an exception on empty object name"""
    with raises(AssertionError):
        # invalid scheme
        pu("s3://a/")


def test_s3_open(mocker):
    fs_mock = mocker.patch("s3fs.S3FileSystem").return_value
    fs_mock.open.return_value = io.BytesIO(b"a,b\n0,1\n")
    tmpfile = s3_open("s3://my_key:my_secret@mybucket/file.csv")
    assert tmpfile.name.endswith(".s3tmp")
    assert tmpfile.read() == b"a,b\n0,1\n"


def test_s3_open_with_token(mocker: MockerFixture) -> None:
    tempfile_mock = MagicMock()
    mocker.patch("tempfile.NamedTemporaryFile", return_value=tempfile_mock)
    mocker.patch("peakina.io.s3.s3_utils._s3_open_file_with_retries")
    s3fs_file_system = mocker.patch("s3fs.S3FileSystem")
    s3fs_file_system.return_value.open.return_value = io.BytesIO(b"a,b\n0,1\n")

    # called with a session_token and something else
    s3_open(
        "s3://my_key:my_secret@mybucket/file.csv",
        client_kwargs={"something": "else", "session_token": "xxxx"},
    )
    s3fs_file_system.assert_called_once_with(
        token="xxxx", secret="my_secret", key="my_key", client_kwargs={"something": "else"}
    )

    # called with just the session token
    s3_open("s3://my_key:my_secret@mybucket/file.csv", client_kwargs={"session_token": "xxxx"})
    s3fs_file_system.assert_called_with(
        token="xxxx", secret="my_secret", key="my_key", client_kwargs=None
    )

    # called with an empty dict
    s3_open("s3://my_key:my_secret@mybucket/file.csv", client_kwargs={})
    s3fs_file_system.assert_called_with(
        secret="my_secret", key="my_key", token=None, client_kwargs={}
    )

    # called with None
    s3_open("s3://my_key:my_secret@mybucket/file.csv", client_kwargs=None)
    s3fs_file_system.assert_called_with(
        secret="my_secret", key="my_key", token=None, client_kwargs=None
    )


def test__s3_open_file_with_retries(mocker: MagicMock) -> None:
    s3fs_mock = MagicMock()
    side_effect = [
        Exception("Can't open one"),
        Exception("Can't open two"),
        Exception("Can't open three"),
        42,
    ]
    s3fs_mock.open.side_effect = side_effect
    path = "s3://my_key:my_secret@mybucket/file.csv"

    with raises(Exception, match=r"\(2 tries\): Can't open two"):
        _s3_open_file_with_retries(fs=s3fs_mock, path=path, retries=2)

    s3fs_mock.open.side_effect = side_effect
    with raises(Exception, match=r"\(3 tries\): Can't open three"):
        _s3_open_file_with_retries(fs=s3fs_mock, path=path, retries=3)

    assert _s3_open_file_with_retries(fs=s3fs_mock, path=path, retries=4) == 42
