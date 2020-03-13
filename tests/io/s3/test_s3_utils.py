import io

import boto3
from pytest import fixture, raises

from peakina.io.s3.s3_utils import parse_s3_url as pu, s3_open


def test_parse_s3_url_no_credentials():
    """it should parse simple s3 urls without credentials"""
    assert pu('s3://mybucket/myobject.csv') == (None, None, 'mybucket', 'myobject.csv')
    assert pu('s3://mybucket/my/object.csv') == (None, None, 'mybucket', 'my/object.csv')


def test_parse_s3_url_with_credentials():
    """it should parse s3 url with (quoted) credentials"""
    assert pu('s3://ab:cd@mybucket/myobject.csv') == ('ab', 'cd', 'mybucket', 'myobject.csv')
    assert pu('s3://ab:cd@mybucket/my/object.csv') == ('ab', 'cd', 'mybucket', 'my/object.csv')
    assert pu('s3://a%3Ab:cd@mybucket/my/object.csv') == ('a:b', 'cd', 'mybucket', 'my/object.csv')


def test_invalid_scheme_raise_exception():
    """it should raise an exception on invalid scheme"""
    with raises(AssertionError):
        # invalid scheme
        pu('file:///foo.csv')


def test_invalid_credentials_raise_exception():
    """it should raise an exception on invalid credentials"""
    with raises(AssertionError):
        # missing access key
        pu('s3://:x@b/foo.csv')
    with raises(AssertionError):
        # missing secret key
        pu('s3://a:@b/foo.csv')
    with raises(AssertionError):
        # malformed credentials
        pu('s3://a@b/foo.csv')


def test_empty_object_name_raise_exception():
    """it should raise an exception on empty object name"""
    with raises(AssertionError):
        # invalid scheme
        pu('s3://a/')


def test_s3_open(mocker):
    fs_mock = mocker.patch('s3fs.S3FileSystem').return_value
    fs_mock.open.return_value = io.BytesIO(b'a,b\n0,1\n')
    tmpfile = s3_open('s3://mybucket/file.csv')
    assert tmpfile.name.endswith('.s3tmp')
    assert tmpfile.read() == b'a,b\n0,1\n'


@fixture(scope='module')
def s3_bucket(s3_container):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        aws_access_key_id='newAccessKey',
        aws_secret_access_key='newSecretKey',
        endpoint_url=f'http://localhost:{s3_container["port"]}',
    )
    s3_client.create_bucket(Bucket='mybucket')
    return s3_client


@fixture(scope='module')
def s3_container(service_container):
    # Return a docker container with a S3
    return service_container('s3')
