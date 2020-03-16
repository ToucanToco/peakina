import os

from pytest import fixture, raises

from peakina.io.s3.s3_utils import create_s3_client, parse_s3_url as pu

_BUCKET_NAME = 'mybucket'
_PREFIX = 'localData/'


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


def test_s3_bucket(mocker, s3_bucket):
    s3_list_objects = s3_bucket.list_objects(Bucket='mybucket')
    assert s3_list_objects['ResponseMetadata']['HTTPStatusCode'] == 200
    s3_object = s3_bucket.get_object(Bucket='mybucket', Key='0_0.csv')
    s3_object['ResponseMetadata']['HTTPStatusCode'] == 200
    s3_object['Body'].read() == b'a,b\n0,0\n0,1'


@fixture(scope='module')
def s3_container(service_container):
    # Return a docker container with a S3
    return service_container('s3')


@fixture(scope='module')
def s3_bucket(s3_container):
    """
    Create an Amazon S3 bucket with a S3 client
    Arguments:
        s3_container -- docker service container
    """
    s3_bucket = create_s3_client(
        aws_access_key_id='newAccessKey',
        aws_secret_access_key='newSecretKey',
        endpoint_url=f'http://localhost:{s3_container["port"]}',
    )
    s3_bucket.create_bucket(Bucket='mybucket')

    def check_and_feed(s3_bucket):
        directory = os.getcwd() + '/tests/fixtures/'
        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            if filename.endswith(".csv"):
                s3_bucket.upload_file(f'{directory}{filename}', 'mybucket', filename)

    check_and_feed(s3_bucket)
    return s3_bucket
