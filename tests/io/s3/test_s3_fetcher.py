import boto3
from pytest import fixture, raises

from peakina.io.s3.s3_fetcher import S3Fetcher


@fixture(scope='module')
def s3_container(service_container):
    def check(host_port):
        session = boto3.session.Session()
        s3_url = f'http://localhost:{host_port}'
        s3_client = session.client(
            service_name='s3',
            aws_access_key_id='accessKey1',
            aws_secret_access_key='verySecretKey1',
            endpoint_url=s3_url,
        )
        s3_client.list_buckets()

    return service_container('s3', check)


@fixture(scope='module')
def s3_endpoint_url(s3_container):
    session = boto3.session.Session()
    s3_url = f'http://localhost:{s3_container["port"]}'
    s3_client = session.client(
        service_name='s3',
        aws_access_key_id='accessKey1',
        aws_secret_access_key='verySecretKey1',
        endpoint_url=s3_url,
    )
    s3_client.create_bucket(Bucket='mybucket')
    s3_client.upload_file('tests/fixtures/0_0.csv', 'mybucket', '0_0.csv')
    s3_client.upload_file('tests/fixtures/0_1.csv', 'mybucket', '0_1.csv')
    return s3_url


def test_s3_fetcher(s3_endpoint_url):
    dirpath = 's3://accessKey1:verySecretKey1@mybucket'
    filepath = f'{dirpath}/0_0.csv'

    with S3Fetcher.open(filepath, client_kwargs={'endpoint_url': s3_endpoint_url}) as f:
        assert f.read() == b'a,b\n0,0\n0,1'

    with raises(NotImplementedError):
        S3Fetcher.listdir(dirpath)
    with raises(NotImplementedError):
        S3Fetcher.mtime(filepath)
