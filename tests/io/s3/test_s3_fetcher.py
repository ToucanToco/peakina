from contextlib import suppress
import peakina as pk
from peakina.io.s3.s3_fetcher import S3Fetcher
from tests.io.s3.test_s3_utils import s3_bucket, s3_container
from peakina.io.s3.s3_utils import s3_list_dir, s3_mtime, s3_read, s3_open


def test_s3_read(mocker, s3_bucket):
    # it should read the content of a file
    file = s3_read(
        url='s3://newAccessKey:newSecretKey@mybucket/0_0.csv', endpoint_url=s3_bucket._endpoint.host
    )
    assert file == b'a,b\n0,0\n0,1'


def test_s3_list_dir(mocker, s3_bucket):
    ## it should return the list of elements presents on the s3_bucket
    directory = s3_list_dir(
        url='s3://newAccessKey:newSecretKey@mybucket', endpoint_url=s3_bucket._endpoint.host
    )
    assert [
        '0_0.csv',
        '0_0_sep.csv',
        '0_1.csv',
        'empty.csv',
        'fixture-1.csv',
        'fixture-1.staging.csv',
        'fixture-2.csv',
        'fixture.csv',
        'fixture_empty.csv',
        'fixturesep.csv',
        'latin_1.csv',
        'latin_1_sep.csv',
        'sep_parse_error.csv',
    ] == directory


def test_s3_mtime(mocker, s3_bucket):
    # it should return the last modification date of a file in timestamp format
    file_mtime = s3_mtime(
        url='s3://newAccessKey:newSecretKey@mybucket/0_0.csv', endpoint_url=s3_bucket._endpoint.host
    )
    assert isinstance(file_mtime, int)


def test_s3_fetcher(mocker, s3_bucket):
    dirpath = 's3://newAccessKey:newSecretKey@mybucket'
    filepath = f'{dirpath}/0_0.csv'
    file = s3_read(
        url='s3://newAccessKey:newSecretKey@mybucket/0_0.csv', endpoint_url=s3_bucket._endpoint.host
    )
    assert file == b'a,b\n0,0\n0,1'
    directory = S3Fetcher.listdir(dirpath, endpoint_url=s3_bucket._endpoint.host)
    assert [
        '0_0.csv',
        '0_0_sep.csv',
        '0_1.csv',
        'empty.csv',
        'fixture-1.csv',
        'fixture-1.staging.csv',
        'fixture-2.csv',
        'fixture.csv',
        'fixture_empty.csv',
        'fixturesep.csv',
        'latin_1.csv',
        'latin_1_sep.csv',
        'sep_parse_error.csv',
    ] == directory

    file_mtime = S3Fetcher.mtime(filepath, endpoint_url=s3_bucket._endpoint.host)
    assert isinstance(file_mtime, int)


def test_s3_fetcher_custom_endpoint(mocker, s3_bucket):
    dirpath = 's3://newAccessKey:newSecretKey@mybucket'
    filepath = f'{dirpath}/0_0.csv'
    file = s3_open(
        url='s3://newAccessKey:newSecretKey@mybucket/0_0.csv',
        client_kwargs={'endpoint_url': s3_bucket._endpoint.host},
    )

    pk.read_pandas(uri=filepath, fetcher_kwargs={'endpoint_url': s3_bucket._endpoint.host})

    kwargs = file.call_args[1]
    assert 'key' in kwargs
    assert 'secret' in kwargs
    assert 'client_kwargs' in kwargs
    assert 'endpoint_url' in kwargs['client_kwargs']
