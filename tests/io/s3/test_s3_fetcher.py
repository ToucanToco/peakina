from peakina.io.s3.s3_fetcher import S3Fetcher


def test_s3_fetcher_open(s3_endpoint_url):
    dirpath = 's3://accessKey1:verySecretKey1@mybucket'
    filepath = f'{dirpath}/0_0.csv'

    with S3Fetcher.open(filepath, client_kwargs={'endpoint_url': s3_endpoint_url}) as f:
        assert f.read() == b'a,b\n0,0\n0,1'


def test_s3_fetcher_listdir(s3_endpoint_url, mocker):
    s3_mtime_mock = mocker.patch('peakina.io.s3.s3_fetcher.s3_mtime')
    dirpath = 's3://accessKey1:verySecretKey1@mybucket'

    s3_fetcher = S3Fetcher('')

    assert s3_fetcher.listdir(dirpath, client_kwargs={'endpoint_url': s3_endpoint_url}) == [
        '0_0.csv',
        '0_1.csv',
    ]
    assert (
        s3_fetcher.mtime(f'{dirpath}/0_0.csv', client_kwargs={'endpoint_url': s3_endpoint_url}) > 0
    )
    s3_mtime_mock.assert_not_called()


def test_s3_fetcher_mtime(s3_endpoint_url):
    dirpath = 's3://accessKey1:verySecretKey1@mybucket'
    filepath = f'{dirpath}/0_0.csv'

    assert S3Fetcher('').mtime(filepath, client_kwargs={'endpoint_url': s3_endpoint_url}) > 0
