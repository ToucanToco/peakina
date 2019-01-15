from pytest import raises

from peakina.io.s3.s3_fetcher import S3Fetcher


def test_s3_fetcher(mocker):
    dirpath = 's3://aws_key:aws_secret@bucketname'
    filepath = f'{dirpath}/objectname'

    # TODO: Test on real s3 like ftp
    mocker.patch('peakina.io.s3.s3_fetcher.s3_open').return_value = 'pika'
    assert S3Fetcher.open(filepath) == 'pika'

    with raises(NotImplementedError):
        S3Fetcher.listdir(dirpath)
    with raises(NotImplementedError):
        S3Fetcher.mtime(filepath)
