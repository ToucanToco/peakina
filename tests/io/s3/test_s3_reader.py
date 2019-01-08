from pytest import raises

from peakina.io.s3.s3_reader import S3Reader


def test_s3_reader(mocker):
    dirpath = 's3://aws_key:aws_secret@bucketname'
    filepath = f'{dirpath}/objectname'

    # TODO: Test on real s3 like ftp
    mocker.patch('peakina.io.s3.s3_reader.s3_open').return_value = 'pika'
    assert S3Reader.open(filepath) == 'pika'

    with raises(NotImplementedError):
        S3Reader.listdir(dirpath)
    with raises(NotImplementedError):
        S3Reader.mtime(filepath)
