from pytest import raises

from peakina.datasource import DataSource


def test_scheme():
    """It should be able to set scheme"""
    assert DataSource('my/local/path/file.csv').scheme == ''
    assert DataSource('ftp://remote/path/file.csv').scheme == 'ftp'
    with raises(AttributeError) as e:
        DataSource('pika://wtf/did/I/write')
    assert str(e.value) == 'Unvalid scheme "pika"'


def test_simple_csv(path):
    """It should be able to detect type if not set"""
    ds = DataSource(path('0_0.csv'), encoding='utf8', sep=',')
    assert ds.df.shape == (2, 2)

    with raises(Exception):
        DataSource(path('0_0.csv'), type='excel', encoding='utf8', sep=',').df()


def test_csv_with_sep(path):
    """It should be able to detect separator if not set"""
    ds = DataSource(path('0_0_sep.csv'))
    assert ds.df.shape == (2, 2)

    ds = DataSource(path('0_0_sep.csv'), sep=',')
    assert ds.df.shape == (2, 1)


def test_csv_with_encoding(path):
    """It should be able to detect the encoding if not set"""
    df = DataSource(path('latin_1.csv')).df
    assert df.shape == (2, 7)
    assert 'unité économique' in df.columns


def test_csv_with_sep_and_encoding(path):
    """It should be able to detect everything"""
    ds = DataSource(path('latin_1_sep.csv'))
    assert ds.df.shape == (2, 7)
