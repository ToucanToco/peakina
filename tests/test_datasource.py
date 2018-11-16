from pytest import raises

from peakina.datasource import DataSource


def test_simple_csv(path):
    """It should be able to detect type if not set"""
    ds = DataSource(path('0_0.csv'), encoding='utf8', sep=',')
    assert ds.get_df().shape == (2, 2)

    with raises(Exception):
        DataSource(path('0_0.csv'), type='excel', encoding='utf8', sep=',').get_df()


def test_csv_with_sep(path):
    """It should be able to detect separator if not set"""
    ds = DataSource(path('0_0_sep.csv'))
    assert ds.get_df().shape == (2, 2)

    ds = DataSource(path('0_0_sep.csv'), sep=',')
    assert ds.get_df().shape == (2, 1)


def test_csv_with_encoding(path):
    """It should be able to detect the encoding if not set"""
    ds = DataSource(path('latin_1.csv'))
    df = ds.get_df()
    assert df.shape == (2, 7)
    assert 'unité économique' in df.columns


def test_csv_with_sep_and_encoding(path):
    """It should be able to detect everything"""
    ds = DataSource(path('latin_1_sep.csv'))
    assert ds.get_df().shape == (2, 7)
