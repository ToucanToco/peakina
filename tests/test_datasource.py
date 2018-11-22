import pandas as pd
from pytest import fixture, raises

from peakina.datasource import DataSource, TypeEnum
from peakina.helpers import UnknownType


@fixture
def read_csv_spy(mocker):
    read_csv = mocker.spy(pd, 'read_csv')
    # need to mock the validation as the signature is changed via the spy
    mocker.patch('peakina.datasource.validate_kwargs', return_value=True)

    return read_csv


def test_scheme():
    """It should be able to set scheme"""
    assert DataSource('my/local/path/file.csv').scheme == ''
    assert DataSource('ftp://remote/path/file.csv').scheme == 'ftp'
    with raises(AttributeError) as e:
        DataSource('pika://wtf/did/I/write')
    assert str(e.value) == 'Unvalid scheme "pika"'


def test_type():
    """It should be able to set type if possible"""
    assert DataSource('myfile.csv').type is TypeEnum.CSV
    with raises(UnknownType):
        DataSource('myfile.csv$')
    assert DataSource('myfile.tsv$', match='glob').type is TypeEnum.CSV
    assert DataSource('myfile.*', match='glob').type is None


def test_validation_kwargs(mocker):
    """It should be able to validate the extra kwargs"""
    validatation_kwargs = mocker.patch('peakina.datasource.validate_kwargs')

    DataSource('myfile.csv')
    validatation_kwargs.assert_called_once_with({}, [pd.read_csv])
    validatation_kwargs.reset_mock()

    DataSource('myfile.*', match='glob')
    validatation_kwargs.assert_called_once_with({}, [pd.read_csv, pd.read_excel, pd.read_json])
    validatation_kwargs.reset_mock()


def test_simple_csv(path):
    """It should be able to detect type if not set"""
    ds = DataSource(path('0_0.csv'), extra_kwargs={'encoding': 'utf8', 'sep': ','})
    assert ds.get_df().shape == (2, 2)

    with raises(Exception):
        DataSource(path('0_0.csv'), type='excel', encoding='utf8', sep=',').get_df()


def test_csv_with_sep(path):
    """It should be able to detect separator if not set"""
    ds = DataSource(path('0_0_sep.csv'))
    assert ds.get_df().shape == (2, 2)

    ds = DataSource(path('0_0_sep.csv'), extra_kwargs={'sep': ','})
    assert ds.get_df().shape == (2, 1)


def test_csv_with_encoding(path):
    """It should be able to detect the encoding if not set"""
    df = DataSource(path('latin_1.csv')).get_df()
    assert df.shape == (2, 7)
    assert 'unité économique' in df.columns


def test_csv_with_sep_and_encoding(path):
    """It should be able to detect everything"""
    ds = DataSource(path('latin_1_sep.csv'))
    assert ds.get_df().shape == (2, 7)


def test_match(path):
    """It should be able to concat files matching a pattern"""
    ds = DataSource(path(r'0_\d.csv'), match='regex')  # 0_0.csv, 0_1.csv
    assert ds.get_df().shape == (4, 2)


def test_match_different_file_types(path):
    """It should be able to match even different types, encodings or seps"""
    ds = DataSource(path('0_*'), match='glob')
    assert ds.get_df().shape == (8, 2)  # 0_0.csv, 0_0_sep.csv, 0_1.csv, 0_2.xls
