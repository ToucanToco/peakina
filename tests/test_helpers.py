from pytest import raises

from peakina.helpers import (
    UnknownType,
    bytes_head,
    detect_encoding,
    detect_sep,
    detect_type,
    str_head,
    validate_encoding,
)


def test_bytes_head(path):
    assert bytes_head(path('0_0.csv'), 1) == b'a,b\n'
    assert bytes_head(path('0_0.csv'), 100) == b'a,b\n0,0\n0,1'


def test_str_head(path):
    assert str_head(path('0_0.csv'), 1) == 'a,b\n'
    assert str_head(path('0_0.csv'), 100) == 'a,b\n0,0\n0,1'
    with raises(UnicodeDecodeError):
        str_head(path('latin_1.csv'), 1)
    assert str_head(path('latin_1.csv'), 1, encoding='latin1')[:4] == 'Date'


def test_validate_encoding(path):
    assert validate_encoding(path('0_0.csv'), None)
    assert validate_encoding(path('0_0.csv'), 'utf8')
    assert not validate_encoding(path('latin_1.csv'), 'utf8')
    assert validate_encoding(path('latin_1.csv'), 'latin1')


def test_detect_encoding(path):
    assert detect_encoding(path('latin_1.csv')) == 'ISO-8859-1'


def test_detect_sep(path):
    assert detect_sep(path('0_0.csv')) == ','
    assert detect_sep(path('0_0_sep.csv')) == ';'


def test_detect_type(path):
    assert detect_type(path('0_0.csv')) == 'csv'
    with raises(UnknownType) as e:
        detect_type(path('fixture.xml'))
    assert str(e.value) == 'Unknown type. Supported types are: csv, excel, json'
