from pytest import raises

from peakina.helpers import (
    UnknownType,
    bytes_head,
    detect_encoding,
    detect_sep,
    detect_type,
    guess_type,
    str_head,
    validate_encoding,
    validate_kwargs,
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
    assert str(e.value) == "Unknown detected type. Supported types are: 'csv', 'excel', 'json'."


def test_guess_type():
    assert guess_type('a.tsv', is_regex=False) == 'csv'
    with raises(UnknownType) as e:
        guess_type('a.tsv$', is_regex=False)
    assert str(e.value) == "Unknown guessed type None. Supported types are: 'csv', 'excel', 'json'."
    with raises(UnknownType):
        guess_type('a.jpg', is_regex=False)
    with raises(UnknownType):
        guess_type('a.jpg$', is_regex=True)
    assert guess_type('a.tsv', is_regex=True) == 'csv'
    assert guess_type('a.tsv$', is_regex=True) == 'csv'
    assert guess_type('a.*', is_regex=True) is None


def test_validate_kwargs():
    import pandas as pd

    assert validate_kwargs({'encoding': 'utf8'}, [pd.read_csv])
    with raises(ValueError) as e:
        validate_kwargs({'sheet_name': 0}, [pd.read_csv])
    assert str(e.value) == "Unsupported kwargs: 'sheet_name'"
    assert validate_kwargs({'sheet_name': 0}, [pd.read_csv, pd.read_excel])
