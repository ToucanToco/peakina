import pytest

from peakina.helpers import (
    bytes_head,
    detect_encoding,
    detect_sep,
    detect_type,
    mdtm_to_string,
    pd_read,
    str_head,
    validate_encoding,
    validate_kwargs,
    validate_sep,
)


def test_detect_type_no_regex():
    """It should find the right type of a file and raise an exception if not supported"""
    assert detect_type('file.csv') == 'csv'
    assert detect_type('file.tsv') == 'csv'
    assert detect_type('file.xml') == 'xml'
    with pytest.raises(ValueError) as e:
        detect_type('file.doc')
    assert (
        str(e.value) == "Unsupported mimetype 'application/msword'. "
        "Supported types are: 'csv', 'excel', 'json', 'parquet', 'xml'."
    )
    with pytest.raises(ValueError):
        detect_type('file*.csv$')
    with pytest.raises(ValueError):
        detect_type('file*')


def test_detect_type_with_regex():
    """It should find the type of a regex and not raise an error if it coulnd't be guessed"""
    assert detect_type('file*.csv$', is_regex=True) == 'csv'
    with pytest.raises(ValueError):
        detect_type('file*.doc$', is_regex=True)
    assert detect_type('file*', is_regex=True) is None


def test_bytes_head(path):
    """It should get the first lines of a file as bytes"""
    assert bytes_head(path('0_0.csv'), 1) == b'a,b\n'
    assert bytes_head(path('0_0.csv'), 100) == b'a,b\n0,0\n0,1'


def test_str_head(path):
    """It should get the first lines of a file as string"""
    assert str_head(path('0_0.csv'), 1) == 'a,b\n'
    assert str_head(path('0_0.csv'), 100) == 'a,b\n0,0\n0,1'
    with pytest.raises(UnicodeDecodeError):
        str_head(path('latin_1.csv'), 1)
    assert str_head(path('latin_1.csv'), 1, encoding='latin1')[:4] == 'Date'


def test_detect_encoding(path):
    """It should detect the proper encoding"""
    assert detect_encoding(path('latin_1.csv')) == 'ISO-8859-1'


def test_validate_encoding(path):
    """It should validate if an encoding seems good"""
    assert validate_encoding(path('0_0.csv'))
    assert validate_encoding(path('0_0.csv'), 'utf8')
    assert not validate_encoding(path('latin_1.csv'), 'utf8')
    assert validate_encoding(path('latin_1.csv'), 'latin1')


def test_detect_sep(path):
    """It should detect the right separator of a CSV file"""
    assert detect_sep(path('0_0.csv')) == ','
    assert detect_sep(path('0_0_sep.csv')) == ';'


def test_validate_sep(path):
    """It should validate if a separator seems good"""
    assert validate_sep(path('0_0.csv'))
    assert not validate_sep(path('0_0.csv'), ';')
    assert not validate_sep(path('latin_1_sep.csv'), ',', 'latin1')
    assert validate_sep(path('latin_1_sep.csv'), ';', 'latin1')


def test_validate_sep_error(path):
    """It should return discard the separator in case of parsing errors"""
    assert not validate_sep(path('sep_parse_error.csv'))


def test_validate_kwargs():
    """It should raise an error if at least one kwarg is not in one of the methods"""
    assert validate_kwargs({'encoding': 'utf8'}, 'csv')
    with pytest.raises(ValueError) as e:
        validate_kwargs({'sheet_name': 0}, 'csv')
    assert str(e.value) == "Unsupported kwargs: 'sheet_name'"
    assert validate_kwargs({'sheet_name': 0}, None)
    assert validate_kwargs({'keep_default_na': False, 'encoding': 'utf-8', 'decimal': '.'}, 'excel')
    assert validate_kwargs({'filter': '.'}, 'xml')


def test_mdtm_to_string():
    """It should convert a timestamp as an iso string"""
    assert mdtm_to_string(0) == '1970-01-01T00:00:00Z'


def test_pd_read(path):
    """It should call the right pandas method for reading file"""
    assert pd_read(path('0_0.csv'), 'csv', kwargs={}).shape == (2, 2)
    assert pd_read(path('fixture.xml'), 'xml', kwargs={}).shape == (1, 1)
