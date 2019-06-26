import os

import pytest

fixtures_dir = f'{os.path.dirname(__file__)}/fixtures'


@pytest.fixture(scope='module')
def path():
    def f(filename):
        return f'{fixtures_dir}/{filename}'

    return f


@pytest.fixture
def ftp_path():
    ftp = os.getenv('FTP_PATH')
    if not ftp:
        pytest.skip("'FTP_PATH' is not set")
    return ftp


@pytest.fixture(scope='module')
def http_path():
    return (
        'https://gist.githubusercontent.com/armgilles/'
        '194bcff35001e7eb53a2a8b441e8b2c6/raw/'
        '92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv'
    )
