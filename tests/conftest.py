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
