import os

import pytest

fixtures_dir = f'{os.path.dirname(__file__)}/fixtures'


@pytest.fixture(scope='module')
def path():
    def f(filename):
        return f'{fixtures_dir}/{filename}'

    return f
