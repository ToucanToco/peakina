import time
from datetime import timedelta

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal
from pytest_cases import THIS_MODULE, CaseData, CaseDataGetter, cases_data

from peakina.cache import Cache, CacheEnum


@pytest.fixture
def df_test():
    return pd.DataFrame({'x': [0, 1, 2, 3], 'y': ['a', 'b', 'c', 'd']})


def test_inmemory_cache(df_test):
    """two inmemory caches are independant"""
    c1 = Cache.get_cache(CacheEnum.MEMORY)
    c2 = Cache.get_cache(CacheEnum.MEMORY)

    c1.set('key', df_test)
    assert_frame_equal(c1.get('key'), df_test)
    with pytest.raises(KeyError):
        c2.get('key')

    c1.delete('key')
    with pytest.raises(KeyError):
        c1.get('key')


def test_hdf_cache(mocker, tmp_path, df_test):
    """two hdf caches pointing to the same directory are equivalent"""
    c1 = Cache.get_cache(CacheEnum.HDF, cache_dir=tmp_path)
    c2 = Cache.get_cache(CacheEnum.HDF, cache_dir=tmp_path)

    c1.set('key', df_test)
    assert c1.get_metadata().shape == (1, 3)
    assert_frame_equal(c1.get('key'), df_test)
    assert_frame_equal(c2.get('key'), df_test)
    assert_frame_equal(c2.get_metadata(), c1.get_metadata())

    c1.delete('key')
    assert len(c1.get_metadata()) == 0
    with pytest.raises(KeyError):
        c1.get('key')

    mocker.patch.object(df_test, 'to_hdf').side_effect = IOError('disk full')
    with pytest.raises(OSError):
        c1.set('key', df_test)
    assert len(c1.get_metadata()) == 0


def case_inmemory(tmp_path=None) -> CaseData:
    return (CacheEnum.MEMORY,)


def case_hdf(tmp_path) -> CaseData:
    return (CacheEnum.HDF, tmp_path)


@cases_data(module=THIS_MODULE)
def test_cache_invalidation(df_test, tmp_path, case_data: CaseDataGetter):
    """it should invalidate cache when new mtime is more recent"""
    cache_args = case_data.get(tmp_path)
    c = Cache.get_cache(*cache_args)
    c.set('key', df_test, mtime=10)
    with pytest.raises(KeyError):
        c.get('key', mtime=15)


@cases_data(module=THIS_MODULE)
def test_cache_expiration(df_test, mocker, tmp_path, case_data: CaseDataGetter):
    """it should invalidate cache when expiration date is reached"""
    cache_args = case_data.get(tmp_path)
    c = Cache.get_cache(*cache_args)
    c.set('key', df_test)
    mocker.patch('peakina.cache.time').return_value = time.time() + 3600 * 24 * 9
    assert_frame_equal(c.get('key', expire=timedelta(days=10)), df_test)
    with pytest.raises(KeyError):
        c.get('key', expire=timedelta(days=8))


def test_hdf_store_closed_on_error(df_test, mocker, tmp_path):
    """it should not crash if HDF is not valid

    i.e. if `read_hdf` raises an AssertionError and doesn't close the HDF store
    """
    c1 = Cache.get_cache(CacheEnum.HDF, cache_dir=tmp_path)
    c1.set('key', df_test)
    c1.set_metadata(df_test)
    # cf. https://github.com/pandas-dev/pandas/issues/28430: an exception might
    # occur in `read_hdf` leaving the HDF store opened.
    mocker.patch('pandas.io.pytables.HDFStore.select').side_effect = AssertionError(
        'gaps in blocks ref_loc'
    )
    assert c1.get_metadata().shape == (0, 3)
