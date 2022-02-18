import os
import time
from datetime import timedelta

import pandas as pd
import pytest

from peakina.cache import InMemoryCache
from peakina.datasource import DataSource, read_pandas
from peakina.helpers import TypeEnum
from peakina.io import MatchEnum


@pytest.fixture
def read_csv_spy(mocker):
    read_csv = mocker.spy(pd, "read_csv")
    # need to mock the validation as the signature is changed via the spy
    mocker.patch("peakina.datasource.validate_kwargs", return_value=True)

    return read_csv


def test_scheme():
    """It should be able to set scheme"""
    assert DataSource("my/local/path/file.csv").scheme == ""
    assert DataSource("ftp://remote/path/file.csv").scheme == "ftp"
    with pytest.raises(AttributeError) as e:
        DataSource("pika://wtf/did/I/write")
    assert str(e.value) == "Invalid scheme 'pika'"


def test_type():
    """It should be able to set type if possible"""
    assert DataSource("myfile.csv").type is TypeEnum.CSV
    with pytest.raises(ValueError):
        DataSource("myfile.csv$")
    assert DataSource("myfile.tsv$", match=MatchEnum.GLOB).type is TypeEnum.CSV
    assert DataSource("myfile.*", match=MatchEnum.GLOB).type is None


def test_validation_kwargs(mocker):
    """It should be able to validate the extra kwargs"""
    validatation_kwargs = mocker.patch("peakina.datasource.validate_kwargs")

    DataSource("myfile.csv")
    validatation_kwargs.assert_called_once_with({}, "csv")
    validatation_kwargs.reset_mock()

    DataSource("myfile.*", match=MatchEnum.GLOB)
    validatation_kwargs.assert_called_once_with({}, None)
    validatation_kwargs.reset_mock()


def test_csv_with_sep(path):
    """It should be able to detect separator if not set"""
    ds = DataSource(path("0_0_sep.csv"))
    assert ds.get_df().shape == (2, 2)

    ds = DataSource(path("0_0_sep.csv"), reader_kwargs={"sep": ","})
    assert ds.get_df().shape == (2, 1)


def test_csv_with_encoding(path):
    """It should be able to detect the encoding if not set"""
    df = DataSource(path("latin_1.csv")).get_df()
    assert df.shape == (2, 7)
    assert "unité économique" in df.columns


def test_csv_with_sep_and_encoding(path):
    """It should be able to detect everything"""
    ds = DataSource(path("latin_1_sep.csv"))
    assert ds.get_df().shape == (2, 7)


def test_read_pandas(path):
    """It should be able to detect everything with read_pandas shortcut"""
    assert read_pandas(path("latin_1_sep.csv")).shape == (2, 7)


def test_read_pandas_excel(path):
    """It should be able to detect everything with read_pandas shortcut"""
    assert read_pandas(path("0_2.xls"), keep_default_na=False).shape == (2, 2)


def test_match(path):
    """It should be able to concat files matching a pattern"""
    ds = DataSource(path(r"0_\d.csv"), match=MatchEnum.REGEX)
    df = ds.get_df()
    assert set(df["__filename__"]) == {"0_0.csv", "0_1.csv"}
    assert df.shape == (4, 3)


def test_match_different_file_types(path):
    """It should be able to match even different types, encodings or seps"""
    ds = DataSource(path("0_*"), match=MatchEnum.GLOB)
    df = ds.get_df()
    assert set(df["__filename__"]) == {"0_0.csv", "0_0_sep.csv", "0_1.csv", "0_2.xls"}
    assert df.shape == (8, 3)


@pytest.mark.flaky(reruns=5)
def test_ftp(ftp_path):
    ds = DataSource(f"{ftp_path}/sales.csv")
    assert ds.get_df().shape == (208, 15)


@pytest.mark.flaky(reruns=5)
def test_ftp_match(ftp_path):
    ds = DataSource(f"{ftp_path}/my_data_\\d{{4}}\\.csv$", match=MatchEnum.REGEX)
    assert ds.get_df().shape == (8, 3)


def test_s3(s3_endpoint_url):
    dirpath = "s3://accessKey1:verySecretKey1@mybucket"

    ds = DataSource(
        f"{dirpath}/0_0.csv",
        fetcher_kwargs={"client_kwargs": {"endpoint_url": s3_endpoint_url}},
    )
    assert ds.get_df().shape == (2, 2)

    ds = DataSource(
        f"{dirpath}/0_*.csv",
        match=MatchEnum.GLOB,
        fetcher_kwargs={"client_kwargs": {"endpoint_url": s3_endpoint_url}},
    )
    assert ds.get_df().shape == (4, 3)

    # With subdirectories
    ds = DataSource(
        f"{dirpath}/mydir/0_*.csv",
        match=MatchEnum.GLOB,
        fetcher_kwargs={"client_kwargs": {"endpoint_url": s3_endpoint_url}},
    )
    assert ds.get_df().shape == (4, 3)


def test_basic_excel(path):
    """It should not add a __sheet__ column when retrieving a single sheet"""
    ds = DataSource(path("fixture-multi-sheet.xlsx"))
    df = pd.DataFrame({"Month": [1], "Year": [2019]})
    assert ds.get_df().equals(df)
    assert ds.get_metadata() == {"nrows": 1, "sheetnames": ["January", "February"]}

    # On match datasources, no metadata is returned:
    assert DataSource(path("fixture-multi-sh*t.xlsx"), match=MatchEnum.GLOB).get_metadata() == {}


def test_multi_sheets_excel(path):
    """It should add a __sheet__ column when retrieving multiple sheet"""
    ds = DataSource(path("fixture-multi-sheet.xlsx"), reader_kwargs={"sheet_name": None})
    df = pd.DataFrame({"Month": [1, 2], "Year": [2019, 2019], "__sheet__": ["January", "February"]})
    assert ds.get_df().equals(df)


def test_basic_xml(path):
    """It should apply optional jq filter when extracting an xml datasource"""
    # No jq filter -> everything is in one cell
    assert DataSource(path("fixture.xml")).get_df().shape == (1, 1)

    jq_filter = ".records"
    ds = DataSource(path("fixture.xml"), reader_kwargs={"filter": jq_filter})
    assert ds.get_df().shape == (2, 1)

    jq_filter = '.records .record[] | .["@id"]|=tonumber'
    ds = DataSource(path("fixture.xml"), reader_kwargs={"filter": jq_filter})
    df = pd.DataFrame({"@id": [1, 2], "title": ["Keep on dancin'", "Small Talk"]})
    assert ds.get_df().equals(df)


def test_basic_json(path):
    """It should apply optional jq filter when extracting a json datasource"""
    # No jq filter -> everything is in one cell
    assert DataSource(path("fixture.json")).get_df().shape == (1, 1)

    jq_filter = '.records .record[] | .["@id"]|=tonumber'
    ds = DataSource(path("fixture.json"), reader_kwargs={"filter": jq_filter, "lines": True})
    df = pd.DataFrame({"@id": [1, 2], "title": ["Keep on dancin'", "Small Talk"]})
    assert ds.get_df().equals(df)


def test_basic_parquet(path):
    """It should open a basic parquet file"""
    df = DataSource(path("userdata.parquet")).get_df()
    assert df.shape == (1000, 13)
    df = DataSource(
        path("userdata.parquet"),
        type=TypeEnum.PARQUET,
        reader_kwargs={"columns": ["title", "country"]},
    ).get_df()
    assert df.shape == (1000, 2)


def test_empty_file(path):
    """It should return an empty dataframe if the file is empty"""
    assert DataSource(path("empty.csv")).get_df().equals(pd.DataFrame())


def test_chunk(path):
    """It should be able to retrieve a dataframe with chunks"""
    ds = DataSource(path("0_0.csv"), reader_kwargs={"chunksize": 1})
    assert all(df.shape == (1, 2) for df in ds.get_dfs())
    assert ds.get_df().shape == (2, 2)


def test_chunk_match(path):
    """It should be able to retrieve a dataframe with chunks and match"""
    ds = DataSource(path("0_*.csv"), match=MatchEnum.GLOB, reader_kwargs={"chunksize": 1})
    assert all(df.shape == (1, 3) for df in ds.get_dfs())
    df = ds.get_df()
    assert df.shape == (6, 3)
    assert "__filename__" in df.columns


def test_cache(path, mocker):
    df = pd.DataFrame({"x": [1, 2, 3]})
    ds = DataSource(path("0_0.csv"), expire=timedelta(seconds=10))
    mtime = int(os.path.getmtime(path("0_0.csv")))
    cache = InMemoryCache()
    cache.set(ds.hash, value=df, mtime=mtime)

    assert ds.get_df().shape == (2, 2)  # without cache: read from disk
    assert ds.get_df(cache=cache).shape == (3, 1)  # retrieved from cache

    mock_time = mocker.patch("peakina.cache.time")
    mock_time.return_value = time.time() + 15  # fake 15s elapsed
    assert ds.get_df(cache=cache).shape == (2, 2)  # 15 > 10: cache expires: read from disk
    assert cache.get(ds.hash).shape == (2, 2)  # cache has been updated with the new data

    mock_time.reset_mock()
    cache.set(ds.hash, value=df, mtime=mtime)  # put back the fake df
    assert ds.get_df(cache=cache).shape == (3, 1)  # back to "retrieved from cache"
    # fake a file with a different mtime (e.g: a new file has been uploaded):
    mocker.patch("peakina.io.local.file_fetcher.os.path.getmtime").return_value = mtime - 1
    assert ds.get_df(cache=cache).shape == (2, 2)  # cache has been invalidated
