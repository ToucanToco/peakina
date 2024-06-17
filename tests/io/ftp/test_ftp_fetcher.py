import pandas as pd
from pytest_mock import MockerFixture

from peakina.io.ftp import ftp_fetcher


def test_ftp_fetcher(mocker: MockerFixture, ftp_path: str) -> None:
    ftp_fetcher.get_mtimes_cache.cache_clear()
    fetcher = ftp_fetcher.FTPFetcher()
    mtime_spy = mocker.spy(ftp_fetcher, "ftp_mtime")

    myfile = "my_data_2015.csv"
    myfile_path = f"{ftp_path}/{myfile}"
    tmpfile = fetcher.open(myfile_path)
    assert pd.read_csv(tmpfile).shape == (2, 2)

    mocker.patch("peakina.io.ftp.ftp_fetcher.get_mtimes_cache", return_value={"something": "else"})
    mtime = fetcher.mtime(myfile_path)
    assert mtime is not None
    assert mtime > 1e9
    assert mtime_spy.call_count == 1

    mtime_spy.reset_mock()
    mocker.patch(
        "peakina.io.ftp.ftp_fetcher.get_mtimes_cache", return_value={ftp_path: {myfile: 1e95}}
    )
    assert myfile in fetcher.listdir(ftp_path)  # gets all mtimes
    for year in range(2015, 2018):
        assert (mtime := fetcher.mtime(f"{ftp_path}/my_data_{year}.csv")) is not None
        assert mtime > 1e9
    assert mtime_spy.call_count == 0

    # this new fetcher should reuse the same mtime_cache
    other_fetcher = ftp_fetcher.FTPFetcher()
    other_fetcher.mtime(myfile_path)
    assert mtime_spy.call_count == 0  # <- no call
