import pandas as pd

from peakina.io.ftp import ftp_fetcher


def test_ftp_fetcher(mocker, ftp_path):
    fetcher = ftp_fetcher.FTPFetcher()
    mtime_spy = mocker.spy(ftp_fetcher, "ftp_mtime")

    myfile = "my_data_2015.csv"
    myfile_path = f"{ftp_path}/{myfile}"
    tmpfile = fetcher.open(myfile_path)
    assert pd.read_csv(tmpfile).shape == (2, 2)

    assert (mtime := fetcher.mtime(myfile_path)) is not None
    assert mtime > 1e9
    assert mtime_spy.call_count == 1

    mtime_spy.reset_mock()
    assert myfile in fetcher.listdir(ftp_path)  # gets all mtimes
    for year in range(2015, 2018):
        assert (mtime := fetcher.mtime(f"{ftp_path}/my_data_{year}.csv")) is not None
        assert mtime > 1e9
    assert mtime_spy.call_count == 0

    # this new fetcher should reuse the same mtime_cache
    other_fetcher = ftp_fetcher.FTPFetcher()
    other_fetcher.mtime(myfile_path)
    assert mtime_spy.call_count == 0  # <- no call
