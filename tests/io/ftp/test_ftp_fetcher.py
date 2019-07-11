import pandas as pd

from peakina.io.ftp import ftp_fetcher


def test_ftp_fetcher(mocker, ftp_path):
    fetcher = ftp_fetcher.FTPFetcher('')
    mtime_spy = mocker.spy(ftp_fetcher, 'ftp_mtime')

    myfile = 'my_data_2015.csv'
    myfile_path = f'{ftp_path}/{myfile}'
    tmpfile = fetcher.open(myfile_path)
    assert pd.read_csv(tmpfile).shape == (2, 2)

    assert fetcher.mtime(myfile_path) > 1e9
    assert mtime_spy.call_count == 1

    mtime_spy.reset_mock()
    assert myfile in fetcher.listdir(ftp_path)  # gets all mtimes
    assert fetcher.mtime('my_data_2015.csv') > 1e9
    assert fetcher.mtime('my_data_2016.csv') > 1e9
    assert fetcher.mtime('my_data_2017.csv') > 1e9
    assert mtime_spy.call_count == 0
