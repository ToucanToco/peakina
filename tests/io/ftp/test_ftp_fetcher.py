import pandas as pd

from peakina.io.ftp.ftp_fetcher import FTPFetcher


def test_ftp_fetcher(ftp_path):
    myfile = 'my_data_2015.csv'
    myfile_path = f'{ftp_path}/{myfile}'
    tmpfile = FTPFetcher.open(myfile_path)
    assert pd.read_csv(tmpfile).shape == (2, 2)
    assert myfile in FTPFetcher.listdir(ftp_path)
    assert FTPFetcher.mtime(myfile_path) > 1e9
