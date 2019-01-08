import pandas as pd

from peakina.io.ftp.ftp_reader import FTPReader


def test_ftp_reader(ftp_path):
    myfile = 'my_data_2015.csv'
    myfile_path = f'{ftp_path}/{myfile}'
    tmpfile = FTPReader.open(myfile_path)
    assert pd.read_csv(tmpfile).shape == (2, 2)
    assert myfile in FTPReader.listdir(ftp_path)
    assert FTPReader.mtime(myfile_path) > 1e9
