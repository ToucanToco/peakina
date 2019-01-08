from peakina.main import read_pandas


def test_read_pandas(path, ftp_path):
    assert read_pandas(path('0_0.csv')).shape == (2, 2)
    assert read_pandas(path('0_0.csv'), sep=';').shape == (2, 1)
    df = read_pandas(f'{ftp_path}/my_data_\\d{{4}}\\.csv$', match='regex', dtype={'a': 'str'})
    assert df.shape == (8, 3)
    assert df.iloc[0].tolist() == ['0', 0, 'my_data_2015.csv']
    assert df.iloc[-1].tolist() == ['4', 1, 'my_data_2018.csv']
