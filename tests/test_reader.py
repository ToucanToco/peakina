from peakina.reader import read_pandas


def test_reader(path):
    df = read_pandas(path('0_0.csv'))
    assert df.shape == (2, 2)
