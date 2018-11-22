from peakina.reader import read_pandas


def test_read(path):
    """It should create a datasource and return its dataframe"""

    df = read_pandas(path('0.tsv'), dtype={'b': str}, skiprows=[1])
    assert df.shape == (2, 3)
