from .datasource import DataSource


def read_pandas(file_path, type=None, match=None, **kwargs):
    ds = DataSource(file_path, type, match, kwargs)
    return ds.get_df()
