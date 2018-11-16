from .datasource import DataSource


def read_pandas(file_path, **kwargs):
    return DataSource(file_path, **kwargs).df
