import pandas as pd

from .datasource import DataSource


def read_pandas(file, match=None, type=None, **kwargs) -> pd.DataFrame:
    ds = DataSource(file, type, match, kwargs)  # type: ignore
    return ds.get_df()
