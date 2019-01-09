import pandas as pd

from .datasource import DataSource


def read_pandas(uri, match=None, type=None, **kwargs) -> pd.DataFrame:
    ds = DataSource(uri, type, match, kwargs)  # type: ignore
    return ds.get_df()
