import pandas as pd

from peakina import DataSource


def test_simple_parquet_preview(path):
    """It should be able to get a preview of a parquet file"""
    ds = DataSource(path("fixture.parquet"), reader_kwargs={"columns": ["Date", "Country"]})
    assert ds.get_df().shape == (4900, 2)

    # preview with only `nrows`
    ds = DataSource(
        path("fixture.parquet"),
        reader_kwargs={"preview_nrows": 2, "columns": ["Date", "Country"]},
    )
    assert ds.get_df().shape == (2, 2)
    assert ds.get_df().equals(
        pd.DataFrame({"Date": ["29/01/1900", "31/07/1900"], "Country": ["Australia", "Croatia"]})
    )

    # preview with `offset` and `nrows`
    ds = DataSource(
        path("fixture.parquet"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2, "columns": ["Date", "Country"]},
    )
    assert ds.get_df().shape == (2, 2)
    assert ds.get_df().equals(
        pd.DataFrame({"Date": ["21/08/1900", None], "Country": ["Usa", "Usa"]})
    )

    # preview with only `offset`
    ds = DataSource(
        path("fixture.parquet"),
        reader_kwargs={"preview_offset": 2, "columns": ["Date", "Country"]},
    )
    assert ds.get_df().shape == (4898, 2)
