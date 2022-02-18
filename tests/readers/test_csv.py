import pandas as pd

from peakina import DataSource


def test_simple_csv(path):
    """It should be able to detect type if not set"""
    ds = DataSource(path("0_0.csv"), reader_kwargs={"encoding": "utf8", "sep": ","})
    assert ds.get_df().shape == (2, 2)


def test_simple_csv_preview(path):
    """It should be able to detect type if not set"""
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"encoding": "utf8", "sep": ",", "preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 2)

    assert ds.get_df().equals(pd.DataFrame({"month": ["Mars-14", "Avr-14"], "value": [3.3, 3.1]}))

    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"encoding": "utf8", "sep": ",", "preview_nrows": 2, "preview_offset": 0},
    )
    assert ds.get_df().shape == (2, 2)


def test_simple_csv_metadata(path):
    """It should be able to detect type if not set"""
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"encoding": "utf8", "sep": ",", "preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_metadata()["nrows"] == 12
