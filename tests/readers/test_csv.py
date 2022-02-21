import pandas as pd

from peakina import DataSource


def test_simple_csv(path):
    """It should be able to detect type if not set"""
    ds = DataSource(path("0_0.csv"), reader_kwargs={"encoding": "utf8", "sep": ","})
    assert ds.get_df().shape == (2, 2)


def test_simple_csv_preview(path):
    """It should be able to get a preview of a csv file"""
    ds = DataSource(path("fixture-1.csv"))
    assert ds.get_df().shape == (12, 2)
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                "month": [
                    "Jan-14",
                    "Fev-14",
                    "Mars-14",
                    "Avr-14",
                    "Mai-14",
                    "Juin-14",
                    "Juil-14",
                    "Aout-14",
                    "Sept-14",
                    "Oct-14",
                    "Nov-14",
                    "Dec-14",
                ],
                "value": [2.7, 3.2, 3.3, 3.1, 3.9, 3.4, 3.1, 3.2, 3.4, 3.8, 3.7, 3.6],
            }
        )
    )

    # preview with only `nrows`
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"preview_nrows": 2},
    )
    assert ds.get_df().shape == (2, 2)
    assert ds.get_df().equals(pd.DataFrame({"month": ["Jan-14", "Fev-14"], "value": [2.7, 3.2]}))

    # preview with `offset` and `nrows`
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 2)
    assert ds.get_df().equals(pd.DataFrame({"month": ["Mars-14", "Avr-14"], "value": [3.3, 3.1]}))


def test_simple_csv_metadata(path):
    """It should be able to get metadata of a csv file"""
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_metadata()["nrows"] == 12
