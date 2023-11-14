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


def test_csv_metadata(path):
    """
    It should be able to get metadata of a csv file that contain 12 lines
    """
    # when no kwargs are provided
    ds = DataSource(path("fixture-1.csv"))
    assert ds.get_df().shape == (12, 2)
    assert ds.get_metadata() == {
        "df_rows": 12,
        "total_rows": 12,
    }

    # extra pandas kwargs that impact size of dataframe
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"skipfooter": 5},
    )
    assert ds.get_df().shape == (7, 2)
    assert ds.get_metadata() == {
        "df_rows": 7,
        "total_rows": 12,
    }

    # skiprows as integer
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"skiprows": 3, "skipfooter": 4},
    )
    assert ds.get_df().shape == (5, 2)
    assert ds.get_metadata() == {
        "df_rows": 5,
        "total_rows": 12,
    }

    # skiprows as list
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"skiprows": [0, 2, 4], "skipfooter": 4},
    )
    assert ds.get_df().shape == (5, 2)
    assert ds.get_metadata() == {
        "df_rows": 5,
        "total_rows": 12,
    }

    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"nrows": 3},
    )
    assert ds.get_df().shape == (3, 2)
    assert ds.get_metadata() == {
        "df_rows": 3,
        "total_rows": 12,
    }

    # with only offset, this means df_rows = total_rows - preview_offset
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"preview_offset": 3},
    )
    assert ds.get_df().shape == (9, 2)
    assert ds.get_metadata() == {
        "df_rows": 9,
        "total_rows": 12,
    }

    # with only nrows we request 7 rows and we have df_rows == 7
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"preview_nrows": 7},
    )
    assert ds.get_df().shape == (7, 2)
    assert ds.get_metadata() == {
        "df_rows": 7,
        "total_rows": 12,
    }

    # with nrows and offset
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 2)
    assert ds.get_metadata() == {
        "df_rows": 2,
        "total_rows": 12,
    }

    # we equest 15 lines and we can got only 12 as it's the maximum amount of rows we have
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"preview_nrows": 15},
    )
    assert ds.get_df().shape == (12, 2)
    assert ds.get_metadata() == {
        "df_rows": 12,
        "total_rows": 12,
    }


def test_chunk_and_preview(path):
    """It should be able to retrieve a dataframe with chunks"""
    ds = DataSource(path("0_0.csv"), reader_kwargs={"chunksize": 1, "preview_nrows": 1})
    assert ds.get_df().shape == (1, 2)

    # with nrows and offset
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"chunksize": 3, "preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 2)
    assert ds.get_metadata() == {
        "df_rows": 2,
        "total_rows": 12,
    }

    # we equest 15 lines and we can got only 12 as it's the maximum amount of rows we have
    ds = DataSource(
        path("fixture-1.csv"),
        reader_kwargs={"chunksize": 7, "preview_nrows": 15},
    )
    assert ds.get_df().shape == (12, 2)
    assert ds.get_metadata() == {
        "df_rows": 12,
        "total_rows": 12,
    }
