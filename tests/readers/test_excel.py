import pandas as pd

from peakina import DataSource


def test_simple_xls(path):
    """It should be able to detect type if not set"""
    ds = DataSource(path("0_2.xls"), reader_kwargs={})
    assert ds.get_df().shape == (2, 2)


def test_simple_xls_preview(path):
    """It should be able to get a preview of an excel file"""
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 6)


def test_simple_xls_metadata(path):
    """It should be able to get metadata of an excel file"""
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_metadata()["nrows"] == 170


def test_multisheet_xlsx(path):
    """It should be able to read multiple sheets and add them together"""
    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None},
    )
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                "Month": [1, 2],
                "Year": [2019, 2019],
                "__sheet__": ["January", "February"],
            }
        )
    )

    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={},
    )
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                "Month": [1],
                "Year": [2019],
            }
        )
    )

    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": "February"},
    )
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                "Month": [2],
                "Year": [2019],
            }
        )
    )
