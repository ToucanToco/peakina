import pandas as pd

from peakina import DataSource


def test_simple_xls(path):
    """It should be able to detect type if not set"""
    ds = DataSource(path("0_2.xls"))
    assert ds.get_df().shape == (2, 2)


def test_simple_xls_preview(path):
    """It should be able to get a preview of an excel file"""
    df = DataSource(path("fixture.xls")).get_df()
    assert df.shape == (170, 6)
    pd.testing.assert_frame_equal(
        df[:5],
        pd.DataFrame(
            {
                "breakdown": ["Par territoire"] * 5,
                "catégorie": ["Agglo 1 2014"] * 5,
                "fréquence": [
                    "Au moins 1 fois/mois",
                    "plusieurs fois/an",
                    "1 fois/an",
                    "moins souvent",
                    "jamais",
                ],
                "part": [9, 45, 35, 10, 1],
                "clients": [896] * 5,
                "pays": ["France"] * 5,
            }
        ),
    )

    # preview with only `nrows`
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2},
    )
    assert ds.get_df().shape == (2, 6)
    pd.testing.assert_frame_equal(
        ds.get_df(),
        pd.DataFrame(
            {
                "breakdown": ["Par territoire"] * 2,
                "catégorie": ["Agglo 1 2014"] * 2,
                "fréquence": ["Au moins 1 fois/mois", "plusieurs fois/an"],
                "part": [9, 45],
                "clients": [896] * 2,
                "pays": ["France"] * 2,
            }
        ),
    )

    # preview with `offset` and `nrows`
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 6)
    pd.testing.assert_frame_equal(
        ds.get_df(),
        pd.DataFrame(
            {
                "breakdown": ["Par territoire"] * 2,
                "catégorie": ["Agglo 1 2014"] * 2,
                "fréquence": ["1 fois/an", "moins souvent"],
                "part": [35, 10],
                "clients": [896] * 2,
                "pays": ["France"] * 2,
            }
        ),
    )


def test_xls_metadata(path):
    """It should be able to get metadata of an excel file"""
    # when no kwargs are provided
    ds = DataSource(path("fixture.xls"), reader_kwargs={"sheet_name": "Data"})
    assert ds.get_df().shape == (170, 6)
    assert ds.get_metadata() == {"sheetnames": ["Data"]}

    # extra pandas kwargs that impact size of dataframe
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"skipfooter": 5},
    )
    assert ds.get_df().shape == (165, 6)
    assert ds.get_metadata() == {"sheetnames": ["Data"]}

    # with nrows and offset
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 6)
    assert ds.get_metadata() == {"sheetnames": ["Data"]}


def test_multiple_xls_metadata(path):
    """It should be able to get metadata of an excel file with multiple sheets"""
    # for all sheets
    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None, "preview_nrows": 1, "preview_offset": 1},
    )
    assert ds.get_metadata() == {"sheetnames": ["January", "February"]}

    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None, "preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_metadata() == {"sheetnames": ["January", "February"]}

    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None, "preview_nrows": 2},
    )
    assert ds.get_metadata() == {"sheetnames": ["January", "February"]}

    # for a specific sheet (not the first one)
    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": "February"},
    )
    assert ds.get_df().shape == (3, 2)
    assert ds.get_metadata() == {"sheetnames": ["January", "February"]}


def test_preview_sheet_more_lines_xlsx(path):
    """It should not fail with an 'IndexError' when preview_nrows is bigger than the real amount of rows"""
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2000, "preview_offset": 2},
    )
    assert ds.get_df().shape == (168, 6)
    assert ds.get_metadata() == {"sheetnames": ["Data"]}

    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": "February", "preview_offset": 0, "preview_nrows": 1000},
    )
    # because our excel file has 1 entry on January sheet and 3 entries in February sheet
    pd.testing.assert_frame_equal(
        ds.get_df(), pd.DataFrame({"Month": [2.0, 3.0, 4.0], "Year": [2019.0, 2021.0, 2022.0]})
    )


def test_with_specials_types_xlsx(path):
    """It should be able to read sheet and format types"""
    ds = DataSource(
        path("fixture-multi-sheet-with-types.xlsx"),
    )

    test_dates = ["03/02/2022 05:43:04", "03/02/2022 05:43:04", "03/02/2022 05:43:04"]
    pd.testing.assert_frame_equal(
        ds.get_df(),
        pd.DataFrame(
            {
                "Unnamed: 0": [0.0, 1.0, 2.0],
                "bools": [True, False, True],
                "dates": pd.Series([pd.Timestamp(d) for d in test_dates]).astype("datetime64[ms]"),
                "floats": [12.35, 42.69, 1234567.0],
            }
        ),
    )


def test_unnamed_columns(path):
    """It should be able to read sheet and format types"""
    ds = DataSource(
        path("fixture-multi-sheet-with-types.xlsx"),
        reader_kwargs={"sheet_name": "unnamed_columns"},
    )

    test_dates = ["03/02/2022 05:43:04", "03/02/2022 05:43:04"]
    df = ds.get_df()
    pd.testing.assert_frame_equal(
        df,
        pd.DataFrame(
            {
                "0": [1.0, 2.0],
                "Unnamed: 1": [False, True],
                "Unnamed: 2": pd.Series([pd.Timestamp(d) for d in test_dates]).astype(
                    "datetime64[ms]"
                ),
                "12.35": [42.69, 1234567.0],
            }
        ),
    )


def test_read_with_dtype(path):
    """Check that read excel is able to handle provided dtypes"""
    ds = DataSource(
        path("fixture-single-sheet.xlsx"), reader_kwargs={"dtype": {"Month": "str", "Year": "str"}}
    )
    assert isinstance(ds.get_df()["Month"][0], str)


def test_read_excel_with_formula(path):
    """check that read excel is able to handle a sheet with formula"""
    ds = DataSource(path("formula_excel.xlsx"))
    assert ds.get_df()["sum"][0] == 5


def test_excel_meta_with_broken_max_row(path):
    """check that read excel is able to retrieve max row when it's incorrectly set
    to test, the fixture file formula_excel.xlsx has it's metadata broken
    """
    ds = DataSource(path("formula_excel.xlsx"))
    assert ds.get_metadata() == {"sheetnames": ["Sheet1"]}
