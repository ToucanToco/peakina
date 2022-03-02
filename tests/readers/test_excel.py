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
    assert df[:5].equals(
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
                "part": [9.0, 45.0, 35.0, 10.0, 1.0],
                "clients": [896.0] * 5,
                "pays": ["France"] * 5,
            }
        )
    )

    # preview with only `nrows`
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2},
    )
    assert ds.get_df().shape == (2, 6)
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                "breakdown": ["Par territoire"] * 2,
                "catégorie": ["Agglo 1 2014"] * 2,
                "fréquence": ["Au moins 1 fois/mois", "plusieurs fois/an"],
                "part": [9.0, 45.0],
                "clients": [896.0] * 2,
                "pays": ["France"] * 2,
            }
        )
    )

    # preview with `offset` and `nrows`
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 6)
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                "breakdown": ["Par territoire"] * 2,
                "catégorie": ["Agglo 1 2014"] * 2,
                "fréquence": ["1 fois/an", "moins souvent"],
                "part": [35.0, 10.0],
                "clients": [896.0] * 2,
                "pays": ["France"] * 2,
            }
        )
    )


def test_xls_metadata(path):
    """It should be able to get metadata of an excel file"""
    # when no kwargs are provided
    ds = DataSource(path("fixture.xls"))
    assert ds.get_df().shape == (170, 6)
    assert ds.get_metadata() == {
        "sheetnames": ["Data"],
        "df_rows": 170,
        "total_rows": 170,
    }

    # extra pandas kwargs that impact size of dataframe
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"skipfooter": 5},
    )
    assert ds.get_df().shape == (165, 6)
    assert ds.get_metadata() == {
        "sheetnames": ["Data"],
        "df_rows": 165,
        "total_rows": 170,
    }

    # with only offset, this means df_rows = total_rows - preview_offset
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_offset": 7},
    )
    assert ds.get_df().shape == (163, 6)
    assert ds.get_metadata() == {
        "sheetnames": ["Data"],
        "df_rows": 163,
        "total_rows": 170,
    }

    # with nrows and offset
    ds = DataSource(
        path("fixture.xls"),
        reader_kwargs={"preview_nrows": 2, "preview_offset": 2},
    )
    assert ds.get_df().shape == (2, 6)
    assert ds.get_metadata() == {
        "sheetnames": ["Data"],
        "df_rows": 2,
        "total_rows": 170,
    }


def test_multiple_xls_metadata(path):
    """It should be able to get metadata of an excel file with multiple sheets"""
    # with multiple sheets
    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None, "preview_nrows": 1, "preview_offset": 1},
    )
    # because our excel file has 1 entry on January sheet and 3 entries in February sheet
    assert ds.get_df().shape == (1, 3)
    assert ds.get_metadata() == {
        "sheetnames": ["January", "February"],
        "df_rows": 1,
        "total_rows": 4,
    }

    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None, "preview_nrows": 2, "preview_offset": 2},
    )
    # because our excel file has 1 entry on January sheet and 3 entries in February sheet
    assert ds.get_df().shape == (1, 3)
    assert ds.get_metadata() == {
        "sheetnames": ["January", "February"],
        "df_rows": 1,
        "total_rows": 4,
    }

    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None, "preview_nrows": 2},
    )
    # because our excel file has 1 entry on January sheet and 3 entries in February sheet
    # the result is 3 lines here because we're previewing 2 rows from January's sheet (which is 1 as result) and
    # 2 rows from February's sheet (which is 2 as result)
    # 1 + 2 => 3 lines/rows
    assert ds.get_df().shape == (3, 3)
    assert ds.get_metadata() == {
        "sheetnames": ["January", "February"],
        "df_rows": 3,
        "total_rows": 4,
    }

    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None, "preview_offset": 2},
    )
    # because our excel file has 1 entry on January sheet and 3 entries in February sheet
    # the result is 0 lines/rows here because we're previewing an offset of 2 on available
    # rows from January's sheet (1 row) (as result we have 0 from this sheet) and an offset of 2
    #  on February's sheet rows (3rows) (as result we have 1 from this sheet)
    # 0 + 1 => 1 lines/rows (the line from February sheet)
    assert ds.get_df().shape == (1, 3)
    assert ds.get_df().equals(
        pd.DataFrame({"Month": [4], "Year": [2022], "__sheet__": ["February"]})
    )
    assert ds.get_metadata() == {
        "sheetnames": ["January", "February"],
        "df_rows": 1,
        "total_rows": 4,
    }


def test_multisheet_xlsx(path):
    """It should be able to read multiple sheets and add them together"""
    ds = DataSource(
        path("fixture-multi-sheet.xlsx"),
        reader_kwargs={"sheet_name": None},
    )
    # because our excel file has 1 entry on January sheet and 3 entries in February sheet
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                "Month": [1, 2, 3, 4],
                "Year": [2019, 2019, 2021, 2022],
                "__sheet__": ["January", "February", "February", "February"],
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
    # because our excel file has 1 entry on January sheet and 3 entries in February sheet
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                "Month": [2, 3, 4],
                "Year": [2019, 2021, 2022],
            }
        )
    )


def test_with_specials_types_xlsx(path):
    """It should be able to read sheet and format types"""
    ds = DataSource(
        path("fixture-single-sheet-with-types.xlsx"),
    )
    assert ds.get_df().equals(
        pd.DataFrame(
            {
                None: [0, 1, 2],
                "bools": [True, False, True],
                "dates": ["03/02/2022 05:43:04", "03/02/2022 05:43:04", "03/02/2022 05:43:04"],
            }
        )
    )
