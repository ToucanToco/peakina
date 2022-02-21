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
                "part": [9, 45, 35, 10, 1],
                "clients": [896] * 5,
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
                "part": [9, 45],
                "clients": [896] * 2,
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
                "part": [35, 10],
                "clients": [896] * 2,
                "pays": ["France"] * 2,
            }
        )
    )


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
