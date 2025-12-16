from typing import Callable

import geopandas as gpd

from peakina import DataSource
from peakina.readers import read_geo_data

sample_geojson = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [102.0, 0.5]},
            "properties": {"prop0": "value0", "prop1": 2.0},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]],
            },
            "properties": {"prop0": "value0", "prop1": 0.0},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]
                ],
            },
            "properties": {"prop0": "value0", "prop1": 3.0},
        },
    ],
}


def test_simple_geojson_preview(path):
    """It should be able to get a preview of a geojson file"""
    ds = DataSource(path("sample.geojson"))
    expected = gpd.GeoDataFrame.from_features(sample_geojson)
    result = ds.get_df()
    assert (result["prop0"] == expected["prop0"]).all()
    assert (result["prop1"] == expected["prop1"]).all()
    assert (result["geometry"] == expected["geometry"]).all()

    ds = DataSource(path("sample.geojson"), reader_kwargs={"preview_offset": 1})
    result = ds.get_df()

    expected = gpd.GeoDataFrame.from_features(sample_geojson).iloc[1:]
    expected.index = [0, 1]
    assert (result["prop0"] == expected["prop0"]).all()
    assert (result["prop1"] == expected["prop1"]).all()
    assert (result["geometry"] == expected["geometry"]).all()

    ds = DataSource(path("sample.geojson"), reader_kwargs={"preview_nrows": 2})
    result = ds.get_df()

    expected = gpd.GeoDataFrame.from_features(sample_geojson).iloc[:2]
    assert (result["prop0"] == expected["prop0"]).all()
    assert (result["prop1"] == expected["prop1"]).all()
    assert (result["geometry"] == expected["geometry"]).all()

    ds = DataSource(path("sample.geojson"), reader_kwargs={"preview_offset": 1, "preview_nrows": 1})
    result = ds.get_df()
    expected = gpd.GeoDataFrame.from_features(sample_geojson).iloc[1]
    assert (result["prop0"] == expected["prop0"]).all()
    assert (result["prop1"] == expected["prop1"]).all()
    assert (result["geometry"] == expected["geometry"]).all()


def test_geojson_bbox(path):
    bbox = (102, 0.5, 102.5, 0.5)
    ds = DataSource(path("sample.geojson"), reader_kwargs={"bbox": bbox})
    expected = gpd.GeoDataFrame.from_features(sample_geojson).iloc[:2]
    assert (ds.get_df()["geometry"] == expected["geometry"]).all()


def test_geojson_mask(path):
    mask = gpd.GeoDataFrame.from_features(sample_geojson).iloc[0]["geometry"]
    ds = DataSource(path("sample.geojson"), reader_kwargs={"mask": mask})
    expected = gpd.GeoDataFrame.from_features(sample_geojson).iloc[0]
    assert (ds.get_df()["geometry"] == expected["geometry"]).all()


def test_geo_data_is_made_valid(path: Callable[[str], str]) -> None:
    gdf = read_geo_data(path("france_germany_italy.geojson"))
    # France and Germany are broken, but read_geo_data should call `GeoDataFrame.make_valid`
    assert gdf.geometry.is_valid.all()
