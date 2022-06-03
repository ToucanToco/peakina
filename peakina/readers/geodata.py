from functools import wraps
from typing import Any, Optional

import geopandas as gpd


@wraps(gpd.read_file)
def read_geo_data(
    path: str, preview_offset: int = 0, preview_nrows: Optional[int] = None, **kwargs: Any
) -> gpd.GeoDataFrame:
    if preview_nrows and not preview_offset:
        return gpd.read_file(path, rows=preview_nrows, **kwargs)
    else:
        return gpd.read_file(
            path, rows=slice(preview_offset, preview_nrows + 1 if preview_nrows else None), **kwargs
        )
