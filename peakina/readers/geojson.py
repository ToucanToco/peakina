from functools import wraps
from typing import Any, Optional
import geopandas as gpd


@wraps(gpd.read_file)
def read_file(
    path: str, preview_offset: int = 0, preview_nrows: Optional[int] = None, **kwargs: Any
) -> gpd.GeoDataFrame:
    return gpd.read_file(path, rows=slice(preview_offset, preview_nrows))
