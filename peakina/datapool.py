from os import path
from typing import TYPE_CHECKING, Any, Dict, Hashable, Optional

from peakina.cache import Cache
from peakina.datasource import DataSource

if TYPE_CHECKING:
    import pandas as pd


class DataPool:
    def __init__(
        self,
        config: Dict[Hashable, Dict[str, Any]],
        data_sources_dir: str = "",
        cache: Optional[Cache] = None,
    ) -> None:
        self.cache = cache
        self.datasources: Dict[Hashable, DataSource] = {}
        for ds_id, ds_conf in config.items():
            ds = DataSource(**ds_conf)

            # change local path into absolute path
            if ds.scheme == "" and not path.isabs(ds.uri):
                ds.uri = path.join(data_sources_dir, ds.uri)

            self.datasources[ds_id] = ds

    def __contains__(self, item: Hashable) -> bool:
        return item in self.datasources

    def __getitem__(self, item: Hashable) -> "pd.DataFrame":
        return self.datasources[item].get_df(cache=self.cache)

    def __len__(self) -> int:
        return len(self.datasources)
