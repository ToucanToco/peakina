from os import path
from typing import Dict, Hashable

from .cache import Cache
from .datasource import DataSource


class DataPool:
    def __init__(
        self, config: Dict[Hashable, dict], data_sources_dir: str = '', cache: Cache = None
    ):
        self.cache = cache
        self.datasources: dict = {}
        for ds_id, ds_conf in config.items():
            ds = DataSource(**ds_conf)

            # change local path into absolute path
            if ds.scheme == '' and not path.isabs(ds.uri):
                ds.uri = path.join(data_sources_dir, ds.uri)

            self.datasources[ds_id] = ds

    def __contains__(self, item):
        return item in self.datasources

    def __getitem__(self, item):
        return self.datasources[item].get_df(cache=self.cache)

    def __len__(self):
        return len(self.datasources)
