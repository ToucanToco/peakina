import importlib.metadata as lib_meta

from .cache import Cache, CacheEnum
from .datapool import DataPool
from .datasource import AVAILABLE_SCHEMES, DataSource, read_pandas
from .helpers import TypeEnum
from .io import MatchEnum, fetch

__version__ = lib_meta.version(__package__ or __name__)

__all__ = (
    "AVAILABLE_SCHEMES",
    "Cache",
    "CacheEnum",
    "DataPool",
    "DataSource",
    "MatchEnum",
    "TypeEnum",
    "fetch",
    "read_pandas",
)
