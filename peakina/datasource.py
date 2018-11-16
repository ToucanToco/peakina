import logging
from enum import Enum

import pandas as pd
from pydantic.dataclasses import dataclass

from .helpers import detect_encoding, detect_sep, detect_type, validate_encoding

logger = logging.getLogger(__name__)


class TypeEnum(str, Enum):
    CSV = 'csv'
    EXCEL = 'excel'
    JSON = 'json'


class MatchEnum(str, Enum):
    REGEX = 'regex'
    GLOB = 'glob'


@dataclass
class DataSource:
    file: str
    type: TypeEnum = None
    encoding: str = None
    sep: str = None
    match: MatchEnum = None

    def __post_init__(self):
        self.files = [self.file]

    @staticmethod
    def _get_single_df(file_path: str, file_type: str, encoding: str, sep: str, **kwargs):
        if file_type is None:
            file_type = TypeEnum(detect_type(file_path))
        if not validate_encoding(file_path, encoding):
            encoding = detect_encoding(file_path)
        if sep is None:
            sep = detect_sep(file_path, encoding)
        pd_read = getattr(pd, f'read_{file_type}')
        return pd_read(file_path, encoding=encoding, sep=sep, **kwargs)

    def get_df(self, **kwargs):
        return pd.concat(
            [
                self._get_single_df(file_path, self.type, self.encoding, self.sep, **kwargs)
                for file_path in self.files
            ]
        )
