"""
This module provides access to the base Fetcher class and its registry
where all the fetchers are saved.
The main purpose of this fetcher is to retrieve a file or a list of files in case of a regex
filepath by calling the right registered subclass (local, ftp...) matching the filepath scheme.
The subclasses are all declared in the `io` directory.
"""
import fnmatch
import os
import re
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import IO, Dict, List, Optional, Pattern, Type, Union
from urllib.parse import urlparse

from peakina.helpers import mdtm_to_string


class MatchEnum(str, Enum):
    REGEX = 'regex'
    GLOB = 'glob'


def register(schemes: Union[str, List[str]]):
    if isinstance(schemes, str):
        schemes = [schemes]

    def f(cls):
        for scheme in schemes:
            cls.registry[scheme] = cls
        return cls

    return f


class Fetcher(metaclass=ABCMeta):
    """
    Base class used to:
     - list the files in a directory
     - retrieve the last modification time of a file
     - retrieve one (or many in case of a regex) TextIO from a path
    This class is used by calling `get_fetcher`, which reads the scheme of the path
    ('ftp', 's3', ...) and redirects to the right fetcher.
    All the `Fetcher` subclasses need to implement basic methods in order to be used properly.
    """

    registry: Dict[str, Type['Fetcher']] = {}

    def __init__(self, **kwargs):
        self.extra_kwargs = kwargs

    @classmethod
    def get_fetcher(cls, filepath: str, **fetcher_kwargs) -> 'Fetcher':
        scheme = urlparse(filepath).scheme
        return cls.registry[scheme](**fetcher_kwargs)

    @abstractmethod
    def listdir(self, dirpath: str) -> List[str]:
        """List all the files in a directory"""

    @abstractmethod
    def open(self, filepath: str) -> IO:
        """Same as builtins `open` method in text mode"""

    @abstractmethod
    def mtime(self, filepath: str) -> Optional[int]:
        """Get last modification time of a file"""

    @staticmethod
    def is_matching(filename: str, match: MatchEnum, pattern: Pattern) -> bool:
        if match is MatchEnum.GLOB:
            return bool(fnmatch.fnmatch(filename, pattern.pattern))
        else:
            return bool(pattern.match(filename))

    def get_filepath_list(self, filepath: str, match: Optional[MatchEnum] = None) -> List[str]:
        """Methods to retrieve all the pathes to open"""
        if match is None:
            return [filepath]

        dirpath, basename = os.path.split(filepath)
        pattern = re.compile(basename)
        all_filenames = self.listdir(dirpath)
        matching_filenames = [f for f in all_filenames if self.is_matching(f, match, pattern)]
        return [os.path.join(dirpath, f) for f in sorted(matching_filenames)]

    def get_str_mtime(self, filepath: str) -> Optional[str]:
        try:
            mdtime = self.mtime(filepath)
        except (NotImplementedError, KeyError, OSError):
            mdtime = None
        return mdtm_to_string(mdtime) if mdtime else None

    def get_mtime_dict(self, dirpath: str) -> Dict[str, Optional[str]]:
        return {f: self.get_str_mtime(os.path.join(dirpath, f)) for f in self.listdir(dirpath)}


class fetch:
    """class providing shortcuts for some Fetcher operations"""

    def __init__(self, uri: str, **fetcher_kwargs):
        self.uri = uri
        self.fetcher = Fetcher.get_fetcher(uri, **fetcher_kwargs)

    @property
    def scheme(self):
        return urlparse(self.uri).scheme

    @property
    def dirpath(self):
        return os.path.dirname(self.uri)

    @property
    def basename(self):
        return os.path.basename(self.uri)

    def open(self) -> IO:
        return self.fetcher.open(self.uri)

    def get_str_mtime(self) -> Optional[str]:
        return self.fetcher.get_str_mtime(self.uri)

    def get_mtime_dict(self) -> Dict[str, Optional[str]]:
        dirpath, basename = os.path.split(self.uri)
        return self.fetcher.get_mtime_dict(dirpath)
