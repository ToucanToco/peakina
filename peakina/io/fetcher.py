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
from typing import IO, Dict, List, Optional, Union
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

    registry: dict = {}

    def __init__(self, filepath: str, match: Optional[str] = None):
        self.filepath = filepath
        self.dirpath, self.basename = os.path.split(self.filepath)
        self.scheme = urlparse(filepath).scheme
        if match is None:
            self.match = None
            self.pattern = None
        else:
            self.match = MatchEnum(match)
            self.pattern = re.compile(self.basename)

    @classmethod
    def get_fetcher(cls, filepath: str, match: Optional[str] = None) -> 'Fetcher':
        scheme = urlparse(filepath).scheme
        return cls.registry[scheme](filepath, match)

    @abstractmethod
    def listdir(self, dirpath: str) -> List[str]:
        """List all the files in a directory"""

    @abstractmethod
    def open(self, filepath: str, **fetcher_kwargs) -> IO:
        """Same as builtins `open` method in text mode"""

    @abstractmethod
    def mtime(self, filepath: str) -> Optional[int]:
        """Get last modification time of a file"""

    def is_matching(self, filename: str) -> bool:
        if self.match is None:
            return self.basename == filename
        elif self.match is MatchEnum.GLOB:
            return fnmatch.fnmatch(filename, self.pattern.pattern)  # type: ignore
        else:
            return bool(self.pattern.match(filename))  # type: ignore

    def get_filepath_list(self) -> List[str]:
        """Methods to retrieve all the pathes to open"""
        if self.match is None:
            return [self.filepath]

        all_filenames = self.listdir(self.dirpath)
        matching_filenames = [f for f in all_filenames if self.is_matching(f)]
        return [os.path.join(self.dirpath, f) for f in sorted(matching_filenames)]

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

    def __init__(self, uri: str):
        self.fetcher = Fetcher.get_fetcher(uri)

    @property
    def uri(self):
        return self.fetcher.filepath

    @property
    def dirpath(self):
        return self.fetcher.dirpath

    @property
    def basename(self):
        return self.fetcher.basename

    @property
    def scheme(self):
        return self.fetcher.scheme

    def open(self):
        return self.fetcher.open(self.uri)

    def get_str_mtime(self):
        return self.fetcher.get_str_mtime(self.uri)

    def get_mtime_dict(self):
        return self.fetcher.get_mtime_dict(self.dirpath)
