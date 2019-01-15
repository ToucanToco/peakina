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
from typing import IO, Any, Dict, Generator, List, Optional, Tuple, Union
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

    @staticmethod
    @abstractmethod
    def listdir(dirpath: str) -> List[str]:
        """List all the files in a directory"""

    @staticmethod
    @abstractmethod
    def open(filepath: str) -> IO:
        """Same as builtins `open` method in text mode"""

    @staticmethod
    @abstractmethod
    def mtime(filepath: str) -> Optional[int]:
        """Get last modification time of a file"""

    def get_filepath_list(self) -> List[str]:
        """Methods to retrieve all the pathes to open"""
        if self.match is None:
            return [self.filepath]

        all_filenames = self.listdir(self.dirpath)
        if self.match is MatchEnum.GLOB:
            matching_filenames = fnmatch.filter(all_filenames, self.pattern.pattern)
        else:
            matching_filenames = [f for f in all_filenames if self.pattern.match(f)]
        return [os.path.join(self.dirpath, f) for f in sorted(matching_filenames)]

    def get_files(self) -> Generator[Tuple[str, IO], Any, None]:
        """
        Actually call open on each filepath and retrieve a BinaryIO or TextIO
        Returns a generator of tuples (filename, IO)
        """
        return ((os.path.basename(f), self.open(f)) for f in self.get_filepath_list())

    @classmethod
    def get_str_mtime(cls, filepath: str) -> Optional[str]:
        mdtime = cls.mtime(filepath)
        return mdtm_to_string(mdtime) if mdtime else None

    @classmethod
    def get_mtime_dict(cls, dirpath: str) -> Dict[str, Optional[str]]:
        return {f: cls.get_str_mtime(os.path.join(dirpath, f)) for f in cls.listdir(dirpath)}
