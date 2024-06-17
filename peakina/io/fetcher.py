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
from typing import IO, Any, Callable, Pattern, TypeVar
from urllib.parse import urlparse

from peakina.helpers import mdtm_to_string

F = TypeVar("F", bound=type["Fetcher"])


class MatchEnum(str, Enum):
    REGEX = "regex"
    GLOB = "glob"


def register(schemes: str | list[str]) -> Callable[[F], F]:
    if isinstance(schemes, str):
        schemes = [schemes]

    def f(cls: F) -> F:
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

    registry: dict[str, type["Fetcher"]] = {}

    def __init__(self, **kwargs: Any) -> None:
        self.extra_kwargs = kwargs

    @classmethod
    def get_fetcher(cls, filepath: str, **fetcher_kwargs: Any) -> "Fetcher":
        scheme = urlparse(filepath).scheme
        return cls.registry[scheme](**fetcher_kwargs)

    @abstractmethod
    def listdir(self, dirpath: str) -> list[str]:
        """list all the files in a directory"""

    @abstractmethod
    def open(self, filepath: str) -> IO[bytes] | IO[str]:
        """Same as builtins `open` method in text mode"""

    @abstractmethod
    def mtime(self, filepath: str) -> int | None:
        """Get last modification time of a file"""

    @staticmethod
    def is_matching(filename: str, match: MatchEnum | None, pattern: Pattern[str]) -> bool:
        if match is None:
            return bool(filename == pattern.pattern)
        elif match is MatchEnum.GLOB:
            return bool(fnmatch.fnmatch(filename, pattern.pattern))
        else:
            return bool(pattern.match(filename))

    def get_filepath_list(self, filepath: str, match: MatchEnum | None = None) -> list[str]:
        """Methods to retrieve all the pathes to open"""
        if match is None:
            return [filepath]

        dirpath, basename = os.path.split(filepath)
        pattern = re.compile(basename)
        all_filenames = self.listdir(dirpath)
        matching_filenames = [f for f in all_filenames if self.is_matching(f, match, pattern)]
        return [os.path.join(dirpath, f) for f in sorted(matching_filenames)]

    def get_str_mtime(self, filepath: str) -> str | None:
        try:
            mdtime = self.mtime(filepath)
        except (NotImplementedError, KeyError, OSError):
            mdtime = None
        return mdtm_to_string(mdtime) if mdtime else None

    def get_mtime_dict(self, dirpath: str) -> dict[str, str | None]:
        return {f: self.get_str_mtime(os.path.join(dirpath, f)) for f in self.listdir(dirpath)}


class fetch:
    """class providing shortcuts for some Fetcher operations"""

    def __init__(self, uri: str, **fetcher_kwargs: Any) -> None:
        self.uri = uri
        self.fetcher = Fetcher.get_fetcher(uri, **fetcher_kwargs)

    @property
    def scheme(self) -> str:
        return urlparse(self.uri).scheme

    @property
    def dirpath(self) -> str:
        return os.path.dirname(self.uri)

    @property
    def basename(self) -> str:
        return os.path.basename(self.uri)

    def open(self) -> IO[bytes] | IO[str]:
        return self.fetcher.open(self.uri)

    def get_str_mtime(self) -> str | None:
        return self.fetcher.get_str_mtime(self.uri)

    def get_mtime_dict(self) -> dict[str, str | None]:
        dirpath, basename = os.path.split(self.uri)
        return self.fetcher.get_mtime_dict(dirpath)
