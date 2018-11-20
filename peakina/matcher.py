import fnmatch
import os
import re
from abc import abstractmethod
from enum import Enum
from pathlib import PurePath
from typing import List, Pattern
from urllib.parse import urlparse, urlsplit, urlunsplit

from peakina.io.ftp_utils import ftp_listdir, ftp_schemes


class MatchEnum(str, Enum):
    REGEX = 'regex'
    GLOB = 'glob'


class Matcher:
    """Matcher base class"""

    _registry: dict = {}  # {'': LocalMatcher, 'ftp': FTPMatcher...}

    @classmethod
    def __init_subclass__(cls, *, schemes: list):
        super().__init_subclass__()
        for scheme in schemes:
            cls._registry[scheme] = cls

    def __init__(self, file_path, scheme, match):
        self.file_path = file_path
        self.scheme = scheme
        self.match_type = MatchEnum(match)

    @property
    def basename(self) -> str:
        return os.path.basename(self.file_path)

    @property
    def dirname(self) -> str:
        return os.path.dirname(self.file_path)

    @property
    def pattern(self) -> Pattern:
        return re.compile(self.basename)

    @classmethod
    @abstractmethod
    def listdir(cls, dirname) -> List[str]:
        """TO IMPLEMENT"""

    def all_matches_(self) -> List[str]:
        all_filenames = self.listdir(self.dirname)
        if self.match_type is MatchEnum.GLOB:
            matching_filenames = fnmatch.filter(all_filenames, self.pattern.pattern)
        else:
            matching_filenames = [f for f in all_filenames if self.pattern.match(f)]
        return [os.path.join(self.dirname, f) for f in sorted(matching_filenames)]

    @classmethod
    def all_matches(cls, file_path, scheme=None, match=None) -> List[str]:
        """Main method to retrieve the list of matching file paths with `self.pattern`"""
        if match is None:
            return [file_path]

        scheme = scheme or urlparse(file_path).scheme
        match = MatchEnum(match)
        try:
            return cls._registry[scheme](file_path, scheme, match).all_matches_()
        except KeyError:
            raise NotImplementedError(f'match is not yet implemented for scheme "{scheme}"')


class LocalMatcher(Matcher, schemes=['']):
    @classmethod
    def listdir(cls, dirname):
        return os.listdir(dirname)


class FTPMatcher(Matcher, schemes=ftp_schemes):
    def __init__(self, file_path, scheme, match):
        super().__init__(file_path, scheme, match)
        o = urlsplit(self.file_path)
        self.path = PurePath('?'.join(e for e in o[2:] if e != ''))
        self.folder_url = urlunsplit(o[:2] + (str(self.path.parents[0]), '', '')).rstrip('/')

    @property
    def dirname(self) -> str:
        return self.folder_url

    @classmethod
    def listdir(cls, dirname) -> List[str]:
        return ftp_listdir(dirname)
