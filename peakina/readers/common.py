"""
Common/Shared elements/classes between readers
"""

from typing import List

from pydantic.dataclasses import dataclass


@dataclass
class PreviewArgs:
    nrows: int = 500
    offset: int = 0


def _extract_columns(filepath: str, encoding: str, sep: str) -> List[str]:
    with open(filepath, buffering=10000, encoding=encoding) as ff:
        return ff.readline().replace("\n", "").split(sep)
