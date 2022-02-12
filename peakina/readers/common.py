"""
Common/Shared elements/classes between readers
"""

from pydantic.dataclasses import dataclass


@dataclass
class PreviewArgs:
    nrows: int = 500
    offset: int = 0
