"""
Common/Shared elements/classes between readers
"""

from typing import TypedDict


class PreviewArgs(TypedDict, total=False):
    nrows: int
    offset: int
