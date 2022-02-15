import re

from peakina.io.fetcher import MatchEnum
from peakina.io.local.file_fetcher import FileFetcher


def test_file_fetcher(path):
    fetcher = FileFetcher()
    dirpath = path("")
    filepath = path("0_0.csv")

    assert fetcher.open(filepath).read() == "a,b\n0,0\n0,1"
    assert "0_0.csv" in fetcher.listdir(dirpath)
    assert fetcher.mtime(filepath) > 1e9
    assert (str_mtime := fetcher.get_str_mtime(filepath)) is not None
    assert len(str_mtime) == 20
    assert fetcher.get_mtime_dict(dirpath)["0_0.csv"] == str_mtime


def test_file_fetcher_mtime_oserror(mocker):
    fetcher = FileFetcher()
    mocker.patch.object(fetcher, "mtime").side_effect = OSError("oops")
    assert fetcher.get_str_mtime("whatever") is None


def test_file_fetcher_match(path):
    fetcher = FileFetcher()
    filename = "2020 Report (6).xlsx"
    assert fetcher.is_matching(filename, match=None, pattern=re.compile(filename))
    assert fetcher.is_matching(filename, match=MatchEnum.GLOB, pattern=re.compile(filename))
    assert not fetcher.is_matching(filename, match=MatchEnum.REGEX, pattern=re.compile(filename))
    assert fetcher.is_matching(
        filename, match=MatchEnum.REGEX, pattern=re.compile(r"2020 Report \(6\).xlsx")
    )
