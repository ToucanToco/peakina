import os

from peakina import fetch


def test_fetch(path):
    f = fetch(path("0_0.csv"))
    assert os.path.split(f.uri) == (f.dirpath, f.basename)
    assert f.scheme == ""

    assert f.open().read() == "a,b\n0,0\n0,1"
    assert (str_mtime := f.get_str_mtime()) is not None
    assert str_mtime.endswith("Z")
    assert (mtime_0_0 := f.get_mtime_dict()["0_0.csv"]) is not None
    assert mtime_0_0.endswith("Z")
