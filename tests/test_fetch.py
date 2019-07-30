import os

from peakina import fetch


def test_fetch(path):
    f = fetch(path('0_0.csv'))
    assert os.path.split(f.uri) == (f.dirpath, f.basename)
    assert f.scheme == ''

    assert f.open().read() == 'a,b\n0,0\n0,1'
    assert f.get_str_mtime().endswith('Z')
    assert f.get_mtime_dict()['0_0.csv'].endswith('Z')
