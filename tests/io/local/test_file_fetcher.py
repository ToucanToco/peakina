from peakina.io.local.file_fetcher import FileFetcher


def test_file_fetcher(path):
    dirpath = path('')
    filepath = path('0_0.csv')

    assert FileFetcher.open(filepath).read() == 'a,b\n0,0\n0,1'
    assert '0_0.csv' in FileFetcher.listdir(dirpath)
    assert FileFetcher.mtime(filepath) > 1e9
    str_mtime = FileFetcher.get_str_mtime(filepath)
    assert len(str_mtime) == 20
    assert FileFetcher.get_mtime_dict(dirpath)['0_0.csv'] == str_mtime
