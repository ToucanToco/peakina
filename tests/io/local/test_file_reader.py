from peakina.io.local.file_reader import FileReader


def test_file_reader(path):
    dirpath = path('')
    filepath = path('0_0.csv')

    assert FileReader.open(filepath).read() == 'a,b\n0,0\n0,1'
    assert '0_0.csv' in FileReader.listdir(dirpath)
    assert FileReader.mtime(filepath) > 1e9
    str_mtime = FileReader.get_str_mtime(filepath)
    assert len(str_mtime) == 20
    assert FileReader.get_mtime_dict(dirpath)['0_0.csv'] == str_mtime
