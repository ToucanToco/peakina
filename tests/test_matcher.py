from pytest import raises

from peakina.matcher import FTPMatcher, LocalMatcher, Matcher


def test_registry():
    assert Matcher._registry[''] == LocalMatcher
    assert Matcher._registry['ftp'] == FTPMatcher


def test_all_matches(path):
    assert Matcher.all_matches('no_match', '', None) == ['no_match']

    file_path = path(r'0_\d.csv')
    matches = Matcher.all_matches(file_path, '', match='regex')
    assert matches == [path('0_0.csv'), path('0_1.csv')]

    file_path = path(r'0_*.csv')
    matches = Matcher.all_matches(file_path, '', match='glob')
    assert matches == [path('0_0.csv'), path('0_0_sep.csv'), path('0_1.csv')]

    with raises(NotImplementedError) as e:
        Matcher.all_matches('http://no/matches/for/this/scheme', 'http', match='regex')
    assert str(e.value) == 'match is not yet implemented for scheme "http"'


def test_ftpmatcher(mocker):
    ftp_dir_mock = mocker.patch('peakina.matcher.ftp_listdir')
    fp = FTPMatcher('ftps://user:pw@server.com/pika/chu.csv', scheme='ftps', match='regex')
    assert fp.dirname == 'ftps://user:pw@server.com/pika'
    fp.listdir('a_dir')
    ftp_dir_mock.assert_called_once_with('a_dir')
