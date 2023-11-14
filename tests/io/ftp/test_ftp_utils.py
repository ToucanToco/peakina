import ftplib
import os
import socket
import ssl
from urllib.parse import ParseResult

from paramiko.ssh_exception import SSHException
from pytest import fixture, raises
from pytest_mock import MockFixture

from peakina.io.ftp.ftp_utils import (
    _DEFAULT_MAX_TIMEOUT_SECONDS,
    dir_mtimes,
    ftp_listdir,
    ftp_mtime,
    ftp_open,
    sftp_client,
)


@fixture
def ftp_client(mocker):
    client_mock = mocker.patch("peakina.io.ftp.ftp_utils.client")
    c_ftp = mocker.MagicMock()
    client_mock.return_value.__enter__.return_value = c_ftp, "path"
    return c_ftp


def test_open(ftp_client, mocker):
    ret = ftp_open(url="foo.xls")
    assert ret.name.endswith(".xls")  # check the suffix is the expected one
    assert os.path.exists(ret.name)
    ftp_client.retrbinary.assert_called_once()
    ret.close()
    assert not os.path.exists(ret.name)  # tmp file is deleted on .close()
    ftp_listdir(url="foo")
    ftp_client.nlst.assert_called_once()

    ftp_client.nlst.side_effect = socket.timeout
    with raises(socket.timeout):
        ftp_listdir(url="foo")

    mocker.patch("peakina.io.ftp.ftp_utils.retry_pasv").side_effect = AttributeError
    ret = ftp_open(url="foo.xls")
    assert ret.name.endswith(".xls")  # check the suffix is the expected one
    assert os.path.exists(ret.name)
    ftp_client.getfo.assert_called_once()
    ret.close()
    assert not os.path.exists(ret.name)  # tmp file is deleted on .close()
    ftp_listdir(url="foo")
    ftp_client.listdir.assert_called_once_with("path")

    mocker.patch("peakina.io.ftp.ftp_utils.retry_pasv").side_effect = ftplib.error_perm
    with raises(Exception) as e:
        ftp_open(url="foo")
    assert str(e.value) == "Can't open file path. Please make sure the file exists"


def test_ftp_dir(mocker):
    """nlst now returns the parent dir. We should retrieve only the filenames"""
    client_mock = mocker.patch("peakina.io.ftp.ftp_utils.client")
    client_mock.return_value.__enter__.return_value = (ftplib.FTP_TLS(), "path")
    mocker.patch("ftplib.FTP.nlst").return_value = ["/somepath/file1.csv", "/somepath/file2.csv"]
    assert ftp_listdir("ftps://somepath") == ["file1.csv", "file2.csv"]


def test_retry_open(mocker):
    mocker.patch("peakina.io.ftp.ftp_utils._open").side_effect = [
        ftplib.error_temp("421 Could not create socket"),
        AttributeError("'NoneType' object has no attribute 'sendall'"),
        OSError("Random OSError"),
        SSHException("Random connection dropped error"),
        "ok",
    ]
    mock_sleep = mocker.patch("peakina.io.ftp.ftp_utils.sleep")

    ret = ftp_open(url="foo")
    calls = [mocker.call(2), mocker.call(8), mocker.call(18)]
    mock_sleep.assert_has_calls(calls)
    assert ret == "ok"  # type: ignore[comparison-overlap]


def test_get_mtime(ftp_client):
    ftp_client.sendcmd.return_value = "213 20180101203000"
    assert ftp_mtime(url="foo") == 1_514_838_600

    ftp_client.sendcmd.return_value = "213 20180101203000.123"
    assert ftp_mtime(url="foo") == 1_514_838_600

    ftp_client.sendcmd.side_effect = AttributeError
    ftp_client.stat.return_value.st_mtime = 1_514_835_000
    assert ftp_mtime(url="foo") == 1_514_835_000

    ftp_client.sendcmd.side_effect = ftplib.error_perm("zbruh")
    assert ftp_mtime(url="foo") is None


def test_dir_mtimes(ftp_client, mocker):
    mocker.patch("peakina.io.ftp.ftp_utils._get_all_files").return_value = [
        "file1.csv",
        "file2.csv",
        "file3.csv",
    ]
    get_mtime_mock = mocker.patch("peakina.io.ftp.ftp_utils._get_mtime")
    get_mtime_mock.side_effect = ["mtime1", "mtime2", None]
    assert dir_mtimes("my_url") == {"file1.csv": "mtime1", "file2.csv": "mtime2", "file3.csv": None}
    assert get_mtime_mock.call_args[0][1].startswith("path/file")


def test_ftp_client(mocker):
    mock_ftp_client = mocker.patch("ftplib.FTP").return_value

    url = "ftp://sacha@ondine.com:123/picha/chu.csv"
    ftp_open(url)

    mock_ftp_client.connect.assert_called_once_with(
        host="ondine.com", port=123, timeout=_DEFAULT_MAX_TIMEOUT_SECONDS
    )
    mock_ftp_client.login.assert_called_once_with(passwd="", user="sacha")
    mock_ftp_client.quit.assert_called_once()


def test_ftp_client_quit_resilience(mocker):
    """it should never crash on ftp connection teardown"""
    mock_ftp_client = mocker.patch("ftplib.FTP").return_value
    mock_ftp_client.quit.side_effect = Exception("test")

    ftp_open("ftp://sacha@ondine.com:123/picha/chu.csv")

    mock_ftp_client.quit.assert_called_once()


def test_ftps_client(mocker):
    mock_ftps_client = mocker.patch("peakina.io.ftp.ftp_utils.FTPS").return_value
    url = "ftps://sacha@ondine.com:123/picha/chu.csv"
    ftp_open(url)

    mock_ftps_client.connect.assert_called_once_with(
        host="ondine.com", port=123, timeout=_DEFAULT_MAX_TIMEOUT_SECONDS
    )
    mock_ftps_client.login.assert_called_once_with(passwd="", user="sacha")
    mock_ftps_client.quit.assert_called_once()


def test_ftps_client_ssl_required_on_control_channel(mocker):
    mock_ftps_client = mocker.patch("peakina.io.ftp.ftp_utils.FTPS").return_value
    mock_ftps_client.prot_p.side_effect = [
        ssl.SSLError("SSL/TLS required on the control channel"),
        None,
    ]
    url = "ftps://sacha@ondine.com:123/picha/chu.csv"
    ftp_open(url)

    mock_ftps_client.connect.call_count == 2
    mock_ftps_client.prot_p.call_count == 2
    mock_ftps_client.login.call_count == 1
    mock_ftps_client.login.assert_called_once_with(passwd="", user="sacha")
    mock_ftps_client.quit.call_count == 2


def test_ftps_client_other_error(mocker):
    mock_ftps_client = mocker.patch("peakina.io.ftp.ftp_utils.FTPS").return_value
    mock_ftps_client.prot_p.side_effect = [ssl.SSLError("meh"), None]
    url = "ftps://sacha@ondine.com:123/picha/chu.csv"
    ftp_open(url)
    mock_ftps_client.login.call_count == 0  # never called


def test_ftps_client_quit_resilience(mocker):
    mock_ftps_client = mocker.patch("peakina.io.ftp.ftp_utils.FTPS").return_value
    mock_ftps_client.quit.side_effect = Exception("test")

    ftp_open("ftps://sacha@ondine.com:123/picha/chu.csv")  # Should not crash

    mock_ftps_client.quit.assert_called_once()


def test_sftp_client(mocker):
    mock_ssh_client = mocker.patch("paramiko.SSHClient").return_value

    url = "sftp://id#de@me*de:randompass@atat.com:666/pika/chu.csv"
    ftp_open(url)

    mock_ssh_client.connect.assert_called_once_with(
        timeout=_DEFAULT_MAX_TIMEOUT_SECONDS,
        hostname="atat.com",
        port=666,
        username="id#de@me*de",
        password="randompass",
    )
    mock_ssh_client.open_sftp.assert_called_once()
    mock_ssh_client.close.assert_called_once()

    cl_ftp = mock_ssh_client.open_sftp.return_value
    cl_ftp.sendcmd.side_effect = AttributeError
    ftp_mtime(url)
    cl_ftp.stat.assert_called_once_with("/pika/chu.csv")

    cl_ftp.nlst.side_effect = AttributeError
    url = "sftp://id#de@me*de:randompass@atat.com:666"
    ftp_listdir(url)
    cl_ftp.listdir.assert_called_once_with(".")


def test_sftp_client_silent_close(mocker: MockFixture) -> None:
    invalid_params = ParseResult(scheme="", netloc="", path="", params="", query="", fragment="")
    ssh_client = mocker.patch("paramiko.SSHClient")
    ssh_client.return_value.close.side_effect = AttributeError("NoneType doesnt have .close()")

    with sftp_client(invalid_params) as (sftp, _):
        # This block should raise an exception due to invalid parameters
        # The exception is expected to be suppressed by the context manager in the finally block
        # So, the test will pass if no exception propagates beyond this point

        assert sftp.get_channel()
