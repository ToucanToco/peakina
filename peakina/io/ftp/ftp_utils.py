import ftplib
import logging
import os
import re
import socket
import ssl
import tempfile
from contextlib import contextmanager, suppress
from datetime import datetime
from functools import partial
from ipaddress import ip_address
from os.path import basename, join
from time import sleep
from typing import IO, Any, Callable, ContextManager, Generator, cast
from urllib.parse import ParseResult, quote, unquote, urlparse

import paramiko

FTP_SCHEMES = ["ftp", "ftps", "sftp"]
_DEFAULT_MAX_TIMEOUT_SECONDS = 30
_DEFAULT_MAX_RETRY = 7

FTPClient = ftplib.FTP | paramiko.SFTPClient


class FTPS(ftplib.FTP_TLS):
    ssl_version = ssl.PROTOCOL_TLSv1_2

    def connect(  # type: ignore[override]
        self, host: str, port: int | None, timeout: int = 60
    ) -> str:
        self.host = host
        self.port = port or 990
        self.timeout = timeout

        def _setup_sock() -> socket.socket:
            _sock = socket.create_connection((self.host, self.port), self.timeout)
            self.af = _sock.family
            return _sock

        try:
            self.sock = self.context.wrap_socket(_setup_sock(), server_hostname=self.host)
        except ssl.SSLError:  # pragma: no cover
            # in some cases we must fallback to:
            self.sock = _setup_sock()

        self.file = self.sock.makefile("r")
        self.welcome = self.getresp()
        return self.welcome

    def ntransfercmd(  # type: ignore[override]
        self, cmd: str, rest: str | None = None
    ) -> tuple[socket.socket, int]:
        # override ntransfercmd so it reuses the sock session, to prevent SSLEOFError.
        # cf. https://stackoverflow.com/questions/40536061/ssleoferror-on-ftps-using-python-ftplib
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:  # type: ignore[attr-defined]
            conn = self.context.wrap_socket(
                conn,
                server_hostname=self.host,
                session=self.sock.session,  # type: ignore[union-attr]
            )  # this is the fix
        return conn, size  # type:ignore[return-value] # size could be None here

    def makepasv(self) -> tuple[str, int]:
        # override makepasv so it rewrites the dst address if the server gave a broken one.
        # Inspired by:
        # https://github.com/lavv17/lftp/blob/d67fc14d085849a6b0418bb3e912fea2e94c18d1/src/ftpclass.cc#L774
        host, port = super().makepasv()
        if (
            self.af == socket.AF_INET and ip_address(host).is_private and self.sock is not None
        ):  # pragma: no cover
            host = self.sock.getpeername()[0]
        return host, port


@contextmanager
def ftps_client(params: ParseResult) -> Generator[tuple[FTPS, str], None, None]:
    ftps = FTPS()
    try:
        ftps.connect(
            host=params.hostname or "", port=params.port, timeout=_DEFAULT_MAX_TIMEOUT_SECONDS
        )
        try:
            ftps.prot_p()
            ftps.login(user=params.username or "", passwd=params.password or "")
        except Exception as e:
            if "SSL/TLS required on the control channel" in str(e):
                # This error means we should try the other way: first login, then prot_p:
                ftps.login(user=params.username or "", passwd=params.password or "")
                ftps.prot_p()
            else:
                raise

        yield ftps, params.path

    finally:
        with suppress(Exception):
            ftps.quit()


@contextmanager
def ftp_client(params: ParseResult) -> Generator[tuple[ftplib.FTP, str], None, None]:
    port = params.port or 21
    ftp = ftplib.FTP()
    try:
        ftp.connect(host=params.hostname or "", port=port, timeout=_DEFAULT_MAX_TIMEOUT_SECONDS)
        ftp.login(user=params.username or "", passwd=params.password or "")
        yield ftp, params.path

    finally:
        with suppress(Exception):
            ftp.quit()


@contextmanager
def sftp_client(params: ParseResult) -> Generator[tuple[paramiko.SFTPClient, str], None, None]:
    port = params.port or 22
    ssh_client = paramiko.SSHClient()
    try:
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=params.hostname or "",
            username=params.username,
            password=params.password,
            port=port,
            timeout=_DEFAULT_MAX_TIMEOUT_SECONDS,
        )
        sftp = ssh_client.open_sftp()
        yield sftp, params.path

    finally:
        # In cae of Exception, we don't want to raise it
        with suppress(AttributeError):
            logging.getLogger(__name__).warning("Unable to close the Connection the SSHConnection.")
            ssh_client.close()


def _urlparse(url: str) -> ParseResult:
    """
    We had several cases of ftp usernames having '#' in them which confuses urlparse.
    The # is a reserved char for 'fragment identifiers':
    https://en.wikipedia.org/wiki/Fragment_identifier
    """
    url_params = urlparse(quote(url, safe=":/@"))  # the '#' is marked as unsafe
    # unquote to get back the original bits of string
    return ParseResult(*[unquote(param) for param in url_params])


def client(url: str) -> ContextManager[tuple[FTPClient, str]]:
    parse_result = _urlparse(url)
    ftp_client_mapping: dict[
        str, Callable[[ParseResult], ContextManager[tuple[FTPClient, str]]]
    ] = {
        "ftp": ftp_client,
        "ftps": ftps_client,
        "sftp": sftp_client,
    }
    return ftp_client_mapping[parse_result.scheme](parse_result)


def retry_pasv(c: ftplib.FTP, cmd: str, *args: Any) -> Any:
    """
    Some servers accept the PASV command, but timeout when doing commands using it.
    Solution is to retry a timeout-ed command in active mode.
    """
    fun = partial(getattr(c, cmd), *args)
    try:
        return fun()
    except socket.timeout:
        c.set_pasv(False)
        return fun()


def _open(url: str) -> IO[bytes]:
    extension = url.split(".")[-1]
    ret = tempfile.NamedTemporaryFile(suffix=f".{extension}")
    with client(url) as (c, path):
        try:
            retry_pasv(cast(ftplib.FTP, c), "retrbinary", f"RETR {path}", ret.write)
        except AttributeError:
            cast(paramiko.SFTPClient, c).getfo(path, ret)
        except ftplib.error_perm as e:
            raise Exception(f"Can't open file {path}. Please make sure the file exists") from e

    ret.seek(0)
    return ret


def ftp_open(url: str, retry: int = _DEFAULT_MAX_RETRY) -> IO[bytes]:  # type: ignore
    for i in range(1, retry + 1):
        try:
            return _open(url)
        except (AttributeError, OSError, ftplib.error_temp, paramiko.SSHException) as e:
            log = logging.getLogger(__name__)

            # FileNotFoundError inherits from OSError
            # We need to log that we're not seeing the specified file
            if isinstance(e, FileNotFoundError):  # pragma: no cover
                log.warning(
                    f"File '{os.path.basename(url)}' not available inside : "
                    f"'{os.path.dirname(urlparse(url).path)}' !"
                )

            sleep_time = 2 * i**2
            logging.getLogger(__name__).warning(f"Retry #{i}: Sleeping {sleep_time}s because {e}")
            sleep(sleep_time)


def _get_all_files(c: FTPClient, path: str) -> list[str]:
    try:
        # retry_pasv returns path + the file
        return [basename(x) for x in retry_pasv(cast(ftplib.FTP, c), "nlst", path)]
    except AttributeError:
        if not path:
            path = "."
        return cast(paramiko.SFTPClient, c).listdir(path)


def ftp_listdir(url: str) -> list[str]:
    with client(url) as (c, path):
        return _get_all_files(c, path)


def _get_mtime(c: FTPClient, path: str) -> int | None:
    """Returns timestamp of last modification"""
    try:
        mdtm = cast(ftplib.FTP, c).sendcmd("MDTM " + path)
        # mdtm-response = "213" SP time-val CRLF (e.g. '20180101203000')
        # some FTP servers response include the milliseconds (e.g. '20180101203000.123')
        mtime = re.search(r"^213 (\d+)(\.\d+)?$", mdtm)
        assert mtime is not None
        dt = datetime.strptime(mtime.group(1), "%Y%m%d%H%M%S")
        return int((dt - datetime(1970, 1, 1)).total_seconds())
    except AttributeError:
        return cast(paramiko.SFTPClient, c).stat(path).st_mtime
    except ftplib.error_perm as e:
        logging.getLogger(__name__).warning(
            f"Can't open file {path}. Please make sure the file exists: {e}"
        )
        return None


def ftp_mtime(url: str) -> int | None:
    with client(url) as (cl_ftp, path):
        return _get_mtime(cl_ftp, path)


def dir_mtimes(url: str) -> dict[str, int | None]:
    with client(url) as (cl_ftp, path):
        mtimes_dir = {}
        all_files = _get_all_files(cl_ftp, path)
        for file in all_files:
            mtimes_dir[file] = _get_mtime(cl_ftp, join(path, file))
    return mtimes_dir
