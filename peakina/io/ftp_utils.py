import ftplib
import logging
import re
import socket
import ssl
import tempfile
from datetime import datetime
from functools import partial
from os.path import basename
from time import sleep
from typing.io import BinaryIO
from urllib.parse import ParseResult, quote, unquote, urlparse

import paramiko

ftp_schemes = ['ftp', 'sftp', 'ftps']


def _urlparse(url) -> ParseResult:
    """
    We had several cases of ftp usernames having '#' in them which confuses urlparse.
    The # is a reserved char for 'fragment identifiers':
    https://en.wikipedia.org/wiki/Fragment_identifier
    """
    url_params = urlparse(quote(url, safe=':/@'))  # the '#' is marked as unsafe
    # unquote to get back the original bits of string
    url_params = tuple(unquote(param) for param in tuple(url_params))
    return ParseResult._make(url_params)


class FTPS(ftplib.FTP_TLS):
    def connect(self, host, port, timeout):
        self.host = host
        self.port = port or 990
        self.timeout = timeout

        self.sock = socket.create_connection((self.host, self.port), self.timeout)
        self.af = self.sock.family
        self.sock = ssl.wrap_socket(
            self.sock, self.keyfile, self.certfile, ssl_version=ssl.PROTOCOL_TLSv1
        )
        self.file = self.sock.makefile('r')
        self.welcome = self.getresp()
        return self.welcome


class FTPRegister:
    _registry: dict = {}

    @classmethod
    def __init_subclass__(cls, *, scheme):
        cls._registry[scheme] = cls
        super().__init_subclass__()

    def __init__(self, params: ParseResult):
        self.params = params
        self.path = params.path
        self.client = None

    @classmethod
    def get_connection(cls, url: str):
        params = _urlparse(url)
        return cls._registry[params.scheme](params)


class ftps_connection(FTPRegister, scheme='ftps'):
    def __enter__(self):
        self.client = FTPS()
        self.client.connect(host=self.params.hostname, port=self.params.port, timeout=3)
        self.client.prot_p()
        self.client.login(user=self.params.username, passwd=self.params.password)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.quit()


class ftp_connection(FTPRegister, scheme='ftp'):
    def __enter__(self):
        self.client = ftplib.FTP()
        self.client.connect(host=self.params.hostname, port=self.params.port or 21, timeout=3)
        self.client.login(user=self.params.username, passwd=self.params.password)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.quit()


class sftp_connection(FTPRegister, scheme='sftp'):
    def __enter__(self):
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(
            hostname=self.params.hostname,
            username=self.params.username,
            password=self.params.password,
            port=self.params.port or 22,
            timeout=3,
        )
        self.client = self.ssh_client.open_sftp()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ssh_client.close()


def connection(url):
    return FTPRegister.get_connection(url)


def retry_pasv(client, cmd, *args):
    """
    Some servers accept the PASV command, but timeout when doing commands using it.
    Solution is to retry a timeout-ed command in active mode.
    """
    fun = partial(getattr(client, cmd), *args)
    try:
        return fun()
    except socket.timeout:
        client.set_pasv(False)
        return fun()


def _open(client, path) -> BinaryIO:
    ret = tempfile.NamedTemporaryFile(suffix='.ftptmp')
    try:
        retry_pasv(client, 'retrbinary', f'RETR {path}', ret.write)
    except AttributeError:
        client.getfo(path, ret)
    except ftplib.error_perm as e:
        raise Exception(f'Cannot open file "{path}". Please make sure the file exists') from e
    ret.seek(0)
    return ret


def ftp_open(url: str, retry: int = 4) -> BinaryIO:
    for i in range(1, retry + 1):
        try:
            with connection(url) as conn:
                return _open(conn.client, conn.path)
        except (AttributeError, OSError, ftplib.error_temp) as e:
            sleep_time = 2 * i ** 2
            logging.getLogger(__name__).warning(f'Retry #{i}: Sleeping {sleep_time}s because {e}')
            sleep(sleep_time)


def _get_all_files(c, path):
    try:
        return [basename(file) for file in retry_pasv(c, 'nlst', path)]
    except AttributeError:
        return c.listdir(path)


def ftp_listdir(url: str) -> list:
    with connection(url) as conn:
        return _get_all_files(conn.client, conn.path)


def _get_mtime(client, path):
    """Returns timestamp of last modification"""
    try:
        mdtm = client.sendcmd('MDTM ' + path)
        # mdtm-response = "213" SP time-val CRLF
        mdtm = re.sub(r'^213 ', '', mdtm)
        dt = datetime.strptime(mdtm, '%Y%m%d%H%M%S')
        return (dt - datetime(1970, 1, 1)).total_seconds()
    except AttributeError:
        return client.stat(path).st_mtime
    except ftplib.error_perm as e:
        raise type(e)(f'Cannot open file "{path}". Please make sure the file exists: {e}')


def ftp_mtime(url: str) -> str:
    with connection(url) as conn:
        return _get_mtime(conn.client, conn.path)


def dir_mtimes(url: str) -> dict:
    with connection(url) as conn_ftp:
        dir_mtimes_mapping = {}
        all_files = _get_all_files(conn_ftp.client, conn_ftp.path)
        for file in all_files:
            try:
                dir_mtimes_mapping[file] = _get_mtime(conn_ftp.client, file)
            except ftplib.error_perm:
                dir_mtimes_mapping[file] = None
    return dir_mtimes_mapping
