import ftplib
import logging
import re
import socket
import ssl
import tempfile
from contextlib import contextmanager
from datetime import datetime
from functools import partial
from os.path import basename
from time import sleep
from typing.io import BinaryIO
from urllib.parse import ParseResult, quote, unquote, urlparse

import paramiko

uses_ftp = ['ftp', 'sftp', 'ftps']


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


@contextmanager
def ftps_client(params) -> tuple:
    ftps = FTPS()
    try:
        ftps.connect(host=params.hostname, port=params.port, timeout=3)
        ftps.prot_p()
        ftps.login(user=params.username, passwd=params.password)
        yield ftps, params.path
    finally:
        ftps.quit()


@contextmanager
def ftp_client(params) -> tuple:
    port = params.port or 21
    ftp = ftplib.FTP()
    try:
        ftp.connect(host=params.hostname, port=port, timeout=3)
        ftp.login(user=params.username, passwd=params.password)
        yield ftp, params.path
    finally:
        ftp.quit()


@contextmanager
def sftp_client(params):
    port = params.port or 22
    ssh_client = paramiko.SSHClient()
    try:
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=params.hostname,
            username=params.username,
            password=params.password,
            port=port,
            timeout=3,
        )
        sftp = ssh_client.open_sftp()
        yield sftp, params.path
    finally:
        ssh_client.close()


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


def client(url):
    parse_result = _urlparse(url)
    ftp_client_mapping = {'ftp': ftp_client, 'ftps': ftps_client, 'sftp': sftp_client}
    return ftp_client_mapping[parse_result.scheme](parse_result)


def retry_pasv(c, cmd, *args):
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


def _open(url: str) -> BinaryIO:
    ret = tempfile.NamedTemporaryFile(suffix='.ftptmp')
    with client(url) as (c, path):
        try:
            retry_pasv(c, 'retrbinary', f'RETR {path}', ret.write)
        except AttributeError:
            c.getfo(path, ret)
        except ftplib.error_perm as e:
            raise Exception(f'Cannot open file "{path}". Please make sure the file exists') from e

    ret.seek(0)
    return ret


def ftp_open(url: str, retry: int = 4) -> BinaryIO:
    for i in range(1, retry + 1):
        try:
            return _open(url)
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
    with client(url) as (c, path):
        return _get_all_files(c, path)


def _get_mtime(c, path):
    """Returns timestamp of last modification"""
    try:
        mdtm = c.sendcmd('MDTM ' + path)
        # mdtm-response = "213" SP time-val CRLF
        mdtm = re.sub(r'^213 ', '', mdtm)
        dt = datetime.strptime(mdtm, '%Y%m%d%H%M%S')
        return (dt - datetime(1970, 1, 1)).total_seconds()
    except AttributeError:
        return c.stat(path).st_mtime
    except ftplib.error_perm as e:
        raise type(e)(f'Cannot open file "{path}". Please make sure the file exists: {e}')


def ftp_mtime(url: str) -> str:
    with client(url) as (cl_ftp, path):
        return _get_mtime(cl_ftp, path)


def dir_mtimes(url: str) -> dict:
    with client(url) as (cl_ftp, path):
        dir_mtimes_mapping = {}
        all_files = _get_all_files(cl_ftp, path)
        for file in all_files:
            try:
                dir_mtimes_mapping[file] = _get_mtime(cl_ftp, file)
            except ftplib.error_perm:
                dir_mtimes_mapping[file] = None
    return dir_mtimes_mapping
