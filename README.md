[![Pypi-v](https://img.shields.io/pypi/v/peakina.svg)](https://pypi.python.org/pypi/peakina)
[![Pypi-pyversions](https://img.shields.io/pypi/pyversions/peakina.svg)](https://pypi.python.org/pypi/peakina)
[![Pypi-l](https://img.shields.io/pypi/l/peakina.svg)](https://pypi.python.org/pypi/peakina)
[![Pypi-wheel](https://img.shields.io/pypi/wheel/peakina.svg)](https://pypi.python.org/pypi/peakina)
[![CircleCI](https://img.shields.io/circleci/project/github/ToucanToco/peakina.svg)](https://circleci.com/gh/ToucanToco/peakina)
[![codecov](https://codecov.io/gh/ToucanToco/peakina/branch/master/graph/badge.svg)](https://codecov.io/gh/ToucanToco/peakina)

# Pea Kina _aka 'Giant Panda'_

Wrapper around `pandas` library, which detects separator, encoding
and type of the file. It allows to get a group of files with a matching pattern (python or glob regex).
It can read local but also FTP/FTPS/SFTP and S3/S3N/S3A files.

# Installation

`pip install peakina`

# Usage
Considering a file 'file.csv'
```
a;b
0;0
0;1
```

Just type
```python
>>> import peakina as pk
>>> pk.read_pandas('file.csv')
   a  b
0  0  0
1  0  1
```

Or files on a FTPS server:
- my_data_2015.csv
- my_data_2016.csv
- my_data_2017.csv
- my_data_2018.csv

You can just type

```python
>>> pk.read_pandas('ftps://<path>/my_data_\\d{4}\\.csv$', match='regex', dtype={'a': 'str'})
    a   b     __filename__
0  '0'  0  'my_data_2015.csv'
1  '0'  1  'my_data_2015.csv'
2  '1'  0  'my_data_2016.csv'
3  '1'  1  'my_data_2016.csv'
4  '3'  0  'my_data_2017.csv'
5  '3'  1  'my_data_2017.csv'
6  '4'  0  'my_data_2018.csv'
7  '4'  1  'my_data_2018.csv'
```

## Using cache

You may want to keep the last result in cache, to avoid downloading and extracting the file if it didn't change:

```python
>>> from peakina.cache import Cache
>>> cache = Cache.get_cache('memory')  # in-memory cache
>>> df = pk.read_pandas('file.csv', expire=3600, cache=cache)
```

In this example, the resulting dataframe will be fetched from the cache, unless `file.csv` modification time has changed on disk, or unless the cache is older than 1 hour.

For persistent caching, use: `cache = Cache.get_cache('hdf', cache_dir='/tmp')`


## Use only downloading feature

If you just want to download a file, without converting it to a pandas dataframe:

```python
>>> uri = 'https://i.imgur.com/V9x88.jpg'
>>> f = pk.fetch(uri)
>>> f.get_str_mtime()
'2012-11-04T17:27:14Z'
>>> with f.open() as stream:
...     print('Image size:', len(stream.read()), 'bytes')
...
Image size: 60284 bytes
```
