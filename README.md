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
> import peakina as pk
> pk.read_pandas('file.csv')
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
> pk.read_pandas('ftps://<path>/my_data_\\d{4}\\.csv$', match='regex', dtype={'a': 'str'})
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
