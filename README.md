[![Pypi-v](https://img.shields.io/pypi/v/peakina.svg)](https://pypi.python.org/pypi/peakina)
[![Pypi-pyversions](https://img.shields.io/pypi/pyversions/peakina.svg)](https://pypi.python.org/pypi/peakina)
[![Pypi-l](https://img.shields.io/pypi/l/peakina.svg)](https://pypi.python.org/pypi/peakina)
[![Pypi-wheel](https://img.shields.io/pypi/wheel/peakina.svg)](https://pypi.python.org/pypi/peakina)
[![CircleCI](https://img.shields.io/circleci/project/github/ToucanToco/peakina.svg)](https://circleci.com/gh/ToucanToco/peakina)
[![codecov](https://codecov.io/gh/ToucanToco/peakina/branch/master/graph/badge.svg)](https://codecov.io/gh/ToucanToco/peakina)

# Pea Kina _aka 'Giant Panda'_

Wrapper around `pandas` library and much more...

# Installation

`pip install peakina`

# Usage

```python
import peakina as pk


df = pk.read_pandas('my/local/file.csv')
df = pk.read_pandas('ftps://user:pw@server.com/remote/file.csv')
df = pk.read_pandas('s3://key:secret@bucket/file.csv')
```
