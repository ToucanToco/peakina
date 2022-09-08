# Changelog

## [0.9.0] - 2022-09-27

### Changed

- build: python 3.10 support only.
- dependencies updates:
   -  bump types-chardet from 5.0.1 to 5.0.4
   -  bump s3fs from 2022.5.0 to 2022.7.1
   -  bump geopandas from 0.11.0 to 0.11.1
   -  bump pre-commit from 2.19.0 to 2.20.0
   -  bump pytest-sugar from 0.9.4 to 0.9.5
   -  bump types-pyyaml from 6.0.9 to 6.0.11
   -  bump urllib3 from 1.26.9 to 1.26.11
   -  bump pytest-mock from 3.8.1 to 3.8.2
   -  bump types-chardet from 4.0.4 to 5.0.1
   -  bump pytest-mock from 3.7.0 to 3.8.1
   -  bump mkdocs-material from 8.3.6 to 8.3.9
   -  bump types-python-slugify from 5.0.4 to 6.1.0
   -  bump black from 22.3.0 to 22.6.0
   -  bump aiobotocore from 2.3.3 to 2.3.4
   -  bump types-paramiko from 2.11.1 to 2.11.2
   -  bump types-dataclasses from 0.6.5 to 0.6.6
   -  bump chardet from 4.0.0 to 5.0.0
   -  bump types-pyyaml from 6.0.8 to 6.0.9
   -  bump types-paramiko from 2.11.0 to 2.11.1
   -  bump geopandas from 0.10.2 to 0.11.0
   -  bump mkdocs-material from 8.3.3 to 8.3.6
   -  bump types-paramiko from 2.10.0 to 2.11.0
   -  bump certifi from 2022.5.18.1 to 2022.6.15
   -  bump actions/setup-python from 3 to 4
   -  bump jq from 1.2.3 to 1.3.0
   -  bump docker from 5.0.3 to 6.0.0
- chore: Bump geopandas>=0.11.1

## [0.8.4] -   2022-06-06

### Changed

- Dependency whose version was `<1` have been constrained to `>=0.x,<1` rather than `^0.x`

## [0.8.2] -   2022-06-06
- geodata reader


## [0.7.11] -  2022-04-05

### Changed
- Revert Excel parsing to using pandas

## [0.5.3] - 2020-06-08

### Changed
- ftp and ftps resilience to quit() errors

## [0.5.2] - 2020-04-28

### Changed
- is_matching() should work when match is None

## [0.5.1] - 2020-03-30

### Changed
- fix linter on python 3.6
- append subdirectories when listing s3 dirpath
- listed objects in s3 buckets can have no \'LastModified\' attribute

## [0.5.0] - 2020-03-23

### Added

- Added `listdir` and `mtime` on S3 fetcher to support `match` and `cache`

### Changed

- Use `pyjq` lib instead of `jq` to work on python 3.8
- Dev :: linter changes and switch CI from CircleCI to Github

## [0.4.0] - 2019-11-19

### Added

- Added `verify` parameter to `peakina.io.http.http_fetcher.HTTPFetcher`'s constructor

### Changed

- Check SSL certificates by default when fetching https resources

- Use [fastparquet](https://fastparquet.readthedocs.io/en/latest/) instead of
  [pyarrow](https://pypi.org/project/pyarrow/#description)

## [0.3.0] - 2019-10-10

### Added

- Added parquet support
- Added 'encoding' and 'decimal' parameters for excel
- Added `fetcher_kwargs` and use it for s3 endpoint

### Changed

- Switch to `strict_optional = True` for `mypy`
- Rename `extra_kwargs` into `reader_kwargs`

## [0.2.0] - 2019-08-02

### Added

- Added 'keep_default_na' for excel
- Added `peakina.datasource.Datasource.get_metadata`
- Added JQ filter support for JSON datasources

## [0.1.0] - 2019-07-11

### Added

- Added excel sheetnames support
- Added HTTP fetcher
- Added cache (in memory or HDF file)
- Added validation for `extra_kwargs` passed to `DataSource`

### Changed

- Clearer API: `reader` becomes `fetcher`

## [0.0.1] - 2018-11-16

### Added

- Initial version, showtime!

[0.9.0]: https://github.com/ToucanToco/peakina/compare/v0.8.4...v0.9.0
[0.8.4]: https://github.com/ToucanToco/peakina/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/ToucanToco/peakina/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/ToucanToco/peakina/compare/v0.8.1...v0.8.2
[0.5.3]: https://github.com/ToucanToco/peakina/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/ToucanToco/peakina/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/ToucanToco/peakina/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/ToucanToco/peakina/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ToucanToco/peakina/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ToucanToco/peakina/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ToucanToco/peakina/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ToucanToco/peakina/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/ToucanToco/peakina/tree/v0.0.1
