# Changelog

### Changed

- Removed `CacheEnum.HDF`

## [0.14.0] -  2024-11-21

### Added

- Python 3.13 is now supported

### Changed

- Drop support for HDF cache, and use a Pickle cache instead. CacheEnum.HDF is now an alias to CacheEnum.pickle
  and will be dropped in v0.15.0

## [0.13.0] -  2024-07-17

### Changed

- Require geopandas ^1.0
- Drop support for pydantic V1.

## [0.12.1] - 2023-11-15

### Fixed

- Csv: fix get-metadatas from CSVs files with `skiprows` as list (0-indexed) in Datasource.
- FTP: retry connection on `SSHException` while opening a remote url.

## [0.12.0] - 2023-09-01

### Changed

- Added support for pydantic V2 (We still support pydantic >=1 for now).

## [0.11.1] - 2023-04-19

### Added

- S3 fetcher: Add context from the original exception in case a file cannot be read after several retries

## [0.11.0] - 2023-04-18

- Added support for python 3.11
- Replaced `fastparquet` dependency with `pyarrow`

## [0.10.1] - 2023-04-03

### Changed

- S3: Now it is possible to forward the aws `session_token` to s3fs reader.

## [0.10.0] - 2023-03-30

### Fixed

- Fixed: it is now possible to extract a file's mime type from an URI containing query params.
- Fixed: failed to fetch preview of multiple sftp files by sharing mtime cache between same ftp fetchers.

## [0.9.0] - 2022-09-27

### Changed

- build: python 3.10 support only.
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

[0.12.0]: https://github.com/ToucanToco/peakina/compare/v0.11.1...v0.12.0
[0.11.1]: https://github.com/ToucanToco/peakina/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/ToucanToco/peakina/compare/v0.10.1...v0.11.0
[0.10.1]: https://github.com/ToucanToco/peakina/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/ToucanToco/peakina/compare/v0.9.0...v0.10.0
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
