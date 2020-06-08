# Changelog

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

[0.5.3]: https://github.com/ToucanToco/peakina/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/ToucanToco/peakina/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/ToucanToco/peakina/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/ToucanToco/peakina/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ToucanToco/peakina/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ToucanToco/peakina/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ToucanToco/peakina/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ToucanToco/peakina/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/ToucanToco/peakina/tree/v0.0.1
