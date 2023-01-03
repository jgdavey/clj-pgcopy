# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Support for `inet` type, mapped from
  `java.net.Inet4Address`/`java.net.Inet6Address`.
- New API function `copy-into-table!` to provide table and columns as
  separate arguments that get converted to the necessary table spec
  SQL string.
- New API function `copy-values-into!` for providing the table spec
  SQL string directly, which new optional opts argument.
- Both of these functions take optional opts arguments at the end,
  which currently only contains `:buffer-size` for overriding the
  default output stream buffer size.

### Changed

- BREAKING: Refactored namespaces such that `clj-pgcopy.core` is now
  the main API, `clj-pgcopy.impl` is for private implementation
  details, and `clj-pgcopy.protocols` contains the `IPGBinaryWrite`
  protocol, as well as any other future public protocols.
- BREAKING: The `IPGBinaryWrite` protocol no longer has the unused
  `pg-type` method.
  
### Deprecated

- The `copy-into!` function has been deprecated in favor of the
  `copy-into-table!` and `copy-values-into!` functions. It will be
  removed in a future release.
  
### Fixed

- A type hint was unnecessarily specified as `PGCopyOutputStream`,
  when `OutputStream` sufficed.

## [0.1.2] - 2022-12-18

### Fixed

- Broken 0.1.1 release only worked on very recent versions of Clojure
  due to the use of `abs`. This release returns to using `Math/abs` so
  previous versions of Clojure are still compatible.

## [0.1.1] - 2022-12-18

WARNING: this release was broken and is superceded by 0.1.2.
Do not use this version.

### Changed

- Better buffering behavior for possibly better I/O performance.

### Added

- Handle really large BigDecimal -> numeric conversion

## [0.1.0] - 2019-07-23

### Added

- First public release, with support for some basic types

[unreleased]: https://github.com/jgdavey/clj-pgcopy/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/jgdavey/clj-pgcopy/releases/tag/v0.1.2
[0.1.1]: https://github.com/jgdavey/clj-pgcopy/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/jgdavey/clj-pgcopy/releases/tag/v0.1.0
