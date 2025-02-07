The changelog format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

This project uses [Semantic Versioning](https://semver.org/) - MAJOR.MINOR.PATCH

# Changelog

## 1.1.0 (2025-02-07)


### Fixed

- Fixed `mysql_cache.list` not listing nested banks, thereby fixed presence detection when using the `mysql_cache` module [#9](https://github.com/salt-extensions/saltext-mysql/issues/9)


### Added

- Added support for specifying TLS connection parameters to the execution module [#4](https://github.com/salt-extensions/saltext-mysql/issues/4)


## v1.0.0 (2024-08-08)

Initial release of `saltext-mysql`. This release tracks the functionality in the core Salt code base as of version 3007.1.
