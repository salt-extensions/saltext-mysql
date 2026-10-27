The changelog format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

This project uses [Semantic Versioning](https://semver.org/) - MAJOR.MINOR.PATCH

# Changelog

## 1.1.1 (2026-10-27)


### Fixed

- Fixed `mysql.grant_revoke` ignoring the `escape` parameter, which made it impossible to revoke grants on databases containing wildcard characters (`_`, `%`) when they were added with `escape: False` [#18](https://github.com/salt-extensions/saltext-mysql/issues/18)
- Fixed `mysql_grants.absent` not passing the `escape` parameter to `mysql.grant_revoke` [#21](https://github.com/salt-extensions/saltext-mysql/issues/21)
- Improved `__virtual__` error messages to explicitly name the missing Python MySQL client libraries (mysqlclient/PyMySQL) and how to install them [#25](https://github.com/salt-extensions/saltext-mysql/issues/25)
- Fixed `mysql.query` failing with `TypeError: str() argument 'encoding' must be str, not dict` when using PyMySQL [#26](https://github.com/salt-extensions/saltext-mysql/issues/26)

## 1.1.0 (2025-02-07)


### Fixed

- Fixed `mysql_cache.list` not listing nested banks, thereby fixed presence detection when using the `mysql_cache` module [#9](https://github.com/salt-extensions/saltext-mysql/issues/9)


### Added

- Added support for specifying TLS connection parameters to the execution module [#4](https://github.com/salt-extensions/saltext-mysql/issues/4)


## v1.0.0 (2024-08-08)

Initial release of `saltext-mysql`. This release tracks the functionality in the core Salt code base as of version 3007.1.
