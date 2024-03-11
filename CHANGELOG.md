# Version changelog

## 0.1.1

* Updated project metadata ([#46](https://github.com/databrickslabs/lsql/issues/46)). In this release, the project metadata has been updated to reflect changes in the library's capabilities and dependencies. The project now supports lightweight SQL statement execution using the Databricks SDK for Python, setting it apart from other solutions. The library size comparison in the documentation has been updated, reflecting an increase in the compressed and uncompressed size of Databricks Labs LightSQL, as well as the addition of a new direct dependency, SQLglot. The project's dependencies and URLs in the `pyproject.toml` file have also been updated, including a version update for `databricks-labs-blueprint` and the removal of a specific range for `PyYAML`.

Dependency updates:

 * Updated sqlglot requirement from ~=22.2.1 to ~=22.3.1 ([#43](https://github.com/databrickslabs/lsql/pull/43)).

## 0.1.0

* Ported `StatementExecutionExt` from UCX ([#31](https://github.com/databrickslabs/lsql/issues/31)).

## 0.0.0

Initial commit
