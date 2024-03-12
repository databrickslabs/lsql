# Version changelog

## 0.2.0

* Added `MockBackend.rows("col1", "col2")[(...), (...)]` helper ([#49](https://github.com/databrickslabs/lsql/issues/49)). In this release, we have added a new helper method `MockBackend.rows("col1", "col2")[(...), (...)]` to simplify testing with `MockBackend`. This method allows for the creation of rows using a more concise syntax, taking in the column names and a list of values to be used for each column, and returning a list of `Row` objects with the specified columns and values. Additionally, a `__eq__` method has been introduced to check if two rows are equal by converting the rows to dictionaries using the existing `as_dict` method and comparing them. The `__contains__` method has also been modified to improve the behavior of the `in` keyword when used with rows, ensuring columns can be checked for membership in the row in a more intuitive and predictable manner. These changes make it easier to test and work with `MockBackend`, improving overall quality and maintainability of the project.


## 0.1.1

* Updated project metadata ([#46](https://github.com/databrickslabs/lsql/issues/46)). In this release, the project metadata has been updated to reflect changes in the library's capabilities and dependencies. The project now supports lightweight SQL statement execution using the Databricks SDK for Python, setting it apart from other solutions. The library size comparison in the documentation has been updated, reflecting an increase in the compressed and uncompressed size of Databricks Labs LightSQL, as well as the addition of a new direct dependency, SQLglot. The project's dependencies and URLs in the `pyproject.toml` file have also been updated, including a version update for `databricks-labs-blueprint` and the removal of a specific range for `PyYAML`.

Dependency updates:

 * Updated sqlglot requirement from ~=22.2.1 to ~=22.3.1 ([#43](https://github.com/databrickslabs/lsql/pull/43)).

## 0.1.0

* Ported `StatementExecutionExt` from UCX ([#31](https://github.com/databrickslabs/lsql/issues/31)).

## 0.0.0

Initial commit
