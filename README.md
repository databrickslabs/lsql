Databricks Labs LSQL
===

[![PyPI - Version](https://img.shields.io/pypi/v/databricks-labs-lsql.svg)](https://pypi.org/project/databricks-labs-lsql)
[![build](https://github.com/databrickslabs/ucx/actions/workflows/push.yml/badge.svg)](https://github.com/databrickslabs/lsql/actions/workflows/push.yml) [![codecov](https://codecov.io/github/databrickslabs/lsql/graph/badge.svg?token=p0WKAfW5HQ)](https://codecov.io/github/databrickslabs/ucx)  [![lines of code](https://tokei.rs/b1/github/databrickslabs/lsql)]([https://codecov.io/github/databrickslabs/lsql](https://github.com/databrickslabs/lsql))

[Lightweight](https://github.com/databrickslabs/lsql/blob/main/docs/comparison.md) execution of SQL queries through Databricks SDK for Python.

<!-- TOC -->
* [Databricks Labs LSQL](#databricks-labs-lsql)
* [Installation](#installation)
* [Executing SQL](#executing-sql)
  * [Iterating over results](#iterating-over-results)
  * [Executing without iterating](#executing-without-iterating)
  * [Fetching one record](#fetching-one-record)
  * [Fetching one value](#fetching-one-value)
  * [Parameters](#parameters)
* [SQL backend abstraction](#sql-backend-abstraction)
* [Project Support](#project-support)
<!-- TOC -->

# Installation

```console
pip install databricks-labs-lsql
```

[[back to top](#databricks-labs-lsql)]

# Executing SQL

Primary use-case of :py:meth:`fetch_all` and :py:meth:`execute` methods is oriented at executing SQL queries in
a stateless manner straight away from Databricks SDK for Python, without requiring any external dependencies.
Results are fetched in JSON format through presigned external links. This is perfect for serverless applications
like AWS Lambda, Azure Functions, or any other containerised short-lived applications, where container startup
time is faster with the smaller dependency set.

Applications, that need a more traditional SQL Python APIs with cursors, efficient data transfer of hundreds of
megabytes or gigabytes of data serialized in Apache Arrow format, and low result fetching latency, should use
the stateful Databricks SQL Connector for Python.

Constructor and the most of the methods do accept [common parameters](#parameters).

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.lsql.core import StatementExecutionExt
w = WorkspaceClient()
see = StatementExecutionExt(w)
for (pickup_zip, dropoff_zip) in see('SELECT pickup_zip, dropoff_zip FROM samples.nyctaxi.trips LIMIT 10'):
    print(f'pickup_zip={pickup_zip}, dropoff_zip={dropoff_zip}')
```

[[back to top](#databricks-labs-lsql)]

## Iterating over results

Method `fetch_all` returns an iterator of objects, that resemble `pyspark.sql.Row` APIs, but full
compatibility is not the goal of this implementation. Method accepts [common parameters](#parameters).

```python
import os
from databricks.sdk import WorkspaceClient
from databricks.labs.lsql.core import StatementExecutionExt

results = []
w = WorkspaceClient()
see = StatementExecutionExt(w, warehouse_id=os.environ.get("TEST_DEFAULT_WAREHOUSE_ID"))
for pickup_zip, dropoff_zip in see.fetch_all("SELECT pickup_zip, dropoff_zip FROM samples.nyctaxi.trips LIMIT 10"):
    results.append((pickup_zip, dropoff_zip))
```

[[back to top](#databricks-labs-lsql)]

## Executing without iterating

When you only need to execute the query and have no need to iterate over results, use the `execute` method, 
which accepts [common parameters](#parameters).

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.lsql.core import StatementExecutionExt

w = WorkspaceClient()
see = StatementExecutionExt(w)
see.execute("CREATE TABLE foo AS SELECT * FROM range(10)")
```

[[back to top](#databricks-labs-lsql)]

## Fetching one record

Method `fetch_one` returns a single record from the result set. If the result set is empty, it returns `None`. 
If the result set contains more than one record, it raises `ValueError`.

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.lsql.core import StatementExecutionExt

w = WorkspaceClient()
see = StatementExecutionExt(w)
pickup_zip, dropoff_zip = see.fetch_one("SELECT pickup_zip, dropoff_zip FROM samples.nyctaxi.trips LIMIT 1")
print(f'pickup_zip={pickup_zip}, dropoff_zip={dropoff_zip}')
```

[[back to top](#databricks-labs-lsql)]

## Fetching one value

Method `fetch_value` returns a single value from the result set. If the result set is empty, it returns `None`.

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.lsql.core import StatementExecutionExt

w = WorkspaceClient()
see = StatementExecutionExt(w)
count = see.fetch_value("SELECT COUNT(*) FROM samples.nyctaxi.trips")
print(f'count={count}')
```

[[back to top](#databricks-labs-lsql)]

## Parameters

* `warehouse_id` (str, optional) - Warehouse upon which to execute a statement. If not given, it will use the warehouse specified in the constructor or the first available warehouse that is not in the `DELETED` or `DELETING` state.
* `byte_limit` (int, optional) - Applies the given byte limit to the statement's result size. Byte counts are based on internal representations and may not match measurable sizes in the JSON format.
* `catalog` (str, optional) - Sets default catalog for statement execution, similar to `USE CATALOG` in SQL. If not given, it will use the default catalog or the catalog specified in the constructor.
* `schema` (str, optional) - Sets default schema for statement execution, similar to `USE SCHEMA` in SQL. If not given, it will use the default schema or the schema specified in the constructor.
* `timeout` (timedelta, optional) - Timeout after which the query is cancelled. If timeout is less than 50 seconds, it is handled on the server side. If the timeout is greater than 50 seconds, Databricks SDK for Python cancels the statement execution and throws `TimeoutError`. If not given, it will use the timeout specified in the constructor.

[[back to top](#databricks-labs-lsql)]

# SQL backend abstraction

This framework allows for mapping with strongly-typed dataclasses between SQL and Python runtime. It handles the schema 
creation logic purely from Python datastructure.

`SqlBackend` is used to define the methods that are required to be implemented by any SQL backend
that is used by the library. The methods defined in this class are used to execute SQL statements, 
fetch results from SQL statements, and save data to tables. Available backends are:

- `StatementExecutionBackend` used for reading/writing records purely through REST API
- `DatabricksConnectBackend` used for reading/writing records through Databricks Connect
- `RuntimeBackend` used for execution within Databricks Runtime
- `MockBackend` used for unit testing

Common methods are:
- `execute(str)` - Execute a SQL statement and wait till it finishes
- `fetch(str)` - Execute a SQL statement and iterate over all results
- `save_table(full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass)` - Save a sequence of dataclass instances to a table

[[back to top](#databricks-labs-lsql)]

# Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.
