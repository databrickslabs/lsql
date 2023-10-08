# databricks-labs-lsql

[![PyPI - Version](https://img.shields.io/pypi/v/databricks-labs-lightsql.svg)](https://pypi.org/project/databricks-labs-lightsql)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/databricks-labs-lightsql.svg)](https://pypi.org/project/databricks-labs-lightsql)

-----

Execute SQL statements in a stateless manner.

## Installation

```console
pip install databricks-labs-lsql
```

## Executing SQL

Primary use-case of :py:meth:`iterate_rows` and :py:meth:`execute` methods is oriented at executing SQL queries in
a stateless manner straight away from Databricks SDK for Python, without requiring any external dependencies.
Results are fetched in JSON format through presigned external links. This is perfect for serverless applications
like AWS Lambda, Azure Functions, or any other containerised short-lived applications, where container startup
time is faster with the smaller dependency set.

    for (pickup_zip, dropoff_zip) in w.statement_execution.iterate_rows(warehouse_id,
        'SELECT pickup_zip, dropoff_zip FROM nyctaxi.trips LIMIT 10', catalog='samples'):
        print(f'pickup_zip={pickup_zip}, dropoff_zip={dropoff_zip}')

Method :py:meth:`iterate_rows` returns an iterator of objects, that resemble :class:`pyspark.sql.Row` APIs, but full
compatibility is not the goal of this implementation.

    iterate_rows = functools.partial(w.statement_execution.iterate_rows, warehouse_id, catalog='samples')
    for row in iterate_rows('SELECT * FROM nyctaxi.trips LIMIT 10'):
        pickup_time, dropoff_time = row[0], row[1]
        pickup_zip = row.pickup_zip
        dropoff_zip = row['dropoff_zip']
        all_fields = row.as_dict()
        print(f'{pickup_zip}@{pickup_time} -> {dropoff_zip}@{dropoff_time}: {all_fields}')

When you only need to execute the query and have no need to iterate over results, use the :py:meth:`execute`.

    w.statement_execution.execute(warehouse_id, 'CREATE TABLE foo AS SELECT * FROM range(10)')

## Working with dataclasses

This framework allows for mapping with strongly-typed dataclasses between SQL and Python runtime.

It handles the schema creation logic purely from Python datastructure.

## Mocking for unit tests

This includes a lightweight framework to map between dataclass instances and different SQL execution backends:
- `MockBackend` used for unit testing
- `RuntimeBackend` used for execution within Databricks Runtime
- `StatementExecutionBackend` used for reading/writing records purely through REST API

## Pick the library that you need

_Simple applications_, like AWS Lambdas or Azure Functions, and scripts, that are **constrained by the size of external 
dependencies** or **cannot depend on compiled libraries**, like `pyarrow` (88M), `pandas` (71M), `numpy` (60M), 
`libarrow` (41M), `cygrpc` (30M), `libopenblas64` (22M), **need less than 5M of dependencies** (see [detailed report](docs/comparison.md)), 
experience the [Unified Authentication](https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication),
and **work only with Databricks SQL Warehouses**, should use this library. 

Applications, that need the full power of Databricks Runtime locally with the full velocity of PySpark SDL, experience
the [Unified Authentication](https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication) 
across all Databricks tools, efficient data transfer serialized in Apache Arrow format, and low result fetching latency, 
should use the stateful [Databricks Connect 2.x](https://docs.databricks.com/en/dev-tools/databricks-connect/index.html).

Applications, that need to a more traditional SQL Python APIs with cursors, efficient data transfer of hundreds of
megabytes or gigabytes of data serialized in Apache Arrow format, and low result fetching latency, should use
the stateful [Databricks SQL Connector for Python](https://docs.databricks.com/en/dev-tools/python-sql-connector.html).

| ...                                     | Databricks Connect 2.x                 | Databricks SQL Connector                          | PyODBC + ODBC Driver                              | Databricks Labs LightSQL           |
|-----------------------------------------|----------------------------------------|---------------------------------------------------|---------------------------------------------------|------------------------------------|
 | Light-weight mocking                    | no                                     | no                                                | no                                                | **yes**                            |
 | Extended support for dataclasses        | limited                                | no                                                | no                                                | **yes**                            |
 | Strengths                               | almost Databricks Runtime, but locally | works with Python ecosystem                       | works with ODBC ecosystem                         | **tiny**                           |
 | Compressed size                         | 60M                                    | 51M (85%)                                         | 44M (73.3%)                                       | **0.8M (1.3%)**                    |
 | Uncompressed size                       | 312M                                   | 280M (89.7%)                                      | ?                                                 | **30M (9.6%)**                     |
 | Direct dependencies                     | 23                                     | 14                                                | 2                                                 | **1** (Python SDK)                 |
 | Unified Authentication                  | yes (via Python SDK)                   | no                                                | no                                                | **yes** (via Python SDK)           |
 | Works with                              | Databricks Clusters only               | Databricks Clusters and Databricks SQL Warehouses | Databricks Clusters and Databricks SQL Warehouses | **Databricks SQL Warehouses only** |
 | Full equivalent of Databricks Runtime   | yes                                    | no                                                | no                                                | **no**                             |
 | Efficient memory usage via Apache Arrow | yes                                    | yes                                               | yes                                               | **no**                             |
 | Connection handling                     | stateful                               | stateful                                          | stateful                                          | **stateless**                      |
 | Official                                | yes                                    | yes                                               | yes                                               | **no**                             |
 | Version checked                         | 14.0.1                                 | 2.9.3                                             | driver v2.7.5                                     | 0.1.0                              |

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.
