import datetime
import os
import re
import sys
from dataclasses import dataclass
from unittest import mock
from unittest.mock import MagicMock, call, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    DataLoss,
    NotFound,
    PermissionDenied,
    Unknown,
)
from databricks.sdk.service.sql import (
    ColumnInfo,
    ColumnInfoTypeName,
    Format,
    ResultData,
    ResultManifest,
    ResultSchema,
    StatementResponse,
    StatementState,
    StatementStatus,
)

from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import (
    MockBackend,
    RuntimeBackend,
    StatementExecutionBackend,
)
from databricks.labs.lsql.core import DeltaConcurrentAppend

# pylint: disable=protected-access


@dataclass
class Foo:
    first: str
    second: bool


@dataclass
class Baz:
    first: str
    second: str | None = None


@dataclass
class Bar:
    first: str
    second: bool
    third: float


def test_statement_execution_backend_execute_happy():
    ws = create_autospec(WorkspaceClient)
    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )

    seb = StatementExecutionBackend(ws, "abc")

    seb.execute("CREATE TABLE foo")

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="abc",
        statement="CREATE TABLE foo",
        catalog=None,
        schema=None,
        disposition=None,
        format=Format.JSON_ARRAY,
        byte_limit=None,
        wait_timeout=None,
    )


def test_statement_execution_backend_with_overrides():
    ws = create_autospec(WorkspaceClient)
    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )

    seb = StatementExecutionBackend(ws, "abc")

    seb.execute("CREATE TABLE foo", catalog="foo", schema="bar")

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="abc",
        statement="CREATE TABLE foo",
        catalog="foo",
        schema="bar",
        disposition=None,
        format=Format.JSON_ARRAY,
        byte_limit=None,
        wait_timeout=None,
    )


def test_statement_execution_backend_fetch_happy():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT)])),
        result=ResultData(data_array=[["1"], ["2"], ["3"]]),
        statement_id="bcd",
    )

    seb = StatementExecutionBackend(ws, "abc")

    result = list(seb.fetch("SELECT id FROM range(3)"))

    assert [Row(id=1), Row(id=2), Row(id=3)] == result


def test_statement_execution_backend_save_table_overwrite_empty_table():
    ws = create_autospec(WorkspaceClient)
    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )
    seb = StatementExecutionBackend(ws, "abc")
    seb.save_table("a.b.c", [Baz("1")], Baz, mode="overwrite")
    ws.statement_execution.execute_statement.assert_has_calls(
        [
            mock.call(
                warehouse_id="abc",
                statement="CREATE TABLE IF NOT EXISTS a.b.c (first STRING NOT NULL, second STRING) USING DELTA",
                catalog=None,
                schema=None,
                disposition=None,
                format=Format.JSON_ARRAY,
                byte_limit=None,
                wait_timeout=None,
            ),
            mock.call(
                warehouse_id="abc",
                statement="TRUNCATE TABLE a.b.c",
                catalog=None,
                schema=None,
                disposition=None,
                format=Format.JSON_ARRAY,
                byte_limit=None,
                wait_timeout=None,
            ),
            mock.call(
                warehouse_id="abc",
                statement="INSERT INTO a.b.c (first, second) VALUES ('1', NULL)",
                catalog=None,
                schema=None,
                disposition=None,
                format=Format.JSON_ARRAY,
                byte_limit=None,
                wait_timeout=None,
            ),
        ]
    )


def test_statement_execution_backend_save_table_empty_records():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )

    seb = StatementExecutionBackend(ws, "abc")

    seb.save_table("a.b.c", [], Bar)

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="abc",
        statement="CREATE TABLE IF NOT EXISTS a.b.c "
        "(first STRING NOT NULL, second BOOLEAN NOT NULL, third FLOAT NOT NULL) USING DELTA",
        catalog=None,
        schema=None,
        disposition=None,
        format=Format.JSON_ARRAY,
        byte_limit=None,
        wait_timeout=None,
    )


def test_statement_execution_backend_save_table_two_records():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )

    seb = StatementExecutionBackend(ws, "abc")

    seb.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo)

    ws.statement_execution.execute_statement.assert_has_calls(
        [
            mock.call(
                warehouse_id="abc",
                statement="CREATE TABLE IF NOT EXISTS a.b.c (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA",
                catalog=None,
                schema=None,
                disposition=None,
                format=Format.JSON_ARRAY,
                byte_limit=None,
                wait_timeout=None,
            ),
            mock.call(
                warehouse_id="abc",
                statement="INSERT INTO a.b.c (first, second) VALUES ('aaa', TRUE), ('bbb', FALSE)",
                catalog=None,
                schema=None,
                disposition=None,
                format=Format.JSON_ARRAY,
                byte_limit=None,
                wait_timeout=None,
            ),
        ]
    )


def test_statement_execution_backend_save_table_in_batches_of_two():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )

    seb = StatementExecutionBackend(ws, "abc", max_records_per_batch=2)

    seb.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False), Foo("ccc", True)], Foo)

    ws.statement_execution.execute_statement.assert_has_calls(
        [
            mock.call(
                warehouse_id="abc",
                statement="CREATE TABLE IF NOT EXISTS a.b.c (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA",
                catalog=None,
                schema=None,
                disposition=None,
                format=Format.JSON_ARRAY,
                byte_limit=None,
                wait_timeout=None,
            ),
            mock.call(
                warehouse_id="abc",
                statement="INSERT INTO a.b.c (first, second) VALUES ('aaa', TRUE), ('bbb', FALSE)",
                catalog=None,
                schema=None,
                disposition=None,
                format=Format.JSON_ARRAY,
                byte_limit=None,
                wait_timeout=None,
            ),
            mock.call(
                warehouse_id="abc",
                statement="INSERT INTO a.b.c (first, second) VALUES ('ccc', TRUE)",
                catalog=None,
                schema=None,
                disposition=None,
                format=Format.JSON_ARRAY,
                byte_limit=None,
                wait_timeout=None,
            ),
        ]
    )


def test_runtime_backend_execute():
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = MagicMock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        spark = pyspark_sql_session.SparkSession.builder.getOrCreate()

        runtime_backend = RuntimeBackend()

        runtime_backend.execute("CREATE TABLE foo")

        spark.sql.assert_called_with("CREATE TABLE foo")


def test_runtime_backend_fetch():
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = MagicMock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        spark = pyspark_sql_session.SparkSession.builder.getOrCreate()

        spark.sql().collect.return_value = [Row(id=1), Row(id=2), Row(id=3)]

        runtime_backend = RuntimeBackend()

        result = runtime_backend.fetch("SELECT id FROM range(3)", catalog="foo", schema="bar")

        assert [Row(id=1), Row(id=2), Row(id=3)] == list(result)

        calls = [call("USE CATALOG foo"), call("USE SCHEMA bar"), call("SELECT id FROM range(3)")]
        spark.sql.assert_has_calls(calls)


def test_runtime_backend_save_table():
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = MagicMock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        spark = pyspark_sql_session.SparkSession.builder.getOrCreate()

        runtime_backend = RuntimeBackend()

        runtime_backend.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo)

        spark.createDataFrame.assert_called_with(
            [Foo(first="aaa", second=True), Foo(first="bbb", second=False)],
            "first STRING NOT NULL, second BOOLEAN NOT NULL",
        )
        spark.createDataFrame().write.saveAsTable.assert_called_with("a.b.c", mode="append")


def test_runtime_backend_save_table_with_row_containing_none_with_nullable_class(mocker):
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = MagicMock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        spark = pyspark_sql_session.SparkSession.builder.getOrCreate()

        runtime_backend = RuntimeBackend()

        runtime_backend.save_table("a.b.c", [Baz("aaa", "ccc"), Baz("bbb", None)], Baz)

        spark.createDataFrame.assert_called_with(
            [Baz(first="aaa", second="ccc"), Baz(first="bbb", second=None)],
            "first STRING NOT NULL, second STRING",
        )
        spark.createDataFrame().write.saveAsTable.assert_called_with("a.b.c", mode="append")


@dataclass
class DummyClass:
    key: str
    value: str | None = None


def test_save_table_with_not_null_constraint_violated():
    rows = [DummyClass("1", "test"), DummyClass("2", None), DummyClass(None, "value")]

    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = MagicMock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session

        runtime_backend = RuntimeBackend()

        with pytest.raises(
            Exception, match="Not null constraint violated for column key, row = {'key': None, 'value': 'value'}"
        ):
            runtime_backend.save_table("a.b.c", rows, DummyClass)


@pytest.mark.parametrize(
    "msg,err_t",
    [
        ("SCHEMA_NOT_FOUND foo schema does not exist", NotFound),
        (".. TABLE_OR_VIEW_NOT_FOUND ..", NotFound),
        (".. UNRESOLVED_COLUMN.WITH_SUGGESTION ..", BadRequest),
        ("DELTA_TABLE_NOT_FOUND foo table does not exist", NotFound),
        ("DELTA_MISSING_TRANSACTION_LOG foo table does not exist", DataLoss),
        ("PARSE_SYNTAX_ERROR foo", BadRequest),
        ("foo Operation not allowed", PermissionDenied),
        ("foo error failure", Unknown),
        ("[DELTA_CONCURRENT_APPEND] ConcurrentAppendException: Files were added ...", DeltaConcurrentAppend),
    ],
)
def test_runtime_backend_error_mapping_similar_to_statement_execution(msg, err_t) -> None:
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = MagicMock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        spark = pyspark_sql_session.SparkSession.builder.getOrCreate()

        spark.sql.side_effect = Exception(msg)

        runtime_backend = RuntimeBackend()

        with pytest.raises(err_t, match=re.escape(msg)):
            runtime_backend.execute("SELECT * from bar")

        with pytest.raises(err_t, match=re.escape(msg)):
            list(runtime_backend.fetch("SELECT * from bar"))


def test_mock_backend_fails_on_first():
    mock_backend = MockBackend(fails_on_first={"CREATE": ".. DELTA_TABLE_NOT_FOUND .."})

    with pytest.raises(NotFound):
        mock_backend.execute("CREATE TABLE foo")


def test_mock_backend_rows():
    mock_backend = MockBackend(rows={r"SELECT id FROM range\(3\)": [Row(id=1), Row(id=2), Row(id=3)]})

    result = list(mock_backend.fetch("SELECT id FROM range(3)"))

    assert [Row(id=1), Row(id=2), Row(id=3)] == result


def test_mock_backend_save_table():
    mock_backend = MockBackend()

    mock_backend.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo)
    mock_backend.save_table("a.b.c", [Foo("ccc", True)], Foo)

    assert mock_backend.rows_written_for("a.b.c", "append") == [
        Row(first="aaa", second=True),
        Row(first="bbb", second=False),
        Row(first="ccc", second=True),
    ]


def test_mock_backend_save_table_overwrite() -> None:
    mock_backend = MockBackend()

    mock_backend.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo, mode="overwrite")
    mock_backend.save_table("d.e.f", [Foo("ddd", True), Foo("eee", False)], Foo, mode="overwrite")
    mock_backend.save_table("d.e.f", [Foo("fff", True)], Foo, mode="overwrite")

    assert mock_backend.rows_written_for("a.b.c", "overwrite") == [
        Row(first="aaa", second=True),
        Row(first="bbb", second=False),
    ]
    assert mock_backend.rows_written_for("d.e.f", "overwrite") == [
        Row(first="fff", second=True),
    ]


def test_mock_backend_rows_dsl():
    rows = MockBackend.rows("foo", "bar")[
        [1, 2],
        (3, 4),
    ]
    assert rows == [
        Row(foo=1, bar=2),
        Row(foo=3, bar=4),
    ]


def test_mock_backend_overwrite():
    mock_backend = MockBackend()
    mock_backend.save_table("a.b.c", [Foo("a1", True), Foo("c2", False)], Foo, "append")
    mock_backend.save_table("a.b.c", [Foo("aa", True), Foo("bb", False)], Foo, "overwrite")
    mock_backend.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo, "overwrite")

    assert mock_backend.rows_written_for("a.b.c", "append") == []
    assert mock_backend.rows_written_for("a.b.c", "overwrite") == [
        Row(first="aaa", second=True),
        Row(first="bbb", second=False),
    ]


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_mock_backend_has_no_rows_written(mode) -> None:
    mock_backend = MockBackend()
    # There are no rows written
    assert not mock_backend.has_rows_written_for("a.b.c")
    # and the results contains no rows
    assert not mock_backend.rows_written_for("a.b.c", mode)


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_mock_backend_has_zero_rows_written(mode) -> None:
    mock_backend = MockBackend()
    mock_backend.save_table("a.b.c", [], Foo, mode)
    # There are rows written
    assert mock_backend.has_rows_written_for("a.b.c")
    # while the results contains no rows
    assert not mock_backend.rows_written_for("a.b.c", mode)


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_mock_backend_has_rows_written_for_after_first_write(mode) -> None:
    mock_backend = MockBackend()
    mock_backend.save_table("a.b.c", [Foo("a1", True), Foo("c2", False)], Foo, mode)
    assert mock_backend.has_rows_written_for("a.b.c")
    assert not mock_backend.has_rows_written_for("a.b.d")


@pytest.mark.parametrize("first_mode", ["append", "overwrite"])
@pytest.mark.parametrize("second_mode", ["append", "overwrite"])
def test_mock_backend_has_rows_written_for_after_two_writes(first_mode, second_mode) -> None:
    mock_backend = MockBackend()
    mock_backend.save_table("a.b.c", [Foo("a1", True), Foo("c2", False)], Foo, first_mode)
    mock_backend.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo, second_mode)
    assert mock_backend.has_rows_written_for("a.b.c")
    assert not mock_backend.has_rows_written_for("a.b.d")


@dataclass
class Nested:
    foo: Foo
    since: datetime.date
    created: datetime.datetime
    mapping: dict[str, int]
    array: list[int]
    some: float | None = None


def test_supports_complex_types():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = StatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED)
    )

    seb = StatementExecutionBackend(ws, "abc", max_records_per_batch=2)

    today = datetime.date(2024, 9, 11)
    now = datetime.datetime(2024, 9, 11, 12, 13, 14, tzinfo=datetime.timezone.utc)
    seb.save_table(
        "x",
        [
            Nested(Foo("a", True), today, now, {"a": 1, "b": 2}, [1, 2, 3], 0.342532),
        ],
        Nested,
    )

    queries = [_.kwargs["statement"] for _ in ws.statement_execution.method_calls]
    assert [
        "CREATE TABLE IF NOT EXISTS x (foo STRUCT<first:STRING,second:BOOLEAN> NOT NULL, since DATE NOT NULL, created TIMESTAMP NOT NULL, mapping MAP<STRING,LONG> NOT NULL, array ARRAY<LONG> NOT NULL, some FLOAT) USING DELTA",
        "INSERT INTO x (foo, since, created, mapping, array, some) VALUES (STRUCT('a' AS first, TRUE AS second), DATE '2024-9-11', TIMESTAMP '2024-09-11 12:13:14+0000', MAP('a', 1, 'b', 2), ARRAY(1, 2, 3), 0.342532)",
    ] == queries
