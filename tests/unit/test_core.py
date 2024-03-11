import datetime
from unittest.mock import create_autospec

import pytest
import requests
from databricks.sdk import WorkspaceClient, errors
from databricks.sdk.service.sql import (
    ColumnInfo,
    ColumnInfoTypeName,
    EndpointInfo,
    ExecuteStatementResponse,
    ExternalLink,
    Format,
    GetStatementResponse,
    ResultData,
    ResultManifest,
    ResultSchema,
    ServiceError,
    ServiceErrorCode,
    State,
    StatementState,
    StatementStatus,
    timedelta,
)

from databricks.labs.lsql.core import Row, StatementExecutionExt


@pytest.mark.parametrize(
    "row",
    [
        Row(foo="bar", enabled=True),
        Row(["foo", "enabled"], ["bar", True]),
    ],
)
def test_row_from_kwargs(row):
    assert row.foo == "bar"
    assert row["foo"] == "bar"
    assert "foo" in row
    assert len(row) == 2
    assert list(row) == ["bar", True]
    assert row.as_dict() == {"foo": "bar", "enabled": True}
    foo, enabled = row
    assert foo == "bar"
    assert enabled is True
    assert str(row) == "Row(foo='bar', enabled=True)"
    with pytest.raises(AttributeError):
        print(row.x)


def test_row_factory():
    factory = Row.factory(["a", "b"])
    row = factory(1, 2)
    a, b = row
    assert a == 1
    assert b == 2


def test_row_factory_with_generator():
    factory = Row.factory(["a", "b"])
    row = factory(_ + 1 for _ in range(2))
    a, b = row
    assert a == 1
    assert b == 2


def test_selects_warehouse_from_config():
    ws = create_autospec(WorkspaceClient)
    ws.config.warehouse_id = "abc"
    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws)
    see.execute("SELECT 2+2")

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="abc",
        statement="SELECT 2+2",
        format=Format.JSON_ARRAY,
        disposition=None,
        byte_limit=None,
        catalog=None,
        schema=None,
        wait_timeout=None,
    )


def test_selects_warehouse_from_existing_first_running():
    ws = create_autospec(WorkspaceClient)

    ws.warehouses.list.return_value = [
        EndpointInfo(id="abc", state=State.DELETING),
        EndpointInfo(id="bcd", state=State.RUNNING),
        EndpointInfo(id="cde", state=State.RUNNING),
    ]

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
    )

    see = StatementExecutionExt(ws)
    see.execute("SELECT 2+2")

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="bcd",
        statement="SELECT 2+2",
        format=Format.JSON_ARRAY,
        disposition=None,
        byte_limit=None,
        catalog=None,
        schema=None,
        wait_timeout=None,
    )


def test_selects_warehouse_from_existing_not_running():
    ws = create_autospec(WorkspaceClient)

    ws.warehouses.list.return_value = [
        EndpointInfo(id="efg", state=State.DELETING),
        EndpointInfo(id="fgh", state=State.DELETED),
        EndpointInfo(id="bcd", state=State.STOPPED),
        EndpointInfo(id="cde", state=State.STARTING),
    ]

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
    )

    see = StatementExecutionExt(ws)
    see.execute("SELECT 2+2")

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="bcd",
        statement="SELECT 2+2",
        format=Format.JSON_ARRAY,
        disposition=None,
        byte_limit=None,
        catalog=None,
        schema=None,
        wait_timeout=None,
    )


def test_no_warehouse_given():
    ws = create_autospec(WorkspaceClient)

    see = StatementExecutionExt(ws)

    with pytest.raises(ValueError):
        see.execute("SELECT 2+2")


def test_execute_poll_succeeds():
    ws = create_autospec(WorkspaceClient)
    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.PENDING),
        statement_id="bcd",
    )
    ws.statement_execution.get_statement.return_value = GetStatementResponse(
        manifest=ResultManifest(),
        result=ResultData(byte_count=100500),
        statement_id="bcd",
        status=StatementStatus(state=StatementState.SUCCEEDED),
    )

    see = StatementExecutionExt(ws)

    response = see.execute("SELECT 2+2", warehouse_id="abc")

    assert response.status.state == StatementState.SUCCEEDED
    assert response.result.byte_count == 100500
    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="abc",
        statement="SELECT 2+2",
        format=Format.JSON_ARRAY,
        disposition=None,
        byte_limit=None,
        catalog=None,
        schema=None,
        wait_timeout=None,
    )
    ws.statement_execution.get_statement.assert_called_with("bcd")


@pytest.mark.parametrize(
    "status_error,platform_error_type",
    [
        (None, errors.Unknown),
        (ServiceError(), errors.Unknown),
        (ServiceError(message="..."), errors.Unknown),
        (ServiceError(error_code=ServiceErrorCode.RESOURCE_EXHAUSTED, message="..."), errors.ResourceExhausted),
        (ServiceError(message="... SCHEMA_NOT_FOUND ..."), errors.NotFound),
        (ServiceError(message="... TABLE_OR_VIEW_NOT_FOUND ..."), errors.NotFound),
        (ServiceError(message="... DELTA_TABLE_NOT_FOUND ..."), errors.NotFound),
        (ServiceError(message="... DELTA_TABLE_NOT_FOUND ..."), errors.NotFound),
        (ServiceError(message="... DELTA_MISSING_TRANSACTION_LOG ..."), errors.DataLoss),
    ],
)
def test_execute_fails(status_error, platform_error_type):
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.FAILED, error=status_error),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws, warehouse_id="abc")

    with pytest.raises(platform_error_type):
        see.execute("SELECT 2+2")


def test_execute_poll_waits():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.PENDING),
        statement_id="bcd",
    )

    ws.statement_execution.get_statement.side_effect = [
        GetStatementResponse(status=StatementStatus(state=StatementState.RUNNING), statement_id="bcd"),
        GetStatementResponse(
            manifest=ResultManifest(),
            result=ResultData(byte_count=100500),
            statement_id="bcd",
            status=StatementStatus(state=StatementState.SUCCEEDED),
        ),
    ]

    see = StatementExecutionExt(ws, warehouse_id="abc")

    response = see.execute("SELECT 2+2")

    assert response.status.state == StatementState.SUCCEEDED
    assert response.result.byte_count == 100500


def test_execute_poll_timeouts_on_client():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.PENDING),
        statement_id="bcd",
    )

    ws.statement_execution.get_statement.return_value = GetStatementResponse(
        status=StatementStatus(state=StatementState.RUNNING),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws, warehouse_id="abc")
    with pytest.raises(TimeoutError):
        see.execute("SELECT 2+2", timeout=timedelta(seconds=1))

    ws.statement_execution.cancel_execution.assert_called_with("bcd")


def test_fetch_all_no_chunks():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(
            schema=ResultSchema(
                columns=[
                    ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT),
                    ColumnInfo(name="since", type_name=ColumnInfoTypeName.DATE),
                    ColumnInfo(name="now", type_name=ColumnInfoTypeName.TIMESTAMP),
                ]
            )
        ),
        result=ResultData(external_links=[ExternalLink(external_link="https://singed-url")]),
        statement_id="bcd",
    )

    http_session = create_autospec(requests.Session)
    http_session.get("https://singed-url").json.return_value = [
        ["1", "2023-09-01", "2023-09-01T13:21:53Z"],
        ["2", "2023-09-01", "2023-09-01T13:21:53Z"],
    ]

    see = StatementExecutionExt(ws, warehouse_id="abc", http_session_factory=lambda: http_session)

    rows = list(see.fetch_all("SELECT id, CAST(NOW() AS DATE) AS since, NOW() AS now FROM range(2)"))

    assert len(rows) == 2
    assert rows[0].id == 1
    assert isinstance(rows[0].since, datetime.date)
    assert isinstance(rows[0].now, datetime.datetime)

    http_session.get.assert_called_with("https://singed-url")


def test_fetch_all_no_chunks_no_converter():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(
            schema=ResultSchema(
                columns=[
                    ColumnInfo(name="id", type_name=ColumnInfoTypeName.INTERVAL),
                ]
            )
        ),
        result=ResultData(external_links=[ExternalLink(external_link="https://singed-url")]),
        statement_id="bcd",
    )

    http_session = create_autospec(requests.Session)
    http_session.get("https://singed-url").json.return_value = [["1"], ["2"]]

    see = StatementExecutionExt(ws, warehouse_id="abc", http_session_factory=lambda: http_session)

    with pytest.raises(ValueError, match="id has no INTERVAL converter"):
        list(see.fetch_all("SELECT id FROM range(2)"))


def test_fetch_all_two_chunks():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(
            schema=ResultSchema(
                columns=[
                    ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT),
                    ColumnInfo(name="now", type_name=ColumnInfoTypeName.TIMESTAMP),
                ]
            )
        ),
        result=ResultData(external_links=[ExternalLink(external_link="https://first", next_chunk_index=1)]),
        statement_id="bcd",
    )

    ws.statement_execution.get_statement_result_chunk_n.return_value = ResultData(
        external_links=[ExternalLink(external_link="https://second")]
    )

    http_session = create_autospec(requests.Session)
    http_session.get(...).json.side_effect = [
        # https://first
        [["1", "2023-09-01T13:21:53Z"], ["2", "2023-09-01T13:21:53Z"]],
        # https://second
        [["3", "2023-09-01T13:21:53Z"], ["4", "2023-09-01T13:21:53Z"]],
    ]

    see = StatementExecutionExt(ws, warehouse_id="abc", http_session_factory=lambda: http_session)

    rows = list(see.fetch_all("SELECT id, NOW() AS now FROM range(4)"))
    assert len(rows) == 4
    assert [_.id for _ in rows] == [1, 2, 3, 4]

    ws.statement_execution.get_statement_result_chunk_n.assert_called_with("bcd", 1)


def test_fetch_one():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT)])),
        result=ResultData(data_array=[["4"]]),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws, warehouse_id="abc")

    row = see.fetch_one("SELECT 2+2 AS id")

    assert row.id == 4

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="abc",
        statement="SELECT 2 + 2 AS id LIMIT 1",
        format=Format.JSON_ARRAY,
        disposition=None,
        byte_limit=None,
        catalog=None,
        schema=None,
        wait_timeout=None,
    )


def test_fetch_one_none():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT)])),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws, warehouse_id="abc")

    row = see.fetch_one("SELECT 2+2 AS id")

    assert row is None


def test_fetch_one_disable_magic():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT)])),
        result=ResultData(data_array=[["4"], ["5"], ["6"]]),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws, warehouse_id="abc")

    row = see.fetch_one("SELECT 2+2 AS id", disable_magic=True)

    assert row.id == 4

    ws.statement_execution.execute_statement.assert_called_with(
        warehouse_id="abc",
        statement="SELECT 2+2 AS id",
        format=Format.JSON_ARRAY,
        disposition=None,
        byte_limit=None,
        catalog=None,
        schema=None,
        wait_timeout=None,
    )


def test_fetch_value():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT)])),
        result=ResultData(data_array=[["4"]]),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws, warehouse_id="abc")

    value = see.fetch_value("SELECT 2+2 AS id")

    assert value == 4


def test_fetch_value_none():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT)])),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws, warehouse_id="abc")

    value = see.fetch_value("SELECT 2+2 AS id")

    assert value is None


def test_callable_returns_iterator():
    ws = create_autospec(WorkspaceClient)

    ws.statement_execution.execute_statement.return_value = ExecuteStatementResponse(
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name="id", type_name=ColumnInfoTypeName.INT)])),
        result=ResultData(data_array=[["4"], ["5"], ["6"]]),
        statement_id="bcd",
    )

    see = StatementExecutionExt(ws, warehouse_id="abc")

    rows = list(see("SELECT 2+2 AS id"))

    assert len(rows) == 3
    assert rows == [Row(id=4), Row(id=5), Row(id=6)]
