from dataclasses import dataclass
from unittest import mock
from unittest.mock import call, create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service._internal import Wait
from databricks.sdk.service.compute import (
    CommandStatus,
    CommandStatusResponse,
    ContextStatusResponse,
    Language,
    Results,
    ResultType,
)

from databricks.labs.lsql.backends import CommandContextBackend


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


def test_command_context_backend_execute_happy():
    ws = create_autospec(WorkspaceClient)
    ws.command_execution.create.return_value = Wait[ContextStatusResponse](
        waiter=lambda callback, timeout: ContextStatusResponse(id="abc")
    )
    ws.command_execution.execute.return_value = Wait[CommandStatusResponse](
        waiter=lambda callback, timeout: CommandStatusResponse(
            results=Results(data="success"), status=CommandStatus.FINISHED
        )
    )

    ccb = CommandContextBackend(ws, "abc")

    ccb.execute("CREATE TABLE foo")

    ws.command_execution.execute.assert_called_with(
        cluster_id="abc", language=Language.SQL, context_id="abc", command="CREATE TABLE foo"
    )


def test_command_context_backend_with_overrides():
    ws = create_autospec(WorkspaceClient)
    ws.command_execution.create.return_value = Wait[ContextStatusResponse](
        waiter=lambda callback, timeout: ContextStatusResponse(id="abc")
    )
    ws.command_execution.execute.return_value = Wait[CommandStatusResponse](
        waiter=lambda callback, timeout: CommandStatusResponse(
            results=Results(data="success"), status=CommandStatus.FINISHED
        )
    )

    ccb = CommandContextBackend(ws, "abc")

    ccb.execute("CREATE TABLE foo", catalog="foo", schema="bar")

    ws.command_execution.execute.assert_has_calls(
        [
            call(cluster_id="abc", language=Language.SQL, context_id="abc", command="USE CATALOG foo"),
            call(cluster_id="abc", language=Language.SQL, context_id="abc", command="USE SCHEMA bar"),
            call(cluster_id="abc", language=Language.SQL, context_id="abc", command="CREATE TABLE foo"),
        ]
    )


def test_command_context_backend_fetch_happy():
    ws = create_autospec(WorkspaceClient)
    ws.command_execution.create.return_value = Wait[ContextStatusResponse](
        waiter=lambda callback, timeout: ContextStatusResponse(id="abc")
    )
    ws.command_execution.execute.return_value = Wait[CommandStatusResponse](
        waiter=lambda callback, timeout: CommandStatusResponse(
            results=Results(
                data=[["1"], ["2"], ["3"]],
                result_type=ResultType.TABLE,
                schema=[{"name": "id", "type": '"int"', "metadata": "{}"}],
            ),
            status=CommandStatus.FINISHED,
        )
    )

    ccb = CommandContextBackend(ws, "abc")

    result = list(ccb.fetch("SELECT id FROM range(3)"))

    assert [["1"], ["2"], ["3"]] == result


def test_command_context_backend_save_table_overwrite_empty_table():
    ws = create_autospec(WorkspaceClient)
    ws.command_execution.create.return_value = Wait[ContextStatusResponse](
        waiter=lambda callback, timeout: ContextStatusResponse(id="abc")
    )
    ws.command_execution.execute.return_value = Wait[CommandStatusResponse](
        waiter=lambda callback, timeout: CommandStatusResponse(
            results=Results(data="success"), status=CommandStatus.FINISHED
        )
    )

    ccb = CommandContextBackend(ws, "abc")
    ccb.save_table("a.b.c", [Baz("1")], Baz, mode="overwrite")

    ws.command_execution.execute.assert_has_calls(
        [
            mock.call(
                cluster_id="abc",
                language=Language.SQL,
                context_id="abc",
                command="CREATE TABLE IF NOT EXISTS a.b.c (first STRING NOT NULL, second STRING) USING DELTA",
            ),
            mock.call(
                cluster_id="abc",
                language=Language.SQL,
                context_id="abc",
                command="TRUNCATE TABLE a.b.c",
            ),
            mock.call(
                cluster_id="abc",
                language=Language.SQL,
                context_id="abc",
                command="INSERT INTO a.b.c (first, second) VALUES ('1', NULL)",
            ),
        ]
    )


def test_command_context_backend_save_table_empty_records():
    ws = create_autospec(WorkspaceClient)
    ws.command_execution.create.return_value = Wait[ContextStatusResponse](
        waiter=lambda callback, timeout: ContextStatusResponse(id="abc")
    )
    ws.command_execution.execute.return_value = Wait[CommandStatusResponse](
        waiter=lambda callback, timeout: CommandStatusResponse(
            results=Results(data="success"), status=CommandStatus.FINISHED
        )
    )

    ccb = CommandContextBackend(ws, "abc")

    ccb.save_table("a.b.c", [], Bar)

    ws.command_execution.execute.assert_called_with(
        cluster_id="abc",
        language=Language.SQL,
        context_id="abc",
        command="CREATE TABLE IF NOT EXISTS a.b.c "
        "(first STRING NOT NULL, second BOOLEAN NOT NULL, third FLOAT NOT NULL) USING DELTA",
    )


def test_command_context_backend_save_table_two_records():
    ws = create_autospec(WorkspaceClient)
    ws.command_execution.create.return_value = Wait[ContextStatusResponse](
        waiter=lambda callback, timeout: ContextStatusResponse(id="abc")
    )
    ws.command_execution.execute.return_value = Wait[CommandStatusResponse](
        waiter=lambda callback, timeout: CommandStatusResponse(
            results=Results(data="success"), status=CommandStatus.FINISHED
        )
    )

    ccb = CommandContextBackend(ws, "abc")

    ccb.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo)

    ws.command_execution.execute.assert_has_calls(
        [
            mock.call(
                cluster_id="abc",
                language=Language.SQL,
                context_id="abc",
                command="CREATE TABLE IF NOT EXISTS a.b.c (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA",
            ),
            mock.call(
                cluster_id="abc",
                language=Language.SQL,
                context_id="abc",
                command="INSERT INTO a.b.c (first, second) VALUES ('aaa', TRUE), ('bbb', FALSE)",
            ),
        ]
    )


def test_command_context_backend_save_table_in_batches_of_two(mocker):
    ws = create_autospec(WorkspaceClient)
    ws.command_execution.create.return_value = Wait[ContextStatusResponse](
        waiter=lambda callback, timeout: ContextStatusResponse(id="abc")
    )
    ws.command_execution.execute.return_value = Wait[CommandStatusResponse](
        waiter=lambda callback, timeout: CommandStatusResponse(
            results=Results(data="success"), status=CommandStatus.FINISHED
        )
    )

    ccb = CommandContextBackend(ws, "abc", max_records_per_batch=2)

    ccb.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False), Foo("ccc", True)], Foo)

    ws.command_execution.execute.assert_has_calls(
        [
            mock.call(
                cluster_id="abc",
                language=Language.SQL,
                context_id="abc",
                command="CREATE TABLE IF NOT EXISTS a.b.c (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA",
            ),
            mock.call(
                cluster_id="abc",
                language=Language.SQL,
                context_id="abc",
                command="INSERT INTO a.b.c (first, second) VALUES ('aaa', TRUE), ('bbb', FALSE)",
            ),
            mock.call(
                cluster_id="abc",
                language=Language.SQL,
                context_id="abc",
                command="INSERT INTO a.b.c (first, second) VALUES ('ccc', TRUE)",
            ),
        ]
    )
