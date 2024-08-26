import base64
import datetime
import json
import logging
import random
import threading
import time
import types
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import requests
import sqlglot
from databricks.sdk import WorkspaceClient, errors
from databricks.sdk.errors import DataLoss, NotFound
from databricks.sdk.service.sql import (
    ColumnInfoTypeName,
    Disposition,
    Format,
    ServiceError,
    ServiceErrorCode,
    State,
    StatementState,
    StatementStatus,
)

if TYPE_CHECKING:  # MyPy complains about the double import
    from databricks.sdk.service.sql import StatementResponse
else:
    try:
        # databricks-sdk>=0.30.0
        from databricks.sdk.service.sql import StatementResponse
    except ImportError:
        # databricks-sdk<0.30.0
        from databricks.sdk.service.sql import (
            ExecuteStatementResponse as StatementResponse,
        )


MAX_SLEEP_PER_ATTEMPT = 10

MAX_PLATFORM_TIMEOUT = 50

MIN_PLATFORM_TIMEOUT = 5

logger = logging.getLogger(__name__)


class Row(tuple):
    """Row is a tuple with named fields that resembles PySpark's SQL Row API."""

    def __new__(cls, *args, **kwargs):
        """Create a new instance of Row."""
        if args and kwargs:
            raise ValueError("cannot mix positional and keyword arguments")
        if kwargs:
            # PySpark's compatibility layer
            row = tuple.__new__(cls, list(kwargs.values()))
            row.__columns__ = list(kwargs.keys())
            return row
        if len(args) == 1 and hasattr(cls, "__columns__") and isinstance(args[0], (types.GeneratorType, list, tuple)):
            # this type returned by Row.factory() and we already know the column names
            return cls(*args[0])
        if len(args) == 2 and isinstance(args[0], (list, tuple)) and isinstance(args[1], (list, tuple)):
            # UCX's compatibility layer
            row = tuple.__new__(cls, args[1])
            row.__columns__ = args[0]
            return row
        return tuple.__new__(cls, args)

    @classmethod
    def factory(cls, col_names: list[str]) -> type:
        """Create a new Row class with the given column names."""
        return type("Row", (Row,), {"__columns__": col_names})

    def as_dict(self) -> dict[str, Any]:
        """Convert the row to a dictionary with the same conventions as Databricks SDK."""
        return dict(zip(self.__columns__, self, strict=True))

    # PySpark's compatibility
    def asDict(self, recursive: bool = False) -> dict[str, Any]:
        _ = recursive
        return self.as_dict()

    def __eq__(self, other):
        """Check if the rows are equal."""
        if not isinstance(other, Row):
            return False
        # compare rows as dictionaries, because the order
        # of fields in constructor is not guaranteed
        return self.as_dict() == other.as_dict()

    def __contains__(self, item):
        """Check if the column is in the row."""
        return item in self.__columns__

    def __getitem__(self, col):
        """Get the column by index or name."""
        if isinstance(col, int | slice):
            return super().__getitem__(col)
        # if columns are named `2 + 2`, for example
        return self.__getattr__(col)

    def __getattr__(self, col):
        """Get the column by name."""
        if col.startswith("__"):
            raise AttributeError(col)
        try:
            idx = self.__columns__.index(col)
            return self[idx]
        except IndexError:
            raise AttributeError(col) from None
        except ValueError:
            raise AttributeError(col) from None

    def __repr__(self):
        """Get the string representation of the row."""
        return f"Row({', '.join(f'{k}={v!r}' for (k, v) in zip(self.__columns__, self, strict=True))})"


class StatementExecutionExt:
    """Execute SQL statements in a stateless manner.

    Primary use-case of :py:meth:`fetch_all` and :py:meth:`execute` methods is oriented at executing SQL queries in
    a stateless manner straight away from Databricks SDK for Python, without requiring any external dependencies.
    Results are fetched in JSON format through presigned external links. This is perfect for serverless applications
    like AWS Lambda, Azure Functions, or any other containerised short-lived applications, where container startup
    time is faster with the smaller dependency set.

    >>> for (pickup_zip, dropoff_zip) in see('SELECT pickup_zip, dropoff_zip FROM samples.nyctaxi.trips LIMIT 10'):
    >>>     print(f'pickup_zip={pickup_zip}, dropoff_zip={dropoff_zip}')

    Method :py:meth:`fetch_all` returns an iterator of objects, that resemble :class:`pyspark.sql.Row` APIs, but full
    compatibility is not the goal of this implementation. See :class:`Row` for more details.

    When you only need to execute the query and have no need to iterate over results, use the :py:meth:`execute`.

    Applications, that need to a more traditional SQL Python APIs with cursors, efficient data transfer of hundreds of
    megabytes or gigabytes of data serialized in Apache Arrow format, and low result fetching latency, should use
    the stateful Databricks SQL Connector for Python."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        ws: WorkspaceClient,
        disposition: Disposition | None = None,
        warehouse_id: str | None = None,
        byte_limit: int | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        timeout: timedelta = timedelta(minutes=20),
        disable_magic: bool = False,
        http_session_factory: Callable[[], requests.Session] | None = None,
    ):
        if not http_session_factory:
            http_session_factory = requests.Session
        self._ws = ws
        self._http = http_session_factory()
        self._lock = threading.Lock()
        self._warehouse_id = warehouse_id
        self._schema = schema
        self._timeout = timeout
        self._catalog = catalog
        self._disable_magic = disable_magic
        self._byte_limit = byte_limit
        self._disposition = disposition
        self._type_converters: dict[ColumnInfoTypeName, Callable[[str], Any]] = {
            ColumnInfoTypeName.ARRAY: json.loads,
            ColumnInfoTypeName.BINARY: base64.b64decode,
            ColumnInfoTypeName.BOOLEAN: lambda value: value.lower() == "true",
            ColumnInfoTypeName.CHAR: str,
            ColumnInfoTypeName.DATE: self._parse_date,
            ColumnInfoTypeName.DOUBLE: float,
            ColumnInfoTypeName.FLOAT: float,
            ColumnInfoTypeName.INT: int,
            ColumnInfoTypeName.LONG: int,
            ColumnInfoTypeName.MAP: json.loads,
            ColumnInfoTypeName.NULL: lambda _: None,
            ColumnInfoTypeName.SHORT: int,
            ColumnInfoTypeName.STRING: str,
            ColumnInfoTypeName.STRUCT: json.loads,
            ColumnInfoTypeName.TIMESTAMP: self._parse_timestamp,
        }

    def execute(
        self,
        statement: str,
        *,
        warehouse_id: str | None = None,
        byte_limit: int | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        timeout: timedelta | None = None,
    ) -> StatementResponse:
        """Execute a SQL statement and block until results are ready, including starting
        the warehouse if needed.

        This is a high-level implementation that works with fetching records in JSON format.
        It can be considered as a quick way to run SQL queries by just depending on
        Databricks SDK for Python without the need of any other compiled library dependencies.

        This method is a higher-level wrapper over Databricks SDK and fetches results
        in JSON format through the external link disposition, with client-side polling until
        the statement succeeds in execution. Whenever the statement is failed, cancelled, or
        closed, this method raises `DatabricksError` subclass with the state message and
        the relevant error code.

        To seamlessly iterate over the rows from query results, please use :py:meth:`fetch_all`.

        :param statement: str
          SQL statement to execute
        :param warehouse_id: str (optional)
            Warehouse upon which to execute a statement. If not given, it will use the warehouse specified
            in the constructor or the first available warehouse that is not in the DELETED or DELETING state.
        :param byte_limit: int (optional)
            Applies the given byte limit to the statement's result size. Byte counts are based on internal
            representations and may not match measurable sizes in the JSON format.
        :param catalog: str (optional)
            Sets default catalog for statement execution, similar to `USE CATALOG` in SQL. If not given,
            it will use the default catalog or the catalog specified in the constructor.
        :param schema: str (optional)
            Sets default schema for statement execution, similar to `USE SCHEMA` in SQL. If not given,
            it will use the default schema or the schema specified in the constructor.
        :param timeout: timedelta (optional)
            Timeout after which the query is cancelled. If timeout is less than 50 seconds,
            it is handled on the server side. If the timeout is greater than 50 seconds,
            Databricks SDK for Python cancels the statement execution and throws `TimeoutError`.
            If not given, it will use the timeout specified in the constructor.
        :return: StatementResponse
        """
        # The wait_timeout field must be 0 seconds (disables wait),
        # or between 5 seconds and 50 seconds.
        timeout, wait_timeout = self._statement_timeouts(timeout)
        logger.debug(f"Executing SQL statement: {statement}")

        # technically, we can do Disposition.EXTERNAL_LINKS, but let's push it further away.
        # format is limited to Format.JSON_ARRAY, but other iterations may include ARROW_STREAM.
        immediate_response = self._ws.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id or self._default_warehouse(),
            catalog=catalog or self._catalog,
            schema=schema or self._schema,
            disposition=self._disposition,
            byte_limit=byte_limit or self._byte_limit,
            wait_timeout=wait_timeout,
            format=Format.JSON_ARRAY,
        )

        status = immediate_response.status
        if status is None:
            status = StatementStatus(state=StatementState.FAILED)
        if status.state == StatementState.SUCCEEDED:
            return immediate_response

        self._raise_if_needed(status)

        attempt = 1
        status_message = "polling..."
        deadline = time.time() + timeout.total_seconds()
        statement_id = immediate_response.statement_id
        if not statement_id:
            msg = f"No statement id: {immediate_response}"
            raise ValueError(msg)
        while time.time() < deadline:
            res = self._ws.statement_execution.get_statement(statement_id)
            result_status = res.status
            if not result_status:
                msg = f"Result status is none: {res}"
                raise ValueError(msg)
            state = result_status.state
            if not state:
                state = StatementState.FAILED
            if state == StatementState.SUCCEEDED:
                return StatementResponse(
                    manifest=res.manifest, result=res.result, statement_id=statement_id, status=result_status
                )
            status_message = f"current status: {state.value}"
            self._raise_if_needed(result_status)
            sleep = min(attempt, MAX_SLEEP_PER_ATTEMPT)
            logger.debug(f"SQL statement {statement_id}: {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        self._ws.statement_execution.cancel_execution(statement_id)
        msg = f"timed out after {timeout}: {status_message}"
        raise TimeoutError(msg)

    def __call__(self, statement: str):
        """Execute a SQL statement and block until results are ready."""
        yield from self.fetch_all(statement)

    def fetch_all(
        self,
        statement: str,
        *,
        warehouse_id: str | None = None,
        byte_limit: int | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        timeout: timedelta | None = None,
    ) -> Iterator[Row]:
        """Execute a query and iterate over all available records.

        This method is a wrapper over :py:meth:`execute` with the handling of chunked result
        processing and deserialization of those into separate rows, which are yielded from
        a returned iterator. Every row API resembles those of :class:`pyspark.sql.Row`,
        but full compatibility is not the goal of this implementation.

        >>> ws = WorkspaceClient(...)
        >>> see = StatementExecutionExt(ws, warehouse_id=env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"), catalog="samples")
        >>> for row in see("SELECT * FROM nyctaxi.trips LIMIT 10"):
        >>>     pickup_time, dropoff_time = row[0], row[1]
        >>>     pickup_zip = row.pickup_zip
        >>>     dropoff_zip = row["dropoff_zip"]
        >>>     all_fields = row.as_dict()
        >>>     logger.info(f"{pickup_zip}@{pickup_time} -> {dropoff_zip}@{dropoff_time}: {all_fields}")

        :param statement: str
            SQL statement to execute
        :param warehouse_id: str (optional)
            Warehouse upon which to execute a statement. See :py:meth:`execute` for more details.
        :param byte_limit: int (optional)
            Result-size limit in bytes. See :py:meth:`execute` for more details.
        :param catalog: str (optional)
            Catalog for statement execution. See :py:meth:`execute` for more details.
        :param schema: str (optional)
            Schema for statement execution. See :py:meth:`execute` for more details.
        :param timeout: timedelta (optional)
            Timeout after which the query is cancelled. See :py:meth:`execute` for more details.
        :return: Iterator[Row]
        """
        execute_response = self.execute(
            statement, warehouse_id=warehouse_id, byte_limit=byte_limit, catalog=catalog, schema=schema, timeout=timeout
        )
        assert execute_response.statement_id is not None
        result_data = execute_response.result
        if result_data is None:
            return
        converter = self._result_converter(execute_response)
        while True:
            if result_data.data_array:
                for data in result_data.data_array:
                    yield converter(data)
            next_chunk_index = result_data.next_chunk_index
            if result_data.external_links:
                for external_link in result_data.external_links:
                    assert external_link.external_link is not None
                    next_chunk_index = external_link.next_chunk_index
                    response = self._http.get(external_link.external_link)
                    response.raise_for_status()
                    for data in response.json():
                        yield converter(data)
            if not next_chunk_index:
                return
            result_data = self._ws.statement_execution.get_statement_result_chunk_n(
                execute_response.statement_id, next_chunk_index
            )

    def fetch_one(self, statement: str, disable_magic: bool = False, **kwargs) -> Row | None:
        """Execute a query and fetch the first available record.

        This method is a wrapper over :py:meth:`fetch_all` and fetches only the first row
        from the result set. If no records are available, it returns `None`.

        >>> row = see.fetch_one('SELECT * FROM samples.nyctaxi.trips LIMIT 1')
        >>> if row:
        >>>     pickup_time, dropoff_time = row[0], row[1]
        >>>     pickup_zip = row.pickup_zip
        >>>     dropoff_zip = row['dropoff_zip']
        >>>     all_fields = row.as_dict()
        >>>     print(f'{pickup_zip}@{pickup_time} -> {dropoff_zip}@{dropoff_time}: {all_fields}')

        :param statement: str
            SQL statement to execute
        :param disable_magic: bool (optional)
            Disables the magic of adding `LIMIT 1` to the statement. By default, it is `False`.
        :param kwargs: dict
            Additional keyword arguments to pass to :py:meth:`fetch_all`
        :return: Row | None
        """
        disable_magic = disable_magic or self._disable_magic
        if not disable_magic:
            statement = self._add_limit(statement)
        for row in self.fetch_all(statement, **kwargs):
            return row
        return None

    def fetch_value(self, statement: str, **kwargs) -> Any | None:
        """Execute a query and fetch the first available value."""
        for (v,) in self.fetch_all(statement, **kwargs):
            return v
        return None

    def _result_converter(self, execute_response: StatementResponse):
        """Get the result schema from the execute response."""
        manifest = execute_response.manifest
        if not manifest:
            msg = f"missing manifest: {execute_response}"
            raise ValueError(msg)
        manifest_schema = manifest.schema
        if not manifest_schema:
            msg = f"missing schema: {manifest}"
            raise ValueError(msg)
        col_names = []
        col_conv: list[Callable[[str], Any]] = []
        columns = manifest_schema.columns
        if not columns:
            columns = []
        for col in columns:
            assert col.name is not None
            col_names.append(col.name)
            type_name = col.type_name
            if not type_name:
                type_name = ColumnInfoTypeName.NULL
            conv = self._type_converters.get(type_name, None)
            if conv is None:
                msg = f"{col.name} has no {type_name.value} converter"
                raise ValueError(msg)
            col_conv.append(conv)
        row_factory = Row.factory(col_names)

        def converter(data: list[str | None]) -> Row:
            # enumerate() + iterator + tuple constructor makes it more performant on larger humber of records
            # for Python, even though it's far less readable code.
            return row_factory(col_conv[i](value) if value else None for i, value in enumerate(data))

        return converter

    def _statement_timeouts(self, timeout) -> tuple[timedelta, str | None]:
        """Set server-side and client-side timeouts for statement execution."""
        if timeout is None:
            timeout = self._timeout
        wait_timeout = None
        if MIN_PLATFORM_TIMEOUT <= timeout.total_seconds() <= MAX_PLATFORM_TIMEOUT:
            # set server-side timeout
            wait_timeout = f"{timeout.total_seconds()}s"
        assert timeout is not None
        return timeout, wait_timeout

    @staticmethod
    def _parse_date(value: str) -> datetime.date:
        """Parse date from string in ISO format."""
        year, month, day = value.split("-")
        return datetime.date(int(year), int(month), int(day))

    @staticmethod
    def _parse_timestamp(value: str) -> datetime.datetime:
        """Parse timestamp from string in ISO format."""
        # make it work with Python 3.7 to 3.10 as well
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _raise_if_needed(status: StatementStatus):
        """Raise an exception if the statement status is failed, canceled, or closed."""
        if status.state not in [StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED]:
            return
        status_error = status.error
        if status_error is None:
            status_error = ServiceError(message="unknown", error_code=ServiceErrorCode.UNKNOWN)
        error_message = status_error.message
        if error_message is None:
            error_message = ""
        if "SCHEMA_NOT_FOUND" in error_message:
            raise NotFound(error_message)
        if "TABLE_OR_VIEW_NOT_FOUND" in error_message:
            raise NotFound(error_message)
        if "DELTA_TABLE_NOT_FOUND" in error_message:
            raise NotFound(error_message)
        if "does not exist" in error_message:
            raise NotFound(error_message)
        if "DELTA_MISSING_TRANSACTION_LOG" in error_message:
            raise DataLoss(error_message)
        mapping = {
            ServiceErrorCode.ABORTED: errors.Aborted,
            ServiceErrorCode.ALREADY_EXISTS: errors.AlreadyExists,
            ServiceErrorCode.BAD_REQUEST: errors.BadRequest,
            ServiceErrorCode.CANCELLED: errors.Cancelled,
            ServiceErrorCode.DEADLINE_EXCEEDED: errors.DeadlineExceeded,
            ServiceErrorCode.INTERNAL_ERROR: errors.InternalError,
            ServiceErrorCode.IO_ERROR: errors.InternalError,
            ServiceErrorCode.NOT_FOUND: errors.NotFound,
            ServiceErrorCode.RESOURCE_EXHAUSTED: errors.ResourceExhausted,
            ServiceErrorCode.SERVICE_UNDER_MAINTENANCE: errors.TemporarilyUnavailable,
            ServiceErrorCode.TEMPORARILY_UNAVAILABLE: errors.TemporarilyUnavailable,
            ServiceErrorCode.UNAUTHENTICATED: errors.Unauthenticated,
            ServiceErrorCode.UNKNOWN: errors.Unknown,
            ServiceErrorCode.WORKSPACE_TEMPORARILY_UNAVAILABLE: errors.TemporarilyUnavailable,
        }
        error_code = status_error.error_code
        if error_code is None:
            error_code = ServiceErrorCode.UNKNOWN
        error_class = mapping.get(error_code, errors.Unknown)
        raise error_class(error_message)

    def _default_warehouse(self) -> str:
        """Get the default warehouse id from the workspace client configuration
        or DATABRICKS_WAREHOUSE_ID environment variable. If not set, it will use
        the first available warehouse that is not in the DELETED or DELETING state."""
        with self._lock:
            if self._warehouse_id:
                return self._warehouse_id
            # if we create_autospec(WorkspaceClient), the warehouse_id is a MagicMock
            if isinstance(self._ws.config.warehouse_id, str) and self._ws.config.warehouse_id:
                self._warehouse_id = self._ws.config.warehouse_id
                return self._ws.config.warehouse_id
            ids = []
            for v in self._ws.warehouses.list():
                assert v.id is not None
                if v.state in [State.DELETED, State.DELETING]:
                    continue
                if v.state == State.RUNNING:
                    self._ws.config.warehouse_id = v.id
                    return self._ws.config.warehouse_id
                ids.append(v.id)
            if len(ids) > 0:
                # otherwise - first warehouse
                self._ws.config.warehouse_id = ids[0]
                return self._ws.config.warehouse_id
            raise ValueError(
                "no warehouse_id=... given, "
                "neither it is set in the WorkspaceClient(..., warehouse_id=...), "
                "nor in the DATABRICKS_WAREHOUSE_ID environment variable"
            )

    @staticmethod
    def _add_limit(statement: str) -> str:
        """Add a limit 1 to the statement if it does not have one already."""
        statements = sqlglot.parse(statement, read="databricks")
        if not statements:
            raise ValueError(f"cannot parse statement: {statement}")
        statement_ast = statements[0]
        if isinstance(statement_ast, sqlglot.expressions.Select):
            if statement_ast.limit is not None:
                limit = statement_ast.args.get("limit", None)
                if limit and limit.text("expression") != "1":
                    raise ValueError(f"limit is not 1: {limit.text('expression')}")
            return statement_ast.limit(expression=1).sql("databricks")
        return statement
