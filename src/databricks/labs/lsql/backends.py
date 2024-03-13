import dataclasses
import logging
import os
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Sequence
from types import UnionType
from typing import Any, ClassVar, Protocol, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    DatabricksError,
    DataLoss,
    NotFound,
    PermissionDenied,
    Unknown,
)

from databricks.labs.lsql.core import Row, StatementExecutionExt

logger = logging.getLogger(__name__)


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


Result = TypeVar("Result", bound=DataclassInstance)
Dataclass = type[DataclassInstance]
ResultFn = Callable[[], Iterable[Result]]


class SqlBackend(ABC):
    """Abstract base class for SQL backends.

    This class is used to define the interface for SQL backends. It is used to
    define the methods that are required to be implemented by any SQL backend
    that is used by the library. The methods defined in this class are used to
    execute SQL statements, fetch results from SQL statements, and save data
    to tables."""

    @abstractmethod
    def execute(self, sql: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def fetch(self, sql: str) -> Iterator[Any]:
        raise NotImplementedError

    @abstractmethod
    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode: str = "append"):
        raise NotImplementedError

    def create_table(self, full_name: str, klass: Dataclass):
        ddl = f"CREATE TABLE IF NOT EXISTS {full_name} ({self._schema_for(klass)}) USING DELTA"
        self.execute(ddl)

    _builtin_type_mapping: ClassVar[dict[type, str]] = {
        str: "STRING",
        int: "LONG",
        bool: "BOOLEAN",
        float: "FLOAT",
    }

    @classmethod
    def _schema_for(cls, klass: Dataclass):
        fields = []
        for f in dataclasses.fields(klass):
            field_type = f.type
            if isinstance(field_type, UnionType):
                field_type = field_type.__args__[0]
            if field_type not in cls._builtin_type_mapping:
                msg = f"Cannot auto-convert {field_type}"
                raise SyntaxError(msg)
            not_null = " NOT NULL"
            if f.default is None:
                not_null = ""
            spark_type = cls._builtin_type_mapping[field_type]
            fields.append(f"{f.name} {spark_type}{not_null}")
        return ", ".join(fields)

    @classmethod
    def _filter_none_rows(cls, rows, klass):
        if len(rows) == 0:
            return rows

        results = []
        class_fields = dataclasses.fields(klass)
        for row in rows:
            if row is None:
                continue
            for field in class_fields:
                if not hasattr(row, field.name):
                    logger.debug(f"Field {field.name} not present in row {dataclasses.asdict(row)}")
                    continue
                if field.default is not None and getattr(row, field.name) is None:
                    msg = f"Not null constraint violated for column {field.name}, row = {dataclasses.asdict(row)}"
                    raise ValueError(msg)
            results.append(row)
        return results

    _whitespace = re.compile(r"\s{2,}")

    @classmethod
    def _only_n_bytes(cls, j: str, num_bytes: int = 96) -> str:
        j = cls._whitespace.sub(" ", j)
        diff = len(j.encode("utf-8")) - num_bytes
        if diff > 0:
            return f"{j[:num_bytes]}... ({diff} more bytes)"
        return j

    @staticmethod
    def _api_error_from_message(error_message: str) -> DatabricksError:
        if "SCHEMA_NOT_FOUND" in error_message:
            return NotFound(error_message)
        if "TABLE_OR_VIEW_NOT_FOUND" in error_message:
            return NotFound(error_message)
        if "DELTA_TABLE_NOT_FOUND" in error_message:
            return NotFound(error_message)
        if "DELTA_MISSING_TRANSACTION_LOG" in error_message:
            return DataLoss(error_message)
        if "UNRESOLVED_COLUMN.WITH_SUGGESTION" in error_message:
            return BadRequest(error_message)
        if "PARSE_SYNTAX_ERROR" in error_message:
            return BadRequest(error_message)
        if "Operation not allowed" in error_message:
            return PermissionDenied(error_message)
        return Unknown(error_message)


class StatementExecutionBackend(SqlBackend):
    def __init__(self, ws: WorkspaceClient, warehouse_id, *, max_records_per_batch: int = 1000):
        self._sql = StatementExecutionExt(ws, warehouse_id=warehouse_id)
        self._max_records_per_batch = max_records_per_batch
        debug_truncate_bytes = ws.config.debug_truncate_bytes
        # while unit-testing, this value will contain a mock
        self._debug_truncate_bytes = debug_truncate_bytes if isinstance(debug_truncate_bytes, int) else 96

    def execute(self, sql: str) -> None:
        logger.debug(f"[api][execute] {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        self._sql.execute(sql)

    def fetch(self, sql: str) -> Iterator[Row]:
        logger.debug(f"[api][fetch] {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        return self._sql.fetch_all(sql)

    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode="append"):
        if mode == "overwrite":
            msg = "Overwrite mode is not yet supported"
            raise NotImplementedError(msg)
        rows = self._filter_none_rows(rows, klass)
        self.create_table(full_name, klass)
        if len(rows) == 0:
            return
        fields = dataclasses.fields(klass)
        field_names = [f.name for f in fields]
        for i in range(0, len(rows), self._max_records_per_batch):
            batch = rows[i : i + self._max_records_per_batch]
            vals = "), (".join(self._row_to_sql(r, fields) for r in batch)
            sql = f'INSERT INTO {full_name} ({", ".join(field_names)}) VALUES ({vals})'
            self.execute(sql)

    @staticmethod
    def _row_to_sql(row: DataclassInstance, fields: tuple[dataclasses.Field[Any], ...]):
        data = []
        for f in fields:
            value = getattr(row, f.name)
            field_type = f.type
            if isinstance(field_type, UnionType):
                field_type = field_type.__args__[0]
            if value is None:
                data.append("NULL")
            elif field_type == bool:
                data.append("TRUE" if value else "FALSE")
            elif field_type == str:
                value = str(value).replace("'", "''")
                data.append(f"'{value}'")
            elif field_type == int:
                data.append(f"{value}")
            else:
                msg = f"unknown type: {field_type}"
                raise ValueError(msg)
        return ", ".join(data)


class _SparkBackend(SqlBackend):
    def __init__(self, spark, debug_truncate_bytes):
        self._spark = spark
        self._debug_truncate_bytes = debug_truncate_bytes if debug_truncate_bytes is not None else 96

    def execute(self, sql: str) -> None:
        logger.debug(f"[spark][execute] {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        try:
            self._spark.sql(sql)
        except Exception as e:
            error_message = str(e)
            raise self._api_error_from_message(error_message) from None

    def fetch(self, sql: str) -> Iterator[Row]:
        logger.debug(f"[spark][fetch] {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        try:
            return self._spark.sql(sql).collect()
        except Exception as e:
            error_message = str(e)
            raise self._api_error_from_message(error_message) from None

    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode: str = "append"):
        rows = self._filter_none_rows(rows, klass)

        if len(rows) == 0:
            self.create_table(full_name, klass)
            return
        # pyspark deals well with lists of dataclass instances, as long as schema is provided
        df = self._spark.createDataFrame(rows, self._schema_for(klass))
        df.write.saveAsTable(full_name, mode=mode)


class RuntimeBackend(_SparkBackend):
    def __init__(self, debug_truncate_bytes: int | None = None):
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            msg = "Not in the Databricks Runtime"
            raise RuntimeError(msg)
        try:
            # pylint: disable-next=import-error,import-outside-toplevel,useless-suppression
            from pyspark.sql.session import (  # type: ignore[import-not-found]
                SparkSession,
            )

            super().__init__(SparkSession.builder.getOrCreate(), debug_truncate_bytes)
        except ImportError as e:
            raise RuntimeError("pyspark is not available") from e


class DatabricksConnectBackend(_SparkBackend):
    def __init__(self, ws: WorkspaceClient):
        try:
            # pylint: disable-next=import-outside-toplevel
            from databricks.connect import (  # type: ignore[import-untyped]
                DatabricksSession,
            )

            spark = DatabricksSession.builder().sdk_config(ws.config).getOrCreate()
            super().__init__(spark, ws.config.debug_truncate_bytes)
        except ImportError as e:
            raise RuntimeError("Please run `pip install databricks-connect`") from e


class MockBackend(SqlBackend):
    def __init__(
        self, *, fails_on_first: dict[str, str] | None = None, rows: dict | None = None, debug_truncate_bytes=96
    ):
        self._fails_on_first = fails_on_first
        if not rows:
            rows = {}
        self._rows = rows
        self._save_table: list[tuple[str, Sequence[DataclassInstance], str]] = []
        self._debug_truncate_bytes = debug_truncate_bytes
        self.queries: list[str] = []

    def _sql(self, sql: str):
        logger.debug(f"Mock backend.sql() received SQL: {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        seen_before = sql in self.queries
        self.queries.append(sql)
        if not seen_before and self._fails_on_first is not None:
            for match, failure in self._fails_on_first.items():
                if match in sql:
                    raise self._api_error_from_message(failure) from None

    def execute(self, sql):
        self._sql(sql)

    def fetch(self, sql) -> Iterator[Row]:
        self._sql(sql)
        rows = []
        if self._rows:
            for pattern in self._rows.keys():
                r = re.compile(pattern)
                if r.search(sql):
                    logger.debug(f"Found match: {sql}")
                    rows.extend(self._rows[pattern])
        logger.debug(f"Returning rows: {rows}")
        return iter(rows)

    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode: str = "append"):
        if mode == "overwrite":
            msg = "Overwrite mode is not yet supported"
            raise NotImplementedError(msg)
        rows = self._filter_none_rows(rows, klass)
        if klass.__class__ == type:
            row_factory = self._row_factory(klass)
            rows = [row_factory(*dataclasses.astuple(r)) for r in rows]
            self._save_table.append((full_name, rows, mode))

    def rows_written_for(self, full_name: str, mode: str) -> list[DataclassInstance]:
        rows: list[DataclassInstance] = []
        for stub_full_name, stub_rows, stub_mode in self._save_table:
            if not (stub_full_name == full_name and stub_mode == mode):
                continue
            rows += stub_rows
        return rows

    @staticmethod
    def rows(*column_names: str):
        """This method is used to create rows for the mock backend."""
        number_of_columns = len(column_names)
        row_factory = Row.factory(list(column_names))

        class MagicFactory:
            """This class is used to create rows for the mock backend."""

            def __getitem__(self, tuples: list[tuple | list] | tuple[list | tuple]) -> list[Row]:
                if not isinstance(tuples, (list, tuple)):
                    raise TypeError(f"Expected list or tuple, got {type(tuples)}")
                # fix sloppy input
                if tuples and not isinstance(tuples[0], (list, tuple)):
                    tuples = [tuples]
                out = []
                for record in tuples:
                    if not isinstance(record, (list, tuple)):
                        raise TypeError(f"Expected list or tuple, got {type(record)}")
                    if number_of_columns != len(record):
                        raise TypeError(f"Expected {number_of_columns} columns, got {len(record)}: {record}")
                    out.append(row_factory(*record))
                return out

        return MagicFactory()

    @staticmethod
    def _row_factory(klass: Dataclass) -> type:
        return Row.factory([f.name for f in dataclasses.fields(klass)])
