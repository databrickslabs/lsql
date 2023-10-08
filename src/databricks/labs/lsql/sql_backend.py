import dataclasses
import logging
import re
from abc import ABC, abstractmethod
from typing import Iterator, ClassVar

from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.lib import StatementExecutionExt

logger = logging.getLogger(__name__)

class SqlBackend(ABC):
    @abstractmethod
    def execute(self, sql):
        raise NotImplementedError

    @abstractmethod
    def fetch(self, sql) -> Iterator[any]:
        raise NotImplementedError

    @abstractmethod
    def save_table(self, full_name: str, rows: list[any], klass: type, mode: str = "append"):
        raise NotImplementedError

    def create_table(self, full_name: str, klass: type):
        ddl = f"CREATE TABLE IF NOT EXISTS {full_name} ({self._schema_for(klass)}) USING DELTA"
        self.execute(ddl)

    _builtin_type_mapping: ClassVar[dict[type, str]] = {str: "STRING", int: "INT", bool: "BOOLEAN", float: "FLOAT"}

    @classmethod
    def _schema_for(cls, klass):
        fields = []
        for f in dataclasses.fields(klass):
            if f.type not in cls._builtin_type_mapping:
                msg = f"Cannot auto-convert {f.type}"
                raise SyntaxError(msg)
            not_null = " NOT NULL"
            if f.default is None:
                not_null = ""
            spark_type = cls._builtin_type_mapping[f.type]
            fields.append(f"{f.name} {spark_type}{not_null}")
        return ", ".join(fields)

    @classmethod
    def _filter_none_rows(cls, rows, full_name):
        if len(rows) == 0:
            return rows

        results = []
        nullable_fields = set()

        for field in dataclasses.fields(rows[0]):
            if field.default is None:
                nullable_fields.add(field.name)

        for row in rows:
            if row is None:
                continue
            row_contains_none = False
            for column, value in dataclasses.asdict(row).items():
                if value is None and column not in nullable_fields:
                    logger.warning(f"[{full_name}] Field {column} is None, filtering row")
                    row_contains_none = True
                    break

            if not row_contains_none:
                results.append(row)
        return results


class StatementExecutionBackend(SqlBackend):
    def __init__(self, ws: WorkspaceClient, warehouse_id, *, max_records_per_batch: int = 1000):
        self._sql = StatementExecutionExt(ws.api_client)
        self._warehouse_id = warehouse_id
        self._max_records_per_batch = max_records_per_batch

    def execute(self, sql):
        logger.debug(f"[api][execute] {sql}")
        self._sql.execute(self._warehouse_id, sql)

    def fetch(self, sql) -> Iterator[any]:
        logger.debug(f"[api][fetch] {sql}")
        return self._sql.execute_fetch_all(self._warehouse_id, sql)

    def save_table(self, full_name: str, rows: list[any], klass: dataclasses.dataclass, mode="append"):
        if mode == "overwrite":
            msg = "Overwrite mode is not yet supported"
            raise NotImplementedError(msg)
        rows = self._filter_none_rows(rows, full_name)
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
    def _row_to_sql(row, fields):
        data = []
        for f in fields:
            value = getattr(row, f.name)
            if value is None:
                data.append("NULL")
            elif f.type == bool:
                data.append("TRUE" if value else "FALSE")
            elif f.type == str:
                value = str(value).replace("'", "''")
                data.append(f"'{value}'")
            elif f.type == int:
                data.append(f"{value}")
            else:
                msg = f"unknown type: {f.type}"
                raise ValueError(msg)
        return ", ".join(data)


class RuntimeBackend(SqlBackend):
    def __init__(self):
        from pyspark.sql.session import SparkSession

        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            msg = "Not in the Databricks Runtime"
            raise RuntimeError(msg)

        self._spark = SparkSession.builder.getOrCreate()

    def execute(self, sql):
        logger.debug(f"[spark][execute] {sql}")
        self._spark.sql(sql)

    def fetch(self, sql) -> Iterator[any]:
        logger.debug(f"[spark][fetch] {sql}")
        return self._spark.sql(sql).collect()

    def save_table(self, full_name: str, rows: list[any], klass: dataclasses.dataclass, mode: str = "append"):
        rows = self._filter_none_rows(rows, full_name)

        if len(rows) == 0:
            self.create_table(full_name, klass)
            return
        # pyspark deals well with lists of dataclass instances, as long as schema is provided
        df = self._spark.createDataFrame(rows, self._schema_for(rows[0]))
        df.write.saveAsTable(full_name, mode=mode)

class MockBackend(SqlBackend):
    def __init__(self, *, fails_on_first: dict | None = None, rows: dict | None = None):
        self._fails_on_first = fails_on_first
        if not rows:
            rows = {}
        self._rows = rows
        self._save_table = []
        self.queries = []

    def _sql(self, sql):
        logger.debug(f"Mock backend.sql() received SQL: {sql}")
        seen_before = sql in self.queries
        self.queries.append(sql)
        if not seen_before and self._fails_on_first is not None:
            for match, failure in self._fails_on_first.items():
                if match in sql:
                    raise RuntimeError(failure)

    def execute(self, sql):
        self._sql(sql)

    def fetch(self, sql) -> Iterator[any]:
        self._sql(sql)
        rows = []
        if self._rows:
            for pattern in self._rows.keys():
                r = re.compile(pattern)
                if r.match(sql):
                    logger.debug(f"Found match: {sql}")
                    rows.extend(self._rows[pattern])
        logger.debug(f"Returning rows: {rows}")
        return iter(rows)

    def save_table(self, full_name: str, rows: list[any], klass, mode: str = "append"):
        if klass.__class__ == type:
            self._save_table.append((full_name, rows, mode))

    def rows_written_for(self, full_name: str, mode: str) -> list[any]:
        rows = []
        for stub_full_name, stub_rows, stub_mode in self._save_table:
            if not (stub_full_name == full_name and stub_mode == mode):
                continue
            rows += stub_rows
        return rows
