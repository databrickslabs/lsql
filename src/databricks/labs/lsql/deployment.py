import logging
import pkgutil
from typing import Any

from databricks.labs.lsql.backends import Dataclass, SqlBackend

logger = logging.getLogger(__name__)


class SchemaDeployer:
    """Deploy schema, tables, and views for a given inventory schema."""

    def __init__(
        self,
        sql_backend: SqlBackend,
        schema: str,
        mod: Any,
        *,
        catalog: str = "hive_metastore",
    ) -> None:
        self._sql_backend = sql_backend
        self._schema = schema
        self._module = mod
        self._catalog = catalog

    def deploy_schema(self) -> None:
        schema_full_name = f"{self._catalog}.{self._schema}"
        logger.info(f"Ensuring {schema_full_name} database exists")
        self._sql_backend.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_full_name}")

    def delete_schema(self) -> None:
        schema_full_name = f"{self._catalog}.{self._schema}"
        logger.info(f"Deleting {schema_full_name} database")
        self._sql_backend.execute(f"DROP SCHEMA IF EXISTS {schema_full_name} CASCADE")

    def deploy_table(self, name: str, klass: Dataclass) -> None:
        table_full_name = f"{self._catalog}.{self._schema}.{name}"
        logger.info(f"Ensuring {table_full_name} table exists")
        self._sql_backend.create_table(table_full_name, klass)

    def deploy_view(self, name: str, relative_filename: str) -> None:
        query = self._load(relative_filename)
        view_full_name = f"{self._catalog}.{self._schema}.{name}"
        logger.info(f"Ensuring {view_full_name} view matches {relative_filename} contents")
        ddl = f"CREATE OR REPLACE VIEW {view_full_name} AS {query}"
        self._sql_backend.execute(ddl)

    def _load(self, relative_filename: str) -> str:
        data = pkgutil.get_data(self._module.__name__, relative_filename)
        assert data is not None
        sql = data.decode("utf-8")
        sql = sql.replace("$inventory", f"{self._catalog}.{self._schema}")
        return sql
