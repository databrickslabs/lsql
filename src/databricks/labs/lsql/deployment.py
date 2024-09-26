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
        inventory_schema: str,
        mod: Any,
        *,
        inventory_catalog: str = "hive_metastore",
    ) -> None:
        self._sql_backend = sql_backend
        self._inventory_schema = inventory_schema
        self._module = mod
        self._inventory_catalog = inventory_catalog

    def deploy_schema(self) -> None:
        schema_name = f"{self._inventory_catalog}.{self._inventory_schema}"
        logger.info(f"Ensuring {schema_name} database exists")
        self._sql_backend.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def delete_schema(self) -> None:
        schema_name = f"{self._inventory_catalog}.{self._inventory_schema}"
        logger.info(f"Deleting {schema_name} database")
        self._sql_backend.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")

    def deploy_table(self, name: str, klass: Dataclass) -> None:
        table_name = f"{self._inventory_catalog}.{self._inventory_schema}.{name}"
        logger.info(f"Ensuring {table_name} table exists")
        self._sql_backend.create_table(table_name, klass)

    def deploy_view(self, name: str, relative_filename: str) -> None:
        query = self._load(relative_filename)
        view_name = f"{self._inventory_catalog}.{self._inventory_schema}.{name}"
        logger.info(f"Ensuring {view_name} view matches {relative_filename} contents")
        ddl = f"CREATE OR REPLACE VIEW {view_name} AS {query}"
        self._sql_backend.execute(ddl)

    def _load(self, relative_filename: str) -> str:
        data = pkgutil.get_data(self._module.__name__, relative_filename)
        assert data is not None
        sql = data.decode("utf-8")
        sql = sql.replace("$inventory", f"{self._inventory_catalog}.{self._inventory_schema}")
        return sql
