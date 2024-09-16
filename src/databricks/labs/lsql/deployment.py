import datetime as dt
import logging
import pkgutil
from typing import Any

from databricks.sdk.errors import InternalError
from databricks.sdk.retries import retried

from databricks.labs.lsql.backends import Dataclass, SqlBackend

logger = logging.getLogger(__name__)


class SchemaDeployer:
    def __init__(self, sql_backend: SqlBackend, inventory_schema: str, mod: Any):
        self._sql_backend = sql_backend
        self._inventory_schema = inventory_schema
        self._module = mod

    # InternalError are retried for resilience on sporadic Databricks issues
    @retried(on=[InternalError], timeout=dt.timedelta(minutes=1))
    def deploy_schema(self):
        logger.info(f"Ensuring {self._inventory_schema} database exists")
        self._sql_backend.execute(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{self._inventory_schema}")

    def delete_schema(self):
        logger.info(f"deleting {self._inventory_schema} database")

        self._sql_backend.execute(f"DROP SCHEMA IF EXISTS hive_metastore.{self._inventory_schema} CASCADE")

    def deploy_table(self, name: str, klass: Dataclass):
        logger.info(f"Ensuring {self._inventory_schema}.{name} table exists")
        self._sql_backend.create_table(f"hive_metastore.{self._inventory_schema}.{name}", klass)

    def deploy_view(self, name: str, relative_filename: str):
        query = self._load(relative_filename)
        logger.info(f"Ensuring {self._inventory_schema}.{name} view matches {relative_filename} contents")
        ddl = f"CREATE OR REPLACE VIEW hive_metastore.{self._inventory_schema}.{name} AS {query}"
        self._sql_backend.execute(ddl)

    def _load(self, relative_filename: str) -> str:
        data = pkgutil.get_data(self._module.__name__, relative_filename)
        assert data is not None
        sql = data.decode("utf-8")
        sql = sql.replace("$inventory", f"hive_metastore.{self._inventory_schema}")
        return sql
