import logging
from dataclasses import dataclass

import pytest

from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.deployment import SchemaDeployer

from . import views


@pytest.mark.parametrize("inventory_catalog", ["hive_metastore", "inventory"])
def test_deploys_schema(caplog, inventory_catalog: str) -> None:
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        schema="inventory",
        mod=views,
        catalog=inventory_catalog,
    )

    with caplog.at_level(logging.INFO, logger="databricks.labs.lsql.deployment"):
        deployment.deploy_schema()

    assert mock_backend.queries == [f"CREATE SCHEMA IF NOT EXISTS {inventory_catalog}.inventory"]
    assert f"Ensuring {inventory_catalog}.inventory database exists" in caplog.messages


@pytest.mark.parametrize("inventory_catalog", ["hive_metastore", "inventory"])
def test_deletes_schema(caplog, inventory_catalog: str) -> None:
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        schema="inventory",
        mod=views,
        catalog=inventory_catalog,
    )

    with caplog.at_level(logging.INFO, logger="databricks.labs.lsql.deployment"):
        deployment.delete_schema()

    assert mock_backend.queries == [f"DROP SCHEMA IF EXISTS {inventory_catalog}.inventory CASCADE"]
    assert f"Deleting {inventory_catalog}.inventory database" in caplog.messages


@dataclass
class Foo:
    first: str
    second: bool


@pytest.mark.parametrize("inventory_catalog", ["hive_metastore", "inventory"])
def test_deploys_dataclass(caplog, inventory_catalog: str) -> None:
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        schema="inventory",
        mod=views,
        catalog=inventory_catalog,
    )

    with caplog.at_level(logging.INFO, logger="databricks.labs.lsql.deployment"):
        deployment.deploy_table("foo", Foo)

    assert mock_backend.queries == [
        f"CREATE TABLE IF NOT EXISTS {inventory_catalog}.inventory.foo (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA",
    ]
    assert f"Ensuring {inventory_catalog}.inventory.foo table exists" in caplog.messages


@pytest.mark.parametrize("inventory_catalog", ["hive_metastore", "inventory"])
def test_deploys_view(caplog, inventory_catalog: str) -> None:
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        schema="inventory",
        mod=views,
        catalog=inventory_catalog,
    )

    with caplog.at_level(logging.INFO, logger="databricks.labs.lsql.deployment"):
        deployment.deploy_view("some", "some.sql")

    assert mock_backend.queries == [
        f"CREATE OR REPLACE VIEW {inventory_catalog}.inventory.some AS SELECT\n  id,\n  name\n"
        f"FROM {inventory_catalog}.inventory.something"
    ]
    assert f"Ensuring {inventory_catalog}.inventory.some view matches some.sql contents" in caplog.messages
