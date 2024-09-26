from dataclasses import dataclass

import pytest

from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.deployment import SchemaDeployer

from . import views


@pytest.mark.parametrize("inventory_catalog", ["hive_metastore", "inventory"])
def test_deploys_schema(inventory_catalog: str) -> None:
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        inventory_schema="inventory",
        mod=views,
        inventory_catalog=inventory_catalog,
    )

    deployment.deploy_schema()

    assert mock_backend.queries == [f"CREATE SCHEMA IF NOT EXISTS {inventory_catalog}.inventory"]


@pytest.mark.parametrize("inventory_catalog", ["hive_metastore", "inventory"])
def test_deletes_schema(inventory_catalog: str) -> None:
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        inventory_schema="inventory",
        mod=views,
        inventory_catalog=inventory_catalog,
    )

    deployment.delete_schema()

    assert mock_backend.queries == [f"DROP SCHEMA IF EXISTS {inventory_catalog}.inventory CASCADE"]


@pytest.mark.parametrize("inventory_catalog", ["hive_metastore", "inventory"])
def test_deploys_view(inventory_catalog: str) -> None:
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        inventory_schema="inventory",
        mod=views,
        inventory_catalog=inventory_catalog,
    )

    deployment.deploy_view("some", "some.sql")

    assert mock_backend.queries == [
        f"CREATE OR REPLACE VIEW {inventory_catalog}.inventory.some AS SELECT\n  id,\n  name\n"
        f"FROM {inventory_catalog}.inventory.something"
    ]


@dataclass
class Foo:
    first: str
    second: bool


@pytest.mark.parametrize("inventory_catalog", ["hive_metastore", "inventory"])
def test_deploys_dataclass(inventory_catalog: str) -> None:
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        inventory_schema="inventory",
        mod=views,
        inventory_catalog=inventory_catalog,
    )
    deployment.deploy_schema()
    deployment.deploy_table("foo", Foo)
    deployment.delete_schema()

    assert mock_backend.queries == [
        f"CREATE SCHEMA IF NOT EXISTS {inventory_catalog}.inventory",
        f"CREATE TABLE IF NOT EXISTS {inventory_catalog}.inventory.foo (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA",
        f"DROP SCHEMA IF EXISTS {inventory_catalog}.inventory CASCADE",
    ]
