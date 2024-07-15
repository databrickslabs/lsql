from dataclasses import dataclass

from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.deployment import SchemaDeployer

from . import views


def test_deploys_view():
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        inventory_schema="inventory",
        mod=views,
    )

    deployment.deploy_view("some", "some.sql")

    assert mock_backend.queries == [
        "CREATE OR REPLACE VIEW hive_metastore.inventory.some AS SELECT\n  id,\n  name\nFROM hive_metastore.inventory.something"
    ]


@dataclass
class Foo:
    first: str
    second: bool


def test_deploys_dataclass():
    mock_backend = MockBackend()
    deployment = SchemaDeployer(
        sql_backend=mock_backend,
        inventory_schema="inventory",
        mod=views,
    )
    deployment.deploy_schema()
    deployment.deploy_table("foo", Foo)
    deployment.delete_schema()

    assert mock_backend.queries == [
        "CREATE SCHEMA IF NOT EXISTS hive_metastore.inventory",
        "CREATE TABLE IF NOT EXISTS hive_metastore.inventory.foo (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA",
        "DROP SCHEMA IF EXISTS hive_metastore.inventory CASCADE",
    ]
