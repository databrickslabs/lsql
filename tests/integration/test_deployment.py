import pytest

from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.labs.lsql.deployment import SchemaDeployer

from . import views


@pytest.mark.xfail
def test_deploys_database(ws, env_or_skip, make_random) -> None:
    # TODO: create per-project/per-scope catalog
    schema = "default"
    sql_backend = StatementExecutionBackend(ws, env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))

    deployer = SchemaDeployer(sql_backend, schema, views)
    deployer.deploy_schema()
    deployer.deploy_table("foo", views.Foo)
    deployer.deploy_view("some", "some.sql")

    sql_backend.save_table(f"{schema}.foo", [views.Foo("abc", True)], views.Foo)
    rows = list(sql_backend.fetch(f"SELECT * FROM {schema}.some"))

    assert rows == [Row(name="abc", id=1)]
