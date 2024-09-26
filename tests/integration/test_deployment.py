from databricks.labs.lsql import Row
from databricks.labs.lsql.deployment import SchemaDeployer

from . import views


def test_deploys_schema(ws, sql_backend, make_random, make_catalog) -> None:
    """Test deploying a full, minimal inventory schema with a single schema, table and view."""
    catalog = make_catalog(name=f"lsql_test_{make_random()}")
    schema_name = "lsql_test"
    table_full_name = f"{catalog.name}.{schema_name}.foo"

    deployer = SchemaDeployer(sql_backend, schema_name, views, catalog=catalog.name)
    deployer.deploy_schema()
    deployer.deploy_table("foo", views.Foo)
    deployer.deploy_view("some", "some.sql")

    sql_backend.save_table(table_full_name, [views.Foo("abc", True)], views.Foo)

    rows = list(sql_backend.fetch(f"SELECT * FROM {table_full_name}"))
    assert rows == [Row(first="abc", second=1)]
