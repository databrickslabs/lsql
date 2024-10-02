import pytest
from databricks.labs.blueprint.commands import CommandExecutor
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2

from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend

from . import views

INCORRECT_SCHEMA = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk.errors import NotFound
backend = RuntimeBackend()
try:
    backend.execute("USE __NON_EXISTENT__")
    return "FAILED"
except NotFound as e:
    return "PASSED"
"""

INCORRECT_TABLE = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk.errors import NotFound
backend = RuntimeBackend()
try:
    backend.execute("SELECT * FROM default.__RANDOM__")
    return "FAILED"
except NotFound as e:
    return "PASSED"
"""

INCORRECT_DESCRIBE = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk.errors import NotFound
backend = RuntimeBackend()
try:
    query_response = backend.fetch("DESCRIBE __RANDOM__")
    return "FAILED"
except NotFound as e:
    return "PASSED"
"""

INCORRECT_TABLE_FETCH = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk.errors import NotFound
backend = RuntimeBackend()
try:
    query_response = backend.fetch("SELECT * FROM default.__RANDOM__")
    return "FAILED"
except NotFound as e:
    return "PASSED"
"""

SYNTAX_ERROR_EXECUTE = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk.errors import BadRequest
backend = RuntimeBackend()
try:
    backend.execute("SHWO DTABASES")
    return "FAILED"
except BadRequest:
    return "PASSED"
"""

SYNTAX_ERROR_FETCH = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk.errors import BadRequest
backend = RuntimeBackend()
try:
    query_response = backend.fetch("SHWO DTABASES")
    return "FAILED"
except BadRequest:
    return "PASSED"
"""


UNKNOWN_ERROR = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk.errors import Unknown
backend = RuntimeBackend()
try:
    grants = backend.fetch("SHOW GRANTS ON METASTORE")
    return "FAILED"
except Unknown:
    return "PASSED"
"""


@pytest.mark.xfail
def test_runtime_backend_works_maps_permission_denied(ws):
    product_info = ProductInfo.for_testing(SqlBackend)
    installation = Installation.assume_user_home(ws, product_info.product_name())
    with WheelsV2(installation, product_info) as wheels:
        wsfs_wheel = wheels.upload_to_wsfs()

    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)
    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    permission_denied_query = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk.errors import PermissionDenied
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
me = w.current_user.me()
backend = RuntimeBackend()
try:
    backend.execute(f"GRANT CREATE SCHEMA ON CATALOG main TO `{me.groups[0].display}`;")
    return "FAILED"
except PermissionDenied:
    return "PASSED"
"""
    result = commands.run(permission_denied_query)
    assert result == "PASSED"


@pytest.mark.parametrize(
    "query",
    [
        INCORRECT_SCHEMA,
        INCORRECT_TABLE,
        INCORRECT_DESCRIBE,
        INCORRECT_TABLE_FETCH,
        SYNTAX_ERROR_EXECUTE,
        SYNTAX_ERROR_FETCH,
        UNKNOWN_ERROR,
    ],
)
def test_runtime_backend_errors_handled(ws, query):
    product_info = ProductInfo.for_testing(SqlBackend)
    installation = Installation.assume_user_home(ws, product_info.product_name())
    with WheelsV2(installation, product_info) as wheels:
        wsfs_wheel = wheels.upload_to_wsfs()

    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    result = commands.run(query)
    assert result == "PASSED"


def test_statement_execution_backend_works(ws, env_or_skip):
    sql_backend = StatementExecutionBackend(ws, env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))
    rows = list(sql_backend.fetch("SELECT * FROM samples.nyctaxi.trips LIMIT 10"))
    assert len(rows) == 10


def test_statement_execution_backend_overrides(ws, env_or_skip):
    sql_backend = StatementExecutionBackend(ws, env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))
    rows = list(sql_backend.fetch("SELECT * FROM trips LIMIT 10", catalog="samples", schema="nyctaxi"))
    assert len(rows) == 10


def test_statement_execution_backend_overwrites_table(ws, env_or_skip, make_random) -> None:
    sql_backend = StatementExecutionBackend(ws, env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))
    catalog = env_or_skip("TEST_CATALOG")
    schema = env_or_skip("TEST_SCHEMA")

    sql_backend.save_table(f"{catalog}.{schema}.foo", [views.Foo("abc", True)], views.Foo, "append")
    sql_backend.save_table(f"{catalog}.{schema}.foo", [views.Foo("xyz", True)], views.Foo, "overwrite")

    rows = list(sql_backend.fetch(f"SELECT * FROM {catalog}.{schema}.foo"))
    assert rows == [Row(first="xyz", second=True)]

    sql_backend.save_table(f"{catalog}.{schema}.foo", [], views.Foo, "overwrite")

    rows = list(sql_backend.fetch(f"SELECT * FROM {catalog}.{schema}.foo"))
    assert rows == []


def test_runtime_backend_use_statements(ws):
    product_info = ProductInfo.for_testing(SqlBackend)
    installation = Installation.assume_user_home(ws, product_info.product_name())
    with WheelsV2(installation, product_info) as wheels:
        wsfs_wheel = wheels.upload_to_wsfs()

    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)
    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    permission_denied_query = """
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
me = w.current_user.me()
backend = RuntimeBackend()
result_set = backend.fetch(f"SELECT * FROM trips LIMIT 10", catalog="samples", schema="nyctaxi")
if len(list(result_set)) == 10:
    return "PASSED"
else:
    return "FAILED"
"""
    result = commands.run(permission_denied_query)
    assert result == "PASSED"
