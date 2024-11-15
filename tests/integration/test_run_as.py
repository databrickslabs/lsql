import pytest

from databricks.labs.blueprint.commands import CommandExecutor
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2

from databricks.labs.lsql.backends import SqlBackend
from tests.integration.test_backends import INCORRECT_SCHEMA, INCORRECT_TABLE, INCORRECT_DESCRIBE, \
    INCORRECT_TABLE_FETCH, SYNTAX_ERROR_EXECUTE, SYNTAX_ERROR_FETCH, UNKNOWN_ERROR


@pytest.fixture
def ws(make_run_as):
    run_as = make_run_as(account_groups=['role.labs.lsql.write'])
    return run_as.ws

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
