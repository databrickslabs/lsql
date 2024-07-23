from pathlib import Path
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient

from databricks.labs.lsql import cli


def test_cli_create_dashboard_invokes_deploy_dashboard():
    ws = create_autospec(WorkspaceClient)

    dashboard_path = Path(__file__).parent / "queries"
    cli.create_dashboard(ws, dashboard_path, open_browser="f")

    ws.lakeview.create.assert_called_once()
