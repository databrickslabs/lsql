import json
import sys
from pathlib import Path
from unittest import mock
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient

from databricks.labs.lsql import cli
from databricks.labs.lsql.cli import lsql


def test_cli_create_dashboard_invokes_deploy_dashboard():
    ws = create_autospec(WorkspaceClient)

    dashboard_path = Path(__file__).parent / "queries"
    cli.create_dashboard(ws, dashboard_path, open_browser="f")

    ws.lakeview.create.assert_called_once()


def test_cli_fmt_excludes_directories():
    path = Path(__file__).parent.parent.parent
    json_command = json.dumps(
        {
            "command": "fmt",
            "flags": {"log_level": "disabled", "folder": path.as_posix(), "exclude": ["tests/unit/samples/"]},
        }
    )
    with mock.patch.object(sys, "argv", [..., json_command]):
        lsql()
