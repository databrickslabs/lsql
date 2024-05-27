from pathlib import Path

from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboard import Dashboard


def test_load_dashboard(ws):
    dashboards = Dashboard(ws)
    src = "/Workspace/Users/serge.smertin@databricks.com/Trivial Dashboard.lvdash.json"
    dst = Path(__file__).parent / "sample"
    dashboards.save_to_folder(src, dst)