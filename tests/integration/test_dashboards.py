from pathlib import Path

from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import Dashboards


def test_load_dashboard():
    ws = WorkspaceClient(profile='logfood-master')
    dashboards = Dashboards(ws)
    src = "/Workspace/Users/serge.smertin@databricks.com/Databricks Labs GitHub telemetry.lvdash.json"
    dst = Path(__file__).parent / "sample"
    dashboards.save_to_folder(src, dst)