from pathlib import Path

from databricks.labs.lsql.dashboard import Dashboard


def test_load_dashboard(ws):
    dashboard = Dashboard(ws)
    src = "/Workspace/Users/serge.smertin@databricks.com/Trivial Dashboard.lvdash.json"
    dst = Path(__file__).parent / "sample"
    dashboard.save_to_folder(src, dst)


def test_dashboard_deploy_queries(ws):
    dashboard = Dashboard(ws)
    queries = Path(__file__).parent / "queries"
    dashboard.deploy(queries)
