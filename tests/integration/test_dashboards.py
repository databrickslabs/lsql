import json
from dataclasses import fields, is_dataclass
from pathlib import Path

from databricks.labs.lsql.dashboard import Dashboard


def test_load_dashboard(ws):
    dashboard = Dashboard(ws)
    src = "/Workspace/Users/serge.smertin@databricks.com/Trivial Dashboard.lvdash.json"
    dst = Path(__file__).parent / "sample"
    dashboard.save_to_folder(src, dst)