import json
from dataclasses import fields, is_dataclass
from pathlib import Path

from databricks.labs.lsql.dashboard import Dashboard
from databricks.labs.lsql.lakeview.model import CounterSpec


def test_load_dashboard(ws):
    dashboard = Dashboard(ws)
    src = "/Workspace/Users/serge.smertin@databricks.com/Trivial Dashboard.lvdash.json"
    dst = Path(__file__).parent / "sample"
    dashboard.save_to_folder(src, dst)


def test_dashboard_deploys_one_dataset_per_query(ws):
def test_dashboard_deploys_one_dataset_per_query(ws, make_random):
def test_dashboard_creates_one_dataset_per_query(ws, make_random):
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboard(ws).create(queries)
    assert len(dashboard.datasets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_creates_one_counter_widget_per_query(ws, make_random):
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboard(ws).create(queries)

    counter_widgets = []
    for page in dashboard.pages:
        for layout in page.layout:
            if isinstance(layout.widget.spec, CounterSpec):
                counter_widgets.append(layout.widget)

    assert len(counter_widgets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_deploys_dashboard(ws, make_random):
    queries = Path(__file__).parent / "queries"
    dashboard_client = Dashboard(ws)
    lakeview_dashboard = dashboard_client.create(queries)

    dashboard_display_name = f"lsql-D{make_random()}"
    dashboard = dashboard_client.deploy(dashboard_display_name, lakeview_dashboard)

    verify_dashboard = ws.lakeview.get(dashboard.dashboard_id)  # To be sure the dashboard is created in the workspace

    assert verify_dashboard.display_name is not None
    assert verify_dashboard.display_name == dashboard_display_name
