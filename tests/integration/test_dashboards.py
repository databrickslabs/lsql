from pathlib import Path

import pytest

from databricks.labs.lsql.dashboard import Dashboard
from databricks.labs.lsql.lakeview.model import CounterSpec


@pytest.fixture
def dashboard_id(ws, make_random):
    """Clean the lakeview dashboard"""

    dashboard_display_name = f"created_by_lsql_{make_random()}"
    dashboard = ws.lakeview.create(dashboard_display_name)

    yield dashboard.dashboard_id

    ws.lakeview.trash(dashboard.dashboard_id)


def test_load_dashboard(ws):
    dashboard = Dashboard(ws)
    src = "/Workspace/Users/serge.smertin@databricks.com/Trivial Dashboard.lvdash.json"
    dst = Path(__file__).parent / "sample"
    dashboard.save_to_folder(src, dst)


def test_dashboard_deploys_one_dataset_per_query(ws):
def test_dashboard_deploys_one_dataset_per_query(ws, make_random):
def test_dashboard_creates_one_dataset_per_query(ws, make_random):
def test_dashboard_creates_one_dataset_per_query(ws):
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboard(ws).create(queries)
    assert len(dashboard.datasets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_creates_one_counter_widget_per_query(ws):
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboard(ws).create(queries)

    counter_widgets = []
    for page in dashboard.pages:
        for layout in page.layout:
            if isinstance(layout.widget.spec, CounterSpec):
                counter_widgets.append(layout.widget)

    assert len(counter_widgets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_deploys_dashboard(ws, dashboard_id):
    queries = Path(__file__).parent / "queries"
    dashboard_client = Dashboard(ws)
    lakeview_dashboard = dashboard_client.create(queries)

    dashboard = dashboard_client.deploy(lakeview_dashboard, dashboard_id=dashboard_id)

    verify_dashboard = ws.lakeview.get(dashboard.dashboard_id)  # To be sure the dashboard is created in the workspace

    assert verify_dashboard.dashboard_id is not None
    assert verify_dashboard.dashboard_id == dashboard_id
