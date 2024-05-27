import json
from dataclasses import fields, is_dataclass
from pathlib import Path

import pytest

from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview.model import CounterSpec


@pytest.fixture
def dashboard_id(ws, make_random):
    """Clean the lakeview dashboard"""

    dashboard_display_name = f"created_by_lsql_{make_random()}"
    dashboard = ws.lakeview.create(dashboard_display_name)

    yield dashboard.dashboard_id

    ws.lakeview.trash(dashboard.dashboard_id)


def test_load_dashboard(ws):
    dashboard = Dashboards(ws)
    src = "/Workspace/Users/serge.smertin@databricks.com/Trivial Dashboard.lvdash.json"
    dst = Path(__file__).parent / "sample"
    dashboard.save_to_folder(src, dst)


def test_dashboard_creates_one_dataset_per_query(ws):
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboards(ws).create_dashboard(queries)
    assert len(dashboard.datasets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_creates_one_counter_widget_per_query(ws):
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboards(ws).create_dashboard(queries)

    counter_widgets = []
    for page in dashboard.pages:
        for layout in page.layout:
            if isinstance(layout.widget.spec, CounterSpec):
                counter_widgets.append(layout.widget)

    assert len(counter_widgets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_deploys_dashboard(ws, dashboard_id):
    queries = Path(__file__).parent / "queries"
    dashboard_client = Dashboards(ws)
    lakeview_dashboard = dashboard_client.create_dashboard(queries)

    dashboard = dashboard_client.deploy_dashboard(lakeview_dashboard, dashboard_id=dashboard_id)

    assert dashboard_client.get_dashboard(dashboard.path).as_dict() == lakeview_dashboard.as_dict()
