import json
from pathlib import Path

import pytest

from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview.model import Dashboard


@pytest.fixture
def dashboard_id(ws, make_random):
    """Clean the lakeview dashboard"""

    dashboard_display_name = f"created_by_lsql_{make_random()}"
    dashboard = ws.lakeview.create(dashboard_display_name)

    yield dashboard.dashboard_id

    ws.lakeview.trash(dashboard.dashboard_id)


def test_dashboards_deploys_exported_dashboard_definition(ws, dashboard_id):
    dashboard_file = Path(__file__).parent / "dashboards" / "dashboard.json"
    with dashboard_file.open("r") as f:
        lakeview_dashboard = Dashboard.from_dict(json.load(f))

    dashboards = Dashboards(ws)
    dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=dashboard_id)

    assert ws.lakeview.get(dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_the_same_as_created_dashboard(ws, dashboard_id):
    queries = Path(__file__).parent / "dashboards" / "dashboard"
    dashboards = Dashboards(ws)
    dashboard = dashboards.create_dashboard(queries)

    sdk_dashboard = dashboards.deploy_dashboard(dashboard, dashboard_id=dashboard_id)
    new_dashboard = dashboards.get_dashboard(sdk_dashboard.path)

    assert dashboards.with_better_names(dashboard).as_dict() == dashboards.with_better_names(new_dashboard).as_dict()
