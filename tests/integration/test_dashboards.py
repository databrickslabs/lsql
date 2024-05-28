import json
from dataclasses import fields, is_dataclass
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



def replace_recursively(dataklass, replace_fields):
    for field in fields(dataklass):
        value = getattr(dataklass, field.name)
        if is_dataclass(value):
            new_value = replace_recursively(value, replace_fields)
        elif isinstance(value, list):
            new_value = [replace_recursively(v, replace_fields) for v in value]
        elif isinstance(value, tuple):
            new_value = (replace_recursively(v, replace_fields) for v in value)
        else:
            new_value = replace_fields.get(field.name, value)
        setattr(dataklass, field.name, new_value)
    return dataklass


def test_dashboard_deploys_dashboard_the_same_as_created_dashboard(ws, dashboard_id):
    queries = Path(__file__).parent / "dashboards" / "dashboard"
    dashboards = Dashboards(ws)
    dashboard = dashboards.create_dashboard(queries)

    sdk_dashboard = dashboards.deploy_dashboard(dashboard, dashboard_id=dashboard_id)
    new_dashboard = dashboards.get_dashboard(sdk_dashboard.path)

    assert dashboards.with_better_names(dashboard).as_dict() == dashboards.with_better_names(new_dashboard).as_dict()