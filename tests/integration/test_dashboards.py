import json
from dataclasses import fields, is_dataclass
from pathlib import Path

from databricks.labs.lsql.dashboard import Dashboard


def test_load_dashboard(ws):
    dashboards = Dashboard(ws)
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


def test_dashboard_deploys_dashboard(ws, dashboard_id):
    queries = Path(__file__).parent / "queries"
    dashboard_client = Dashboards(ws)
    lakeview_dashboard = dashboard_client.create_dashboard(queries)

    dashboard = dashboard_client.deploy_dashboard(lakeview_dashboard, dashboard_id=dashboard_id)
    deployed_lakeview_dashboard = dashboard_client.get_dashboard(dashboard.path)

    replace_name = {"name": "test", "dataset_name": "test"}  # Dynamically created names
    lakeview_dashboard_wo_name = replace_recursively(lakeview_dashboard, replace_name)
    deployed_lakeview_dashboard_wo_name = replace_recursively(deployed_lakeview_dashboard, replace_name)

    assert lakeview_dashboard_wo_name.as_dict() == deployed_lakeview_dashboard_wo_name.as_dict()


def test_dashboards_deploys_exported_dashboard_definition(ws, dashboard_id):
    dashboard_file = Path(__file__).parent / "dashboards" / "dashboard.json"
    with dashboard_file.open("r") as f:
        lakeview_dashboard = Dashboard.from_dict(json.load(f))

    dashboard_client = Dashboards(ws)
    dashboard = dashboard_client.deploy_dashboard(lakeview_dashboard, dashboard_id=dashboard_id)

    assert ws.lakeview.get(dashboard.dashboard_id)
