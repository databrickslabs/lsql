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
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboard(ws).deploy(f"lsql-D{make_random()}", queries)
    assert len(dashboard.datasets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_deploys_one_counter_widget_per_query(ws, make_random):
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboard(ws).deploy(f"lsql-D{make_random()}", queries)

    counter_widgets = []
    for page in dashboard.pages:
        for layout in page.layout:
            if isinstance(layout.widget.spec, CounterSpec):
                counter_widgets.append(layout.widget)

    assert len(counter_widgets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_deploys_dashboard(ws, make_random):
    dashboard_display_name = f"lsql-D{make_random()}"
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboard(ws).deploy(dashboard_display_name, queries)

    dashboard = ws.lakeview.get(dashboard.dashboard.dashboard_id)

    assert dashboard_display_name is not None
    assert dashboard.display_name == dashboard_display_name