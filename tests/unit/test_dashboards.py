from unittest.mock import create_autospec
from pathlib import Path

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview.model import CounterSpec, Dataset, Dashboard, Page


def test_dashboards_saves_sql_files_to_folder(tmp_path):
    ws = create_autospec(WorkspaceClient)
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboards(ws).create_dashboard(queries)

    Dashboards(ws).save_to_folder(dashboard, tmp_path)

    assert len(list(tmp_path.glob("*.sql"))) == len(dashboard.datasets)
    ws.assert_not_called()


def test_dashboards_saves_yml_files_to_folder(tmp_path):
    ws = create_autospec(WorkspaceClient)
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboards(ws).create_dashboard(queries)

    Dashboards(ws).save_to_folder(dashboard, tmp_path)

    assert len(list(tmp_path.glob("*.yml"))) == len(dashboard.pages)
    ws.assert_not_called()


def test_dashboards_creates_one_dataset_per_query():
    ws = create_autospec(WorkspaceClient)
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboards(ws).create_dashboard(queries)
    assert len(dashboard.datasets) == len([query for query in queries.glob("*.sql")])


def test_dashboards_creates_one_counter_widget_per_query():
    ws = create_autospec(WorkspaceClient)
    queries = Path(__file__).parent / "queries"
    dashboard = Dashboards(ws).create_dashboard(queries)

    counter_widgets = []
    for page in dashboard.pages:
        for layout in page.layout:
            if isinstance(layout.widget.spec, CounterSpec):
                counter_widgets.append(layout.widget)

    assert len(counter_widgets) == len([query for query in queries.glob("*.sql")])


def test_dashboards_deploy_raises_value_error_with_missing_display_name_and_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    lakeview_dashboard = Dashboard([], [])
    with pytest.raises(ValueError):
        dashboards.deploy_dashboard(lakeview_dashboard)
    ws.assert_not_called()


def test_dashboards_deploy_raises_value_error_with_both_display_name_and_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    lakeview_dashboard = Dashboard([], [])
    with pytest.raises(ValueError):
        dashboards.deploy_dashboard(lakeview_dashboard, display_name="test", dashboard_id="test")
    ws.assert_not_called()


def test_dashboards_deploy_calls_create_with_display_name():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    lakeview_dashboard = Dashboard([], [])
    dashboards.deploy_dashboard(lakeview_dashboard, display_name="test")

    ws.lakeview.create.assert_called_once()
    ws.lakeview.update.assert_not_called()


def test_dashboards_deploy_calls_update_with_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    lakeview_dashboard = Dashboard([], [])
    dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id="test")

    ws.lakeview.create.assert_not_called()
    ws.lakeview.update.assert_called_once()


def test_dashboards_with_better_names_replaces_dataset_names_with_display_names():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)

    datasets = [Dataset(name="ugly", query="SELECT 1", display_name="pretty")]
    dashboard = dashboards.with_better_names(Dashboard(datasets, []))

    assert all(dataset.name == "pretty" for dataset in dashboard.datasets)
    ws.assert_not_called()


def test_dashboards_with_better_names_replaces_page_names_with_display_names():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)

    pages = [Page(name="ugly", layout=[], display_name="pretty")]
    dashboard = dashboards.with_better_names(Dashboard([], pages))

    assert all(page.name == "pretty" for page in dashboard.pages)
    ws.assert_not_called()
