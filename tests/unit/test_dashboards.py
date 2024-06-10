from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview import (
    AreaSpec,
    BarSpec,
    ChartEncodingMapWithSingleXy,
    ControlEncodingMap,
    CounterEncodingMap,
    CounterSpec,
    Dashboard,
    Dataset,
    DatePickerSpec,
    DateRangePickerSpec,
    DetailsV1EncodingMap,
    DetailsV1Spec,
    DropdownSpec,
    Layout,
    LineSpec,
    MultiSelectSpec,
    NamedQuery,
    Page,
    PieEncodingMap,
    PieSpec,
    PivotEncodingMap,
    PivotSpec,
    Position,
    QuantitativeScale,
    Query,
    ScatterSpec,
    SingleFieldAxisEncoding,
    SymbolMapEncodingMap,
    SymbolMapSpec,
    TableEncodingMap,
    TableV1EncodingMap,
    TableV1Spec,
    TableV2Spec,
    TextEntrySpec,
    Widget,
    WordCloudEncodingMap,
    WordCloudSpec,
)


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


@pytest.mark.parametrize("spec, expected", [(CounterSpec(CounterEncodingMap()), (1, 3))])
def test_dashboards_gets_width_and_height_spec(spec, expected):
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    assert dashboards._get_width_and_height(spec) == expected  # pylint: disable-next=protected-access
    ws.assert_not_called()


@pytest.mark.parametrize(
    "spec",
    [
        AreaSpec(ChartEncodingMapWithSingleXy(SingleFieldAxisEncoding("x", QuantitativeScale()))),
        BarSpec(ChartEncodingMapWithSingleXy(SingleFieldAxisEncoding("x", QuantitativeScale()))),
        DatePickerSpec(ControlEncodingMap([])),
        DateRangePickerSpec(ControlEncodingMap([])),
        DetailsV1Spec(DetailsV1EncodingMap()),
        DropdownSpec(ControlEncodingMap([])),
        LineSpec(ChartEncodingMapWithSingleXy(SingleFieldAxisEncoding("x", QuantitativeScale()))),
        MultiSelectSpec(ControlEncodingMap([])),
        PieSpec(PieEncodingMap()),
        PivotSpec(PivotEncodingMap()),
        ScatterSpec(ChartEncodingMapWithSingleXy(SingleFieldAxisEncoding("x", QuantitativeScale()))),
        SymbolMapSpec(SymbolMapEncodingMap()),
        TableV1Spec(True, True, TableV1EncodingMap(), [], 1),
        TableV2Spec(TableEncodingMap()),
        TextEntrySpec(ControlEncodingMap([])),
        WordCloudSpec(WordCloudEncodingMap()),
    ],
)
def test_dashboards_raises_not_implemented_error(spec):
    # Keeps track of to-be-implemented specs, remove when implemented
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    with pytest.raises(NotImplementedError):
        dashboards._get_width_and_height(spec)  # pylint: disable-next=protected-access
    ws.assert_not_called()


def test_dashboards_deploy_raises_value_error_with_missing_display_name_and_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    dashboard = Dashboard([], [])
    with pytest.raises(ValueError):
        dashboards.deploy_dashboard(dashboard)
    ws.assert_not_called()


def test_dashboards_deploy_raises_value_error_with_both_display_name_and_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    dashboard = Dashboard([], [])
    with pytest.raises(ValueError):
        dashboards.deploy_dashboard(dashboard, display_name="test", dashboard_id="test")
    ws.assert_not_called()


def test_dashboards_deploy_calls_create_with_display_name():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    dashboard = Dashboard([], [])
    dashboards.deploy_dashboard(dashboard, display_name="test")

    ws.lakeview.create.assert_called_once()
    ws.lakeview.update.assert_not_called()


def test_dashboards_deploy_calls_update_with_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    dashboard = Dashboard([], [])
    dashboards.deploy_dashboard(dashboard, dashboard_id="test")

    ws.lakeview.create.assert_not_called()
    ws.lakeview.update.assert_called_once()


def test_dashboards_save_to_folder_replaces_dataset_names_with_display_names(tmp_path):
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)

    datasets = [Dataset(name="ugly", query="SELECT 1", display_name="pretty")]
    dashboard = dashboards.save_to_folder(Dashboard(datasets, []), tmp_path)

    assert all(dataset.name == "pretty" for dataset in dashboard.datasets)
    ws.assert_not_called()


def test_dashboards_save_to_folder_replaces_page_names_with_display_names(tmp_path):
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)

    pages = [Page(name="ugly", layout=[], display_name="pretty")]
    dashboard = dashboards.save_to_folder(Dashboard([], pages), tmp_path)

    assert all(page.name == "pretty" for page in dashboard.pages)
    ws.assert_not_called()


@pytest.fixture
def ugly_dashboard() -> Dashboard:
    datasets = [Dataset(name="ugly", query="SELECT 1", display_name="pretty")]

    query = Query(dataset_name="ugly", fields=[])
    named_query = NamedQuery(name="main_query", query=query)
    counter_spec = CounterSpec(CounterEncodingMap())
    widget = Widget(name="ugly", queries=[named_query], spec=counter_spec)
    position = Position(x=0, y=0, width=1, height=1)
    layout = Layout(widget=widget, position=position)
    pages = [Page(name="ugly", layout=[layout], display_name="pretty")]

    dashboard = Dashboard(datasets, pages)
    return dashboard


def test_dashboards_save_to_folder_replaces_query_name_with_dataset_name(ugly_dashboard, tmp_path):
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)

    dashboard = dashboards.save_to_folder(ugly_dashboard, tmp_path)

    queries = []
    for page in dashboard.pages:
        for layout in page.layout:
            for named_query in layout.widget.queries:
                queries.append(named_query.query)

    assert all(query.dataset_name == "pretty" for query in queries)
    ws.assert_not_called()


def test_dashboards_save_to_folder_replaces_counter_names(ugly_dashboard, tmp_path):
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)

    dashboard = dashboards.save_to_folder(ugly_dashboard, tmp_path)

    counters = []
    for page in dashboard.pages:
        for layout in page.layout:
            if isinstance(layout.widget.spec, CounterSpec):
                counters.append(layout.widget)

    assert all(counter.name == "counter" for counter in counters)
    ws.assert_not_called()
