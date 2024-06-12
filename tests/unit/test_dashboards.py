import logging
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards
from databricks.labs.lsql.lakeview import (
    CounterEncodingMap,
    CounterSpec,
    Dashboard,
    Dataset,
    Layout,
    NamedQuery,
    Page,
    Position,
    Query,
    Widget,
)


def test_dashboard_configuration_raises_key_error_if_display_name_is_missing():
    with pytest.raises(KeyError):
        DashboardMetadata.from_dict({})


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


def test_dashboards_creates_dashboard_with_first_page_name_after_folder():
    ws = create_autospec(WorkspaceClient)
    queries = Path(__file__).parent / "queries"
    lakeview_dashboard = Dashboards(ws).create_dashboard(queries)
    page = lakeview_dashboard.pages[0]
    assert page.name == "queries"
    assert page.display_name == "queries"


def test_dashboards_creates_dashboard_with_custom_first_page_name(tmp_path):
    with (tmp_path / "dashboard.yml").open("w") as f:
        f.write("display_name: Custom")

    ws = create_autospec(WorkspaceClient)
    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    page = lakeview_dashboard.pages[0]
    assert page.name == "Custom"
    assert page.display_name == "Custom"


@pytest.mark.parametrize("dashboard_content", ["missing_display_name: true", "invalid:\nyml"])
def test_dashboards_handles_invalid_dashboard_yml(tmp_path, dashboard_content):
    queries_path = tmp_path / "queries"
    queries_path.mkdir()
    with (queries_path / "dashboard.yml").open("w") as f:
        f.write(dashboard_content)

    ws = create_autospec(WorkspaceClient)
    lakeview_dashboard = Dashboards(ws).create_dashboard(queries_path)

    page = lakeview_dashboard.pages[0]
    assert page.name == "queries"
    assert page.display_name == "queries"


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


def test_dashboards_skips_invalid_query(tmp_path, caplog):
    ws = create_autospec(WorkspaceClient)

    # Test for the invalid query not to be the first or last query
    for i in range(0, 3, 2):
        with (tmp_path / f"{i}_counter.sql").open("w") as f:
            f.write(f"SELECT {i} AS count")

    invalid_query = "SELECT COUNT(* AS missing_closing_parenthesis"
    with (tmp_path / "1_invalid.sql").open("w") as f:
        f.write(invalid_query)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.lsql.dashboards"):
        lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    assert len(lakeview_dashboard.pages[0].layout) == 2
    assert invalid_query in caplog.text


def test_dashboards_does_not_create_widget_for_yml_file(tmp_path, caplog):
    ws = create_autospec(WorkspaceClient)

    with (tmp_path / "dashboard.yml").open("w") as f:
        f.write("display_name: Git based dashboard")

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    assert len(lakeview_dashboard.pages[0].layout) == 0


@pytest.mark.parametrize(
    "query, names",
    [
        ("SELECT 1", ["1"]),
        ("SELECT 1 AS foo", ["foo"]),
        ("SELECT 'a'", ["a"]),
        ("SELECT 1, 'a', 100 * 20 AS calc", ["1", "a", "calc"]),
        ("SELECT first, second, third FROM table", ["first", "second", "third"]),
        ("SELECT a.first, a.second, b.third FROM table AS a JOIN another_table AS b", ["first", "second", "third"]),
        ("SELECT first, 1 AS second, 'third' FROM table", ["first", "second", "third"]),
        ("SELECT f AS first, s as second, 100 * 20 AS third FROM table", ["first", "second", "third"]),
        ("SELECT first FROM (SELECT first, second FROM table)", ["first"]),
        ("SELECT COUNT(DISTINCT `database`) AS count_total_databases FROM table", ["count_total_databases"]),
        (
            """
WITH raw AS (
  SELECT object_type, object_id, IF(failures == '[]', 1, 0) AS ready 
  FROM $inventory.objects
)
SELECT CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%') AS readiness FROM raw
            """,
            ["readiness"],
        ),
        (
            """
SELECT storage, COUNT(*) count
FROM (
SELECT
       CASE
           WHEN STARTSWITH(location, "dbfs:/mnt") THEN "DBFS MOUNT"
           WHEN STARTSWITH(location, "/dbfs/mnt") THEN "DBFS MOUNT"
           WHEN STARTSWITH(location, "dbfs:/databricks-datasets") THEN "Databricks Demo Dataset"
           WHEN STARTSWITH(location, "/dbfs/databricks-datasets") THEN "Databricks Demo Dataset"
           WHEN STARTSWITH(location, "dbfs:/") THEN "DBFS ROOT"
           WHEN STARTSWITH(location, "/dbfs/") THEN "DBFS ROOT"
           WHEN STARTSWITH(location, "wasb") THEN "UNSUPPORTED"
           WHEN STARTSWITH(location, "adl") THEN "UNSUPPORTED"
           ELSE "EXTERNAL"
       END AS storage
FROM $inventory.tables)
GROUP BY storage
ORDER BY storage;
            """,
            ["storage", "count"],
        ),
        (
            """
WITH raw AS (
  SELECT EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding
  FROM $inventory.objects WHERE failures <> '[]'
)
SELECT finding as `finding`, COUNT(*) AS count 
FROM raw 
GROUP BY finding
ORDER BY count DESC, finding DESC
            """,
            ["finding", "count"],
        ),
        ("SELECT CONCAT(tables.`database`, '.', tables.name) AS name FROM table", ["name"]),
        ('SELECT IF(object_type IN ("MANAGED", "EXTERNAL"), 1, 0) AS is_table FROM table', ["is_table"]),
        ("SELECT DISTINCT policy_name FROM table", ["policy_name"]),
        ("SELECT COLLECT_LIST(DISTINCT run_ids) AS run_ids FROM table", ["run_ids"]),
        ("SELECT substring(component, length('databricks.labs.') + 1) AS component FROM table", ["component"]),
        ("SELECT from_unixtime(timestamp) AS timestamp FROM table", ["timestamp"]),
    ],
)
def test_dashboards_gets_fields_with_expected_names(tmp_path, query, names):
    with (tmp_path / "query.sql").open("w") as f:
        f.write(query)

    ws = create_autospec(WorkspaceClient)
    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    fields = lakeview_dashboard.pages[0].layout[0].widget.queries[0].query.fields
    assert [field.name for field in fields] == names
    ws.assert_not_called()


def test_dashboards_creates_dashboard_with_expected_counter_field_encoding_names(tmp_path):
    with (tmp_path / "query.sql").open("w") as f:
        f.write("SELECT 1 AS amount")

    ws = create_autospec(WorkspaceClient)
    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    counter_spec = lakeview_dashboard.pages[0].layout[0].widget.spec
    assert isinstance(counter_spec, CounterSpec)
    assert counter_spec.encodings.value.field_name == "amount"
    assert counter_spec.encodings.value.display_name == "amount"
    ws.assert_not_called()


def test_dashboards_creates_dashboards_with_second_widget_to_the_right_of_the_first_widget(tmp_path):
    ws = create_autospec(WorkspaceClient)

    for i in range(2):
        with (tmp_path / f"counter_{i}.sql").open("w") as f:
            f.write(f"SELECT {i} AS count")

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    layout = lakeview_dashboard.pages[0].layout
    first_position, second_position = layout[0].position, layout[1].position

    assert first_position.x < second_position.x
    assert first_position.y == second_position.y
    ws.assert_not_called()


def test_dashboards_creates_dashboard_with_many_widgets_not_on_the_first_row(tmp_path):
    ws = create_autospec(WorkspaceClient)
    for i in range(10):
        with (tmp_path / f"counter_{i}.sql").open("w") as f:
            f.write(f"SELECT {i} AS count")

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    layout = lakeview_dashboard.pages[0].layout

    assert layout[-1].position.y > 0
    ws.assert_not_called()


def test_dashboards_creates_dashboard_with_widget_below_text_widget(tmp_path):
    ws = create_autospec(WorkspaceClient)
    with (tmp_path / "000_counter.md").open("w") as f:
        f.write("# Description")
    with (tmp_path / "010_counter.sql").open("w") as f:
        f.write("SELECT 100 AS count")

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    layout = lakeview_dashboard.pages[0].layout

    assert len(layout) == 2
    assert layout[0].position.y < layout[1].position.y
    ws.assert_not_called()


@pytest.mark.parametrize("query_names", [["a", "b", "c"], ["01", "02", "10"]])
def test_dashboards_creates_dashboards_with_widgets_sorted_alphanumerically(tmp_path, query_names):
    ws = create_autospec(WorkspaceClient)

    for query_name in query_names:
        with (tmp_path / f"{query_name}.sql").open("w") as f:
            f.write("SELECT 1 AS count")

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    widget_names = [layout.widget.name for layout in lakeview_dashboard.pages[0].layout]

    assert widget_names == query_names
    ws.assert_not_called()


@pytest.mark.parametrize("query, width, height", [("SELECT 1 AS count", 1, 3)])
def test_dashboards_creates_dashboards_where_widget_has_expected_width_and_height(tmp_path, query, width, height):
    ws = create_autospec(WorkspaceClient)

    with (tmp_path / "query.sql").open("w") as f:
        f.write(query)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    position = lakeview_dashboard.pages[0].layout[0].position

    assert position.width == width
    assert position.height == height
    ws.assert_not_called()


def test_dashboards_creates_dashboards_where_text_widget_has_expected_width_and_height(tmp_path):
    ws = create_autospec(WorkspaceClient)

    with (tmp_path / "description.md").open("w") as f:
        f.write("# Description")

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    position = lakeview_dashboard.pages[0].layout[0].position

    assert position.width == 6
    assert position.height == 2
    ws.assert_not_called()


def test_dashboards_creates_dashboards_where_text_widget_has_expected_text(tmp_path):
    ws = create_autospec(WorkspaceClient)

    content = "# Description"
    with (tmp_path / "description.md").open("w") as f:
        f.write(content)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    widget = lakeview_dashboard.pages[0].layout[0].widget

    assert widget.textbox_spec == content
    ws.assert_not_called()


def test_dashboards_deploy_calls_create_without_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)

    dashboard = Dashboard([], [Page("test", [])])
    dashboards.deploy_dashboard(dashboard)

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
