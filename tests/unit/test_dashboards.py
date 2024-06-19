import functools
import logging
from pathlib import Path
from unittest.mock import create_autospec

import pytest
import yaml
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import (
    BaseHandler,
    DashboardMetadata,
    Dashboards,
    MarkdownHandler,
    QueryHandler,
    QueryTile,
    Tile,
    WidgetMetadata,
    replace_database_in_query,
)
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
    TableV2Spec,
    Widget,
)


def test_dashboard_metadata_raises_key_error_if_display_name_is_missing():
    with pytest.raises(KeyError):
        DashboardMetadata.from_dict({})


def test_dashboard_metadata_sets_display_name_from_dict():
    dashboard_metadata = DashboardMetadata.from_dict({"display_name": "test"})
    assert dashboard_metadata.display_name == "test"


def test_dashboard_metadata_from_and_as_dict_is_a_unit_function():
    raw = {"display_name": "test"}
    dashboard_metadata = DashboardMetadata.from_dict(raw)
    assert dashboard_metadata.as_dict() == raw


def test_dashboard_metadata_from_raw(tmp_path):
    raw = {"display_name": "test"}

    path = tmp_path / "dashboard.yml"
    with path.open("w") as f:
        yaml.safe_dump(raw, f)

    from_dict = DashboardMetadata.from_dict(raw)
    from_path = DashboardMetadata.from_path(path)

    for dashboard_metadata in from_dict, from_path:
        assert dashboard_metadata.display_name == "test"


@pytest.mark.parametrize("dashboard_content", ["missing_display_name: true", "invalid:\nyml", ""])
def test_dashboard_metadata_handles_invalid_yml(tmp_path, dashboard_content):
    path = tmp_path / "dashboard.yml"
    if len(dashboard_content) > 0:
        path.write_text(dashboard_content)

    dashboard_metadata = DashboardMetadata.from_path(path)
    assert dashboard_metadata.display_name == tmp_path.name


def test_widget_metadata_is_markdown():
    widget_metadata = WidgetMetadata(Path("test.md"))
    assert widget_metadata.is_markdown()
    assert not widget_metadata.is_query()


def test_widget_metadata_is_query():
    widget_metadata = WidgetMetadata(Path("test.sql"))
    assert not widget_metadata.is_markdown()
    assert widget_metadata.is_query()


def test_base_handler_parses_empty_header(tmp_path):
    path = tmp_path / "file.txt"
    path.write_text("Hello")
    handler = BaseHandler(path)

    header = handler.parse_header()

    assert header == {}


def test_base_handler_splits_empty_header(tmp_path):
    path = tmp_path / "file.txt"
    path.write_text("Hello")
    handler = BaseHandler(path)

    header, body = handler.split()

    assert header == ""
    assert body == "Hello"


def test_query_handler_parses_empty_header(tmp_path):
    path = tmp_path / "query.sql"
    path.write_text("SELECT 1")
    handler = QueryHandler(path)

    header = handler.parse_header()

    assert all(value is None for value in header.values())


@pytest.mark.parametrize(
    "query",
    [
        "-- --height 5\nSELECT 1 AS count -- --width 6",
        "-- --height 5\nSELECT 1 AS count\n-- --width 6",
        "-- --height 5\nSELECT 1 AS count\n/* --width 6 */",
        "-- --height 5\n-- --width 6\nSELECT 1 AS count",
        "-- --height 5\n/* --width 6 */\nSELECT 1 AS count",
        "/* --height 5*/\n/* --width 6 */\nSELECT 1 AS count",
        "/* --height 5*/\n-- --width 6 */\nSELECT 1 AS count",
    ],
)
def test_query_handler_ignores_comment_on_other_lines(tmp_path, query):
    path = tmp_path / "query.sql"
    path.write_text(query)
    handler = QueryHandler(path)

    header = handler.parse_header()

    assert header["width"] is None
    assert header["height"] == 5


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1\n-- --width 6 --height 6",
        "SELECT 1\n/*\n--width 6\n--height 6*/",
    ],
)
def test_query_handler_ignores_non_header_comment(tmp_path, query):
    path = tmp_path / "query.sql"
    path.write_text(query)
    handler = QueryHandler(path)

    header = handler.parse_header()

    assert all(value is None for value in header.values())


@pytest.mark.parametrize("attribute", ["id", "order", "height", "width", "title", "description"])
def test_query_handler_parses_attribute_from_header(tmp_path, attribute):
    path = tmp_path / "query.sql"
    path.write_text(f"-- --{attribute} 10\nSELECT 1")
    handler = QueryHandler(path)

    header = handler.parse_header()

    assert str(header[attribute]) == "10"


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1",
        "-- --order 10\nSELECT COUNT(* AS invalid_column",
    ],
)
def test_query_handler_splits_no_header(tmp_path, query):
    path = tmp_path / "query.sql"
    path.write_text(query)
    handler = QueryHandler(path)

    header, content = handler.split()

    assert len(header) == 0
    assert content == query


def test_query_handler_splits_header(tmp_path):
    query = "-- --order 10\nSELECT 1"

    path = tmp_path / "query.sql"
    path.write_text(query)
    handler = QueryHandler(path)

    header, content = handler.split()

    assert header == "--order 10"
    assert content == query


def test_markdown_handler_parses_empty_header(tmp_path):
    path = tmp_path / "widget.md"
    path.write_text("# Description")
    handler = MarkdownHandler(path)

    header = handler.parse_header()

    assert header == {}


@pytest.mark.parametrize("attribute", ["id", "order", "height", "width"])
def test_markdown_handler_parses_attribute_from_header(tmp_path, attribute):
    path = tmp_path / "widget.md"
    path.write_text(f"---\n{attribute}: 10\n---\n# Description")
    handler = MarkdownHandler(path)

    header = handler.parse_header()

    assert str(header[attribute]) == "10"


@pytest.mark.parametrize("horizontal_rule", ["---", "--------"])
def test_markdown_handler_splits_header(tmp_path, caplog, horizontal_rule):
    path = tmp_path / "widget.md"
    path.write_text(f"{horizontal_rule}\norder: 10\n{horizontal_rule}\n# Description")
    handler = MarkdownHandler(path)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.lsql.dashboards"):
        header, content = handler.split()

    assert "Missing closing header boundary" not in caplog.text
    assert header == "order: 10"
    assert content == "# Description"


def test_markdown_handler_warns_about_open_ended_header(tmp_path, caplog):
    path = tmp_path / "widget.md"
    body = "---\norder: 1\n# Description"
    path.write_text(body)
    handler = MarkdownHandler(path)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.lsql.dashboards"):
        header, content = handler.split()

    assert "Missing closing header boundary." in caplog.text
    assert len(header) == 0
    assert content == body


def test_widget_metadata_replaces_width_and_height(tmp_path):
    path = tmp_path / "test.sql"
    path.write_text("SELECT 1")
    widget_metadata = WidgetMetadata(path, 1, 1, 1)
    updated_metadata = widget_metadata.from_dict(**{"path": path, "width": 10, "height": 10})
    assert updated_metadata.width == 10
    assert updated_metadata.height == 10


@pytest.mark.parametrize("attribute", ["id", "order", "width", "height", "title", "description"])
def test_widget_metadata_replaces_attribute(tmp_path, attribute: str):
    path = tmp_path / "test.sql"
    path.write_text("SELECT 1")
    widget_metadata = WidgetMetadata(path, 1, 1, 1, "1", "1", "1")
    updated_metadata = widget_metadata.from_dict(**{"path": path, attribute: "10"})
    assert str(getattr(updated_metadata, attribute)) == "10"


def test_widget_metadata_as_dict(tmp_path):
    path = tmp_path / "test.sql"
    path.write_text("SELECT 1")
    raw = {
        "path": path.as_posix(),
        "id": "test",
        "order": "-1",
        "width": "3",
        "height": "6",
        "title": "Test widget",
        "description": "Longer explanation",
    }
    widget_metadata = WidgetMetadata(
        path,
        order=-1,
        width=3,
        height=6,
        title="Test widget",
        description="Longer explanation",
    )
    assert widget_metadata.as_dict() == raw


def test_tile_places_tile_to_the_right():
    widget_metadata = WidgetMetadata(Path("test.sql"), 1, 1, 1)
    tile = Tile(widget_metadata)

    position = Position(0, 4, 3, 4)
    placed_tile = tile.place_after(position)

    assert placed_tile.position.x == position.x + position.width
    assert placed_tile.position.y == 4


def test_tile_places_tile_below():
    widget_metadata = WidgetMetadata(Path("test.sql"), 1, 1, 1)
    tile = Tile(widget_metadata)

    position = Position(5, 4, 3, 4)
    placed_tile = tile.place_after(position)

    assert placed_tile.position.x == 0
    assert placed_tile.position.y == 8


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


def test_dashboard_creates_datasets_using_query(tmp_path):
    ws = create_autospec(WorkspaceClient)

    query = "SELECT count FROM database.table"
    (tmp_path / "counter.sql").write_text(query)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    dataset = lakeview_dashboard.datasets[0]

    assert dataset.query == query
    ws.assert_not_called()


def test_dashboard_creates_datasets_with_transformed_query(tmp_path):
    ws = create_autospec(WorkspaceClient)

    # Note that sqlglot sees "$inventory" (convention in ucx) as a parameter thus only replaces "inventory"
    query = """
WITH raw AS (
  SELECT object_type, object_id, IF(failures == '[]', 1, 0) AS ready
  FROM inventory.objects
)
SELECT COALESCE(CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%'), 'N/A') AS readiness FROM raw
""".lstrip()
    (tmp_path / "counter.sql").write_text(query)

    query_transformer = functools.partial(replace_database_in_query, database="development")
    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path, query_transformer=query_transformer)

    dataset = lakeview_dashboard.datasets[0]

    assert "$inventory.objects" not in dataset.query
    assert "development.objects" in dataset.query
    ws.assert_not_called()


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


def test_dashboards_creates_text_widget_for_invalid_query(tmp_path, caplog):
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

    markdown_widget = lakeview_dashboard.pages[0].layout[1].widget
    assert markdown_widget.textbox_spec == invalid_query
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
def test_query_tile_finds_fields(tmp_path, query, names):
    query_file = tmp_path / "query.sql"
    query_file.write_text(query)

    widget_metadata = WidgetMetadata(query_file, 1, 1, 1)
    tile = QueryTile(widget_metadata)

    fields = tile._find_fields()  # pylint: disable=protected-access

    assert [field.name for field in fields] == names


def test_query_tile_keeps_original_query(tmp_path):
    query = "SELECT x, y FROM a JOIN b"
    query_path = tmp_path / "counter.sql"
    query_path.write_text(query)

    widget_metadata = WidgetMetadata.from_path(query_path)
    query_tile = QueryTile(widget_metadata)

    dataset = query_tile.get_dataset()

    assert dataset.query == query


@pytest.mark.parametrize(
    "query, query_transformed",
    [
        ("SELECT count FROM table", "SELECT count FROM table"),
        ("SELECT count FROM database.table", "SELECT count FROM development.table"),
        ("SELECT count FROM catalog.database.table", "SELECT count FROM catalog.development.table"),
        ("SELECT database FROM database.table", "SELECT database FROM development.table"),
        (
            "SELECT * FROM server.database.table, server.other_database.table",
            "SELECT * FROM server.development.table, server.development.table",
        ),
        (
            "SELECT left.* FROM server.database.table AS left JOIN server.other_database.table AS right ON left.id = right.id",
            "SELECT left.* FROM server.development.table AS left JOIN server.development.table AS right ON left.id = right.id",
        ),
    ],
)
def test_query_tile_creates_database_with_database_overwrite(tmp_path, query, query_transformed):
    query_path = tmp_path / "counter.sql"
    query_path.write_text(query)

    replace_with_development_database = functools.partial(replace_database_in_query, database="development")
    query_tile = QueryTile(WidgetMetadata.from_path(query_path), query_transformer=replace_with_development_database)

    dataset = query_tile.get_dataset()

    assert dataset.query == query_transformed


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


def test_dashboards_creates_dashboard_with_expected_table_field_encodings(tmp_path):
    with (tmp_path / "query.sql").open("w") as f:
        f.write("SELECT 1 AS first, 2 AS second")

    ws = create_autospec(WorkspaceClient)
    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    table_spec = lakeview_dashboard.pages[0].layout[0].widget.spec
    assert isinstance(table_spec, TableV2Spec)
    assert table_spec.encodings.columns[0].field_name == "first"
    assert table_spec.encodings.columns[1].field_name == "second"
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


def test_dashboards_creates_dashboards_with_widgets_order_overwrite(tmp_path):
    ws = create_autospec(WorkspaceClient)

    # Move the 'e' inbetween 'b' and 'c' query. Note that the order 1 puts 'e' on the same position as 'b', but with an
    # order tiebreaker the query name decides the final order.
    (tmp_path / "e.sql").write_text("-- --order 1\nSELECT 1 AS count")
    for query_name in "abcdf":
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    widget_names = [layout.widget.name for layout in lakeview_dashboard.pages[0].layout]

    assert "".join(widget_names) == "abecdf"
    ws.assert_not_called()


def test_dashboards_creates_dashboards_with_widgets_order_overwrite_zero(tmp_path):
    ws = create_autospec(WorkspaceClient)

    # Move the 'e' inbetween 'a' and 'b' query. Note that the order 0 puts 'e' on the same position as 'a', but with an
    # order tiebreaker the query name decides the final order.
    (tmp_path / "e.sql").write_text("-- --order 0\nSELECT 1 AS count")
    for query_name in "abcdf":
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    widget_names = [layout.widget.name for layout in lakeview_dashboard.pages[0].layout]

    assert "".join(widget_names) == "aebcdf"
    ws.assert_not_called()


def test_dashboards_creates_dashboards_with_widget_ordered_using_id(tmp_path):
    ws = create_autospec(WorkspaceClient)

    for query_name in "bcdef":
        with (tmp_path / f"{query_name}.sql").open("w") as f:
            f.write("SELECT 1 AS count")

    with (tmp_path / "z.sql").open("w") as f:
        f.write("-- --id a\nSELECT 1 AS count")  # Should be first because id is 'a'

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    widget_names = [layout.widget.name for layout in lakeview_dashboard.pages[0].layout]

    assert "".join(widget_names) == "abcdef"
    ws.assert_not_called()


@pytest.mark.parametrize(
    "query, width, height",
    [
        ("SELECT 1 AS count", 1, 3),
        ("SELECT 1 AS first, 2 AS second", 6, 6),
    ],
)
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
    assert position.height == 3
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


@pytest.mark.parametrize(
    "header",
    [
        "-- --width 6 --height 3",
        "/* --width 6 --height 3 */",
        "/*\n--width 6\n--height 3 */",
    ],
)
def test_dashboard_creates_dashboard_with_custom_sized_widget(tmp_path, header):
    ws = create_autospec(WorkspaceClient)

    query = f"{header}\nSELECT 82917019218921 AS big_number_needs_big_widget"
    with (tmp_path / "counter.sql").open("w") as f:
        f.write(query)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)
    position = lakeview_dashboard.pages[0].layout[0].position

    assert position.width == 6
    assert position.height == 3
    ws.assert_not_called()


def test_dashboard_creates_dashboard_with_title(tmp_path):
    ws = create_autospec(WorkspaceClient)

    query = "-- --title 'Count me in'\nSELECT 2918"
    (tmp_path / "counter.sql").write_text(query)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    frame = lakeview_dashboard.pages[0].layout[0].widget.spec.frame
    assert frame.title == "Count me in"
    assert frame.show_title
    ws.assert_not_called()


def test_dashboard_creates_dashboard_with_description(tmp_path):
    ws = create_autospec(WorkspaceClient)

    query = "-- --description 'Only when it counts'\nSELECT 2918"
    (tmp_path / "counter.sql").write_text(query)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    frame = lakeview_dashboard.pages[0].layout[0].widget.spec.frame
    assert frame.description == "Only when it counts"
    assert frame.show_description
    ws.assert_not_called()


def test_dashboard_handles_incorrect_query_header(tmp_path, caplog):
    ws = create_autospec(WorkspaceClient)

    # Typo is on purpose
    query = "-- --widh 6 --height 3 \nSELECT 82917019218921 AS big_number_needs_big_widget"
    query_path = tmp_path / "counter.sql"
    query_path.write_text(query)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.lsql.dashboards"):
        lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    position = lakeview_dashboard.pages[0].layout[0].position
    assert position.width == 1
    assert position.height == 3
    assert query_path.as_posix() in caplog.text
    ws.assert_not_called()


def test_dashboard_creates_dashboard_based_on_markdown_header(tmp_path):
    ws = create_autospec(WorkspaceClient)

    for query_name in "abcdef":
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")
    content = "---\norder: -1\nwidth: 6\nheight: 3\n---\n# Description"
    (tmp_path / "widget.md").write_text(content)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    position = lakeview_dashboard.pages[0].layout[0].position
    assert position.width == 6
    assert position.height == 3
    ws.assert_not_called()


def test_dashboard_uses_metadata_above_select_when_query_has_cte(tmp_path):
    ws = create_autospec(WorkspaceClient)

    query = (
        "WITH data AS (SELECT 1 AS count)\n"
        "-- --width 6 --height 6\n"
        "SELECT count FROM data"
    )
    (tmp_path / "widget.sql").write_text(query)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    position = lakeview_dashboard.pages[0].layout[0].position
    assert position.width == 6
    assert position.height == 6
    ws.assert_not_called()


def test_dashboard_ignores_first_line_metadata_when_query_has_cte(tmp_path):
    ws = create_autospec(WorkspaceClient)

    query = (
        "-- --width 6 --height 6\n"
        "WITH data AS (SELECT 1 AS count)\n"
        "SELECT count FROM data"
    )
    (tmp_path / "widget.sql").write_text(query)

    lakeview_dashboard = Dashboards(ws).create_dashboard(tmp_path)

    position = lakeview_dashboard.pages[0].layout[0].position
    assert position.width != 6
    assert position.height != 6
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
