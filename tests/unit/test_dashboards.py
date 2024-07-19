import itertools
import json
import logging
import string
from pathlib import Path
from unittest.mock import create_autospec

import pytest
import sqlglot
import yaml
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import (
    BaseHandler,
    DashboardMetadata,
    Dashboards,
    MarkdownHandler,
    MarkdownTile,
    QueryHandler,
    QueryTile,
    Tile,
    TileMetadata,
    WidgetType,
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
    TableV1Spec,
    Widget,
)


def test_dashboard_metadata_raises_key_error_if_display_name_is_missing():
    with pytest.raises(KeyError):
        DashboardMetadata.from_dict({})


def test_dashboard_metadata_sets_display_name_from_dict():
    dashboard_metadata = DashboardMetadata.from_dict({"display_name": "test"})
    assert dashboard_metadata.display_name == "test"


def test_dashboard_metadata_from_path_raises_not_implemented_error_for_select_star(tmp_path):
    (tmp_path / "star.sql").write_text("SELECT * FROM table")
    with pytest.raises(NotImplementedError) as e:
        DashboardMetadata.from_path(tmp_path)
    assert "star" in str(e)


def test_dashboard_metadata_sets_tiles_from_dict(tmp_path):
    query_path = tmp_path / "test.sql"
    query_path.touch()
    tile_metadata = TileMetadata.from_path(query_path)
    raw = {"display_name": "test", "tiles": {"test": {"path": query_path.as_posix()}}}
    dashboard_metadata = DashboardMetadata.from_dict(raw)
    assert len(dashboard_metadata.tiles) == 1
    assert dashboard_metadata.tiles[0].metadata == tile_metadata


def test_dashboard_metadata_ignores_id_overwrite(caplog):
    raw = {"display_name": "test", "tiles": {"test": {"id": "not_test"}}}

    with caplog.at_level(logging.WARNING, logger="databricks.labs.lsql.dashboards"):
        dashboard_metadata = DashboardMetadata.from_dict(raw)

    assert len(dashboard_metadata.tiles) == 1
    assert dashboard_metadata.tiles[0].metadata.id == "test"
    assert "Parsing unsupported field in dashboard.yml: tiles.test.id" in caplog.text


def test_dashboard_metadata_from_and_as_dict_is_a_unit_function(tmp_path):
    query_path = tmp_path / "test.sql"
    query_path.touch()
    raw_tile = {"path": query_path.as_posix(), "id": "test", "height": 0, "width": 0, "widget_type": "AUTO"}
    raw = {"display_name": "test", "tiles": {"test": raw_tile}}
    dashboard_metadata = DashboardMetadata.from_dict(raw)
    assert dashboard_metadata.as_dict() == raw


def test_dashboard_metadata_from_raw(tmp_path):
    query_path = tmp_path / "test.sql"
    query_path.touch()
    raw_tile = {"path": query_path.as_posix(), "id": "test", "height": 0, "width": 0, "widget_type": "AUTO", "order": 0}
    raw = {"display_name": "test", "tiles": {"test": raw_tile}}

    path = tmp_path / "dashboard.yml"
    with path.open("w") as f:
        yaml.safe_dump(raw, f)

    from_dict = DashboardMetadata.from_dict(raw)
    from_path = DashboardMetadata.from_path(tmp_path)

    assert from_dict == from_path


@pytest.mark.parametrize(
    "dashboard_content",
    [
        "missing_display_name: true",
        "invalid:\nyml",
        "",
    ],
)
def test_dashboard_metadata_handles_invalid_yml(tmp_path, dashboard_content):
    path = tmp_path / "dashboard.yml"
    if len(dashboard_content) > 0:
        path.write_text(dashboard_content)

    dashboard_metadata = DashboardMetadata.from_path(tmp_path)
    assert dashboard_metadata.display_name == tmp_path.name


def test_dashboard_metadata_handles_partial_invalid_yml(tmp_path, caplog):
    dashboard_content = """
display_name: name

tiles:
  correct:
    order: 1
  incorrect:
  - order: 2
  partial_correct:
    order: 3
    non_existing_key: value 
""".lstrip()
    path = tmp_path / "dashboard.yml"
    path.write_text(dashboard_content)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.lsql.dashboards"):
        dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    assert dashboard_metadata.display_name == "name"
    assert len(dashboard_metadata.tiles) == 2
    assert dashboard_metadata.tiles[0].metadata.id == "correct"
    assert dashboard_metadata.tiles[0].metadata.order == 1
    assert dashboard_metadata.tiles[1].metadata.id == "partial_correct"
    assert dashboard_metadata.tiles[1].metadata.order == 3
    assert "Parsing invalid tile metadata in dashboard.yml: tiles.incorrect.[{'order': 2}]" in caplog.text
    assert "Parsing unsupported field in dashboard.yml: tiles.partial_correct.non_existing_key" in caplog.text


def test_dashboard_metadata_validate_valid(tmp_path):
    dashboard_content = """
display_name: name

tiles:
  correct:
    order: 1
""".lstrip()
    (tmp_path / "dashboard.yml").write_text(dashboard_content)
    (tmp_path / "correct.sql").write_text("SELECT 1")

    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    try:
        dashboard_metadata.validate()
    except ValueError as e:
        assert False, f"Invalid dashboard metadata: {e}"
    else:
        assert True, "Valid dashboard metadata"


def test_dashboard_metadata_validate_misses_tile_content(tmp_path):
    dashboard_content = """
display_name: name

tiles:
  correct:
    order: 1
""".lstrip()
    (tmp_path / "dashboard.yml").write_text(dashboard_content)

    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    with pytest.raises(ValueError) as e:
        dashboard_metadata.validate()
    assert "Tile has empty content:" in str(e.value)


def test_dashboard_metadata_validate_finds_duplicate_query_id(tmp_path):
    (tmp_path / "query.sql").write_text("SELECT 1")
    query_content = """-- --id query\nSELECT 1"""
    (tmp_path / "not_query.sql").write_text(query_content)

    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    with pytest.raises(ValueError) as e:
        dashboard_metadata.validate()
    assert "Duplicate id: query" in str(e.value)


def test_dashboard_metadata_validate_finds_duplicate_widget_id(tmp_path):
    (tmp_path / "widget.sql").write_text("SELECT 1")
    (tmp_path / "widget.md").write_text("# Widget")

    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    with pytest.raises(ValueError) as e:
        dashboard_metadata.validate()
    assert "Duplicate id: widget" in str(e.value)


def test_tile_metadata_is_markdown():
    tile_metadata = TileMetadata(Path("test.md"))
    assert tile_metadata.is_markdown()
    assert not tile_metadata.is_query()


def test_tile_metadata_is_query():
    tile_metadata = TileMetadata(Path("test.sql"))
    assert not tile_metadata.is_markdown()
    assert tile_metadata.is_query()


def test_tile_metadata_merges():
    left = TileMetadata(Path("left.sql"), filters=["a"], width=10, widget_type=WidgetType.TABLE)
    right = TileMetadata(Path("right.sql"), widget_type=WidgetType.COUNTER)
    merged = left.merge(right)
    assert merged.id == "right"
    assert merged.width == 10
    assert merged.filters == ["a"]
    assert merged.widget_type == WidgetType.COUNTER


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

    has_default = {"widget_type"}
    assert all(not value for key, value in header.items() if key not in has_default)


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

    assert header["width"] == 0
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

    has_default = {"widget_type"}
    assert all(not value for key, value in header.items() if key not in has_default)


@pytest.mark.parametrize("attribute", ["id", "order", "height", "width", "title", "description", "overrides"])
def test_query_handler_parses_attribute_from_header(tmp_path, attribute):
    path = tmp_path / "query.sql"
    path.write_text(f"-- --{attribute} 10\nSELECT 1")
    handler = QueryHandler(path)

    header = handler.parse_header()

    assert str(header[attribute]) == "10"


def test_query_handler_parses_type_attribute_from_header(tmp_path):
    path = tmp_path / "query.sql"
    path.write_text("-- --type COUNTER\nSELECT 1")
    handler = QueryHandler(path)

    header = handler.parse_header()

    assert header["widget_type"] == "COUNTER"


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


def test_widget_type_raises_value_error_when_converting_auto_to_widget_spec():
    with pytest.raises(ValueError):
        WidgetType.AUTO.as_widget_spec()


def test_widget_type_converts_all_to_widget_spec_except_auto():
    for widget_type in WidgetType:
        if widget_type == WidgetType.AUTO:
            continue
        try:
            widget_type.as_widget_spec()
        except ValueError as e:
            assert False, e


def test_tile_metadata_replaces_width_and_height(tmp_path):
    path = tmp_path / "test.sql"
    path.write_text("SELECT 1")
    tile_metadata = TileMetadata(path, 1, 1, 1)
    updated_metadata = tile_metadata.from_dict({"width": 10, "height": 10})
    assert updated_metadata.width == 10
    assert updated_metadata.height == 10


@pytest.mark.parametrize("attribute", ["id", "order", "width", "height", "title", "description", "widget_type"])
def test_tile_metadata_replaces_attribute(tmp_path, attribute: str):
    path = tmp_path / "test.sql"
    path.write_text("SELECT 1")
    tile_metadata = TileMetadata(
        path,
        order=1,
        width=1,
        height=1,
        id="1",
        title="1",
        description="1",
        widget_type=WidgetType.AUTO,
        overrides={"spec": {"frame": {"showTitle": True}}},
    )
    updated_metadata = tile_metadata.from_dict({attribute: "10"})
    assert str(getattr(updated_metadata, attribute)) == "10"


def test_tile_metadata_replaces_filters(tmp_path):
    path = tmp_path / "test.sql"
    path.write_text("SELECT 1")
    tile_metadata = TileMetadata(
        path,
        filters=[
            "column",
        ],
    )
    updated_metadata = tile_metadata.from_dict({"filters": ["a", "b", "c"]})
    assert updated_metadata.filters == ["a", "b", "c"]


def test_tile_metadata_as_dict(tmp_path):
    path = tmp_path / "test.sql"
    path.write_text("SELECT 1")
    raw = {
        "path": path.as_posix(),
        "id": "test",
        "order": -1,
        "width": 3,
        "height": 6,
        "title": "Test widget",
        "description": "Longer explanation",
        "widget_type": "AUTO",
        "filters": ["column"],
        "overrides": {"spec": {"frame": {"showTitle": True}}},
    }
    tile_metadata = TileMetadata(
        path,
        order=-1,
        width=3,
        height=6,
        title="Test widget",
        description="Longer explanation",
        widget_type=WidgetType.AUTO,
        filters=["column"],
        overrides={"spec": {"frame": {"showTitle": True}}},
    )
    assert tile_metadata.as_dict() == raw


@pytest.mark.parametrize("tile_class", [Tile, QueryTile])
def test_tile_validate_raises_value_error_when_content_is_empty(tmp_path, tile_class):
    tile_metadata_path = tmp_path / "test.sql"
    tile_metadata_path.touch()
    tile = tile_class(TileMetadata(tile_metadata_path))

    with pytest.raises(ValueError):
        tile.validate()


def test_markdown_tile_validate_raises_value_error_when_not_from_markdown_file(tmp_path):
    tile_metadata_path = tmp_path / "test.sql"
    tile_metadata_path.write_text("# Markdown")
    tile = MarkdownTile(TileMetadata(tile_metadata_path))

    with pytest.raises(ValueError):
        tile.validate()


def test_query_tile_validate_raises_value_error_when_not_from_query_file(tmp_path):
    tile_metadata_path = tmp_path / "test.md"
    tile_metadata_path.write_text("SELECT 1")
    tile = QueryTile(TileMetadata(tile_metadata_path))

    with pytest.raises(ValueError):
        tile.validate()


def test_query_tile_validate_raises_value_error_when_query_is_incorrect(tmp_path):
    tile_metadata_path = tmp_path / "test.sql"
    tile_metadata_path.write_text("SELECT COUNT(* FROM table")  # Missing closing parenthesis on purpose
    tile = QueryTile(TileMetadata(tile_metadata_path))

    with pytest.raises(ValueError):
        tile.validate()


def test_tile_places_tile_to_the_right():
    tile_metadata = TileMetadata(Path("test.sql"), 1, 1, 1)
    tile = Tile(tile_metadata)

    position = Position(0, 4, 3, 4)
    tile.place_after(position)

    assert tile.position.x == position.x + position.width
    assert tile.position.y == 4


def test_tile_places_tile_below():
    tile_metadata = TileMetadata(Path("test.sql"), 1, 1, 1)
    tile = Tile(tile_metadata)

    position = Position(5, 4, 3, 4)
    tile.place_after(position)

    assert tile.position.x == 0
    assert tile.position.y == 8


def test_dashboard_metadata_as_lakeview_with_folder_name_as_first_page_name():
    dashboard_metadata = DashboardMetadata.from_path(Path(__file__).parent / "queries")
    dashboard = dashboard_metadata.as_lakeview()
    page = dashboard.pages[0]
    assert page.name == "queries"
    assert page.display_name == "queries"


def test_dashboard_metadata_as_lakeview_with_custom_first_page_name(tmp_path):
    (tmp_path / "dashboard.yml").write_text("display_name: Custom")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    page = dashboard.pages[0]
    assert page.name == "Custom"
    assert page.display_name == "Custom"


@pytest.mark.parametrize("dashboard_content", ["missing_display_name: true", "invalid:\nyml"])
def test_dashboard_metadata_handles_invalid_dashboard_yml(tmp_path, dashboard_content):
    queries_path = tmp_path / "queries"
    queries_path.mkdir()
    (queries_path / "dashboard.yml").write_text(dashboard_content)
    dashboard_metadata = DashboardMetadata.from_path(queries_path)

    dashboard = dashboard_metadata.as_lakeview()

    page = dashboard.pages[0]
    assert page.name == "queries"
    assert page.display_name == "queries"


def test_dashboard_metadata_creates_one_dataset_per_query():
    queries = Path(__file__).parent / "queries"
    dashboard_metadata = DashboardMetadata.from_path(queries)
    dashboard = dashboard_metadata.as_lakeview()
    assert len(dashboard.datasets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_metadata_creates_datasets_using_query(tmp_path):
    query = "SELECT count FROM database.table"
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)
    dashboard = dashboard_metadata.as_lakeview()
    assert dashboard.datasets[0].query == query


def test_dashboard_metadata_creates_datasets_with_database_replaced(tmp_path):
    # Note that sqlglot sees "$inventory" (convention in ucx) as a parameter thus only replaces "inventory"
    query = """
WITH raw AS (
  SELECT object_type, object_id, IF(failures == '[]', 1, 0) AS ready
  FROM inventory.objects
)
SELECT COALESCE(CONCAT(ROUND(SUM(ready) / COUNT(*) * 100, 1), '%'), 'N/A') AS readiness FROM raw
""".lstrip()
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path).replace_database(database="development")

    dashboard = dashboard_metadata.as_lakeview()

    dataset = dashboard.datasets[0]
    assert "$inventory.objects" not in dataset.query
    assert "development.objects" in dataset.query
    assert len(dataset.query.split("\n")) != 1  # Without formatting the query transformer results a single line


def test_dashboard_metadata_creates_one_counter_widget_per_single_column_query():
    queries = Path(__file__).parent / "queries"
    dashboard_metadata = DashboardMetadata.from_path(queries)

    dashboard = dashboard_metadata.as_lakeview()

    counter_widgets = []
    for page in dashboard.pages:
        for layout in page.layout:
            if isinstance(layout.widget.spec, CounterSpec):
                counter_widgets.append(layout.widget)
    assert len(counter_widgets) == len([query for query in queries.glob("*.sql")])


def test_dashboard_metadata_creates_text_widget_for_invalid_query(tmp_path, caplog):
    # Test for the invalid query not to be the first or last query
    for i in range(0, 3, 2):
        (tmp_path / f"{i}_counter.sql").write_text(f"SELECT {i} AS count")
    invalid_query = "SELECT COUNT(* AS missing_closing_parenthesis"
    (tmp_path / "1_invalid.sql").write_text(invalid_query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.lsql.dashboards"):
        dashboard = dashboard_metadata.as_lakeview()

    markdown_widget = dashboard.pages[0].layout[1].widget
    assert markdown_widget.textbox_spec == invalid_query
    assert invalid_query in caplog.text


def test_dashboard_metadata_does_not_create_widget_for_yml_file(tmp_path, caplog):
    (tmp_path / "dashboard.yml").write_text("display_name: Git based dashboard")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)
    dashboard = dashboard_metadata.as_lakeview()
    assert len(dashboard.pages[0].layout) == 0


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

    tile_metadata = TileMetadata(query_file, 1, 1, 1)
    tile = QueryTile.from_tile_metadata(tile_metadata)

    fields = tile._find_fields()  # pylint: disable=protected-access

    assert [field.name for field in fields] == names


def test_query_tile_keeps_original_query(tmp_path):
    query = "SELECT x, y FROM a JOIN b"
    query_path = tmp_path / "counter.sql"
    query_path.write_text(query)

    tile_metadata = TileMetadata.from_path(query_path)
    query_tile = QueryTile.from_tile_metadata(tile_metadata)

    dataset = next(query_tile.get_datasets())

    assert dataset.query == query


@pytest.mark.parametrize(
    "query, query_transformed, catalog_to_replace, database_to_replace",
    [
        ("SELECT count FROM table", "SELECT count FROM table", None, None),
        ("SELECT count FROM database.table", "SELECT count FROM development.table", None, None),
        ("SELECT count FROM catalog.database.table", "SELECT count FROM catalog.development.table", None, None),
        ("SELECT database FROM database.table", "SELECT database FROM development.table", None, None),
        (
            "SELECT a FROM server.database.table, remote_server.other_database.table",
            "SELECT a FROM catalog.development.table, remote_server.development.table",
            "server",
            None,
        ),
        (
            "SELECT left.a FROM hive_metastore.database.table AS left JOIN hive_metastore.other_database.table AS right ON left.id = right.id",
            "SELECT left.a FROM catalog.development.table AS left JOIN catalog.development.table AS right ON left.id = right.id",
            None,
            None,
        ),
        (
            "SELECT left.name FROM database.table AS left JOIN other_database.table AS right ON left.id = right.id",
            "SELECT left.name FROM development.table AS left JOIN other_database.table AS right ON left.id = right.id",
            None,
            "database",
        ),
    ],
)
def test_query_tile_creates_database_with_database_overwrite(
    tmp_path,
    query,
    query_transformed,
    catalog_to_replace,
    database_to_replace,
):
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path).replace_database(
        "catalog",
        "development",
        catalog_to_replace=catalog_to_replace,
        database_to_replace=database_to_replace,
    )

    dashboard = dashboard_metadata.as_lakeview()

    datasets = dashboard.datasets
    assert len(datasets) == 1
    assert datasets[0].query == sqlglot.parse_one(query_transformed).sql(pretty=True)


@pytest.mark.parametrize("width", [5, 8, 13])
@pytest.mark.parametrize("height", [4, 8, 12])
@pytest.mark.parametrize("filters", ["", "a", "ab", "abc", "abcde", "abcdefgh"])
@pytest.mark.parametrize("axes", ["xy", "yx"])
def test_query_tile_fills_up_size(tmp_path, width, height, filters, axes):
    query_path = tmp_path / "counter.sql"
    query_path.write_text("SELECT 1")

    widget_metadata = TileMetadata(query_path, width=width, height=height, filters=list(filters))
    query_tile = QueryTile(widget_metadata)

    positions = [layout.position for layout in query_tile.get_layouts()]

    assert sum(p.width * p.height for p in positions) == width * height

    # On every row/column the positions should line up without (negative) gaps
    axis, group_axis = axes[0], axes[1]
    dimension = "width" if axis == "x" else "height"
    positions_sorted = sorted(positions, key=lambda p: (getattr(p, group_axis), getattr(p, axis)))
    for _, g in itertools.groupby(positions_sorted, lambda p: getattr(p, group_axis)):
        group = list(g)
        for before, after in zip(group[:-1], group[1:]):
            message = f"Gap between positions: {before} -> {after}"
            assert getattr(before, axis) + getattr(before, dimension) == getattr(after, axis), message


def test_table_tile_becomes_wider_with_more_columns(tmp_path):
    query = "SELECT col0, col1"
    query_path = tmp_path / "small.sql"
    query_path.write_text(query)
    small_table = Tile.from_tile_metadata(TileMetadata.from_path(query_path))

    query = "SELECT " + ", ".join(f"col{i}" for i in range(100))
    query_path = tmp_path / "big.sql"
    query_path.write_text(query)
    big_table = Tile.from_tile_metadata(TileMetadata.from_path(query_path))

    assert small_table.position.width < big_table.position.width


def test_dashboard_metadata_creates_counter_with_field_encoding_names(tmp_path):
    (tmp_path / "query.sql").write_text("SELECT 1 AS amount")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    counter_spec = dashboard.pages[0].layout[0].widget.spec
    assert isinstance(counter_spec, CounterSpec)
    assert counter_spec.encodings.value.field_name == "amount"
    assert counter_spec.encodings.value.display_name == "amount"


@pytest.mark.parametrize(
    "query, spec_expected",
    [
        ("SELECT 1", CounterSpec),
        ("SELECT 1, 2", TableV1Spec),
        ("-- --type auto\nSELECT 1, 2", TableV1Spec),
        ("-- --type counter\nSELECT 1, 2", CounterSpec),
    ],
)
def test_dashboard_metadata_infers_query_spec(tmp_path, query, spec_expected):
    (tmp_path / "query.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)
    dashboard = dashboard_metadata.as_lakeview()
    spec = dashboard.pages[0].layout[0].widget.spec
    assert isinstance(spec, spec_expected)


def test_dashboard_metadata_overrides_show_empty_title_in_query_header(tmp_path):
    query = '-- --overrides \'{"spec": {"frame": {"showTitle": true}}}\'\nSELECT 102132 AS count'
    (tmp_path / "query.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    frame = dashboard.pages[0].layout[0].widget.spec.frame
    assert frame.show_title
    assert len(frame.title) == 0


def test_dashboard_metadata_overrides_show_empty_title_from_dashboard_yml(tmp_path):
    dashboard_content = """
display_name: Show empty title

tiles:
  query:
    overrides:
      spec:
        frame:
          showTitle: true
    """.strip()
    (tmp_path / "dashboard.yml").write_text(dashboard_content)
    (tmp_path / "query.sql").write_text("SELECT 20")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    frame = dashboard.pages[0].layout[0].widget.spec.frame
    assert frame.show_title
    assert len(frame.title) == 0


def test_dashboard_metadata_creates_table_with_field_encodings(tmp_path):
    (tmp_path / "query.sql").write_text("select 1 as first, 2 as second")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    table_spec = dashboard.pages[0].layout[0].widget.spec
    assert isinstance(table_spec, TableV1Spec)
    assert table_spec.encodings.columns[0].field_name == "first"
    assert table_spec.encodings.columns[1].field_name == "second"


def test_dashboard_metadata_places_second_widget_to_the_right_of_the_first_widget(tmp_path):
    for i in range(2):
        (tmp_path / f"counter_{i}.sql").write_text(f"SELECT {i} AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    layout = dashboard.pages[0].layout
    first_position, second_position = layout[0].position, layout[1].position
    assert first_position.x < second_position.x
    assert first_position.y == second_position.y


def test_dashboard_metadata_places_many_widgets_not_on_the_first_row(tmp_path):
    for i in range(10):
        (tmp_path / f"counter_{i}.sql").write_text(f"SELECT {i} AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)
    dashboard = dashboard_metadata.as_lakeview()
    assert dashboard.pages[0].layout[-1].position.y > 0


def test_dashboard_metadata_places_widget_below_markdown_widget(tmp_path):
    (tmp_path / "000_counter.md").write_text("# Description")
    (tmp_path / "010_counter.sql").write_text("SELECT 100 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    layout = dashboard.pages[0].layout
    assert len(layout) == 2
    assert layout[0].position.y < layout[1].position.y


@pytest.mark.parametrize("query_names", [["a", "b", "c"], ["01", "02", "10"]])
def test_dashboard_metadata_sorts_widgets_alphanumerically(tmp_path, query_names):
    for query_name in query_names:
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)
    dashboard = dashboard_metadata.as_lakeview()
    assert [layout.widget.name for layout in dashboard.pages[0].layout] == query_names


def test_dashboard_metadata_orders_widget_using_overwrite(tmp_path):
    # Move the 'e' inbetween 'b' and 'c' query. Note that the order 1 puts 'e' on the same position as 'b', but with an
    # order tiebreaker the query name decides the final order.
    (tmp_path / "e.sql").write_text("-- --order 1\nSELECT 1 AS count")
    for query_name in "abcdf":
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    assert [layout.widget.name for layout in dashboard.pages[0].layout] == list("abecdf")


def test_dashboard_metadata_orders_widget_with_overwrite_zero(tmp_path):
    # Move the 'e' inbetween 'a' and 'b' query. Note that the order 0 puts 'e' on the same position as 'a', but with an
    # order tiebreaker the query name decides the final order.
    (tmp_path / "e.sql").write_text("-- --order 0\nSELECT 1 AS count")
    for query_name in "abcdf":
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    assert [layout.widget.name for layout in dashboard.pages[0].layout] == list("aebcdf")


def test_dashboard_metadata_orders_widgets_using_id(tmp_path):
    (tmp_path / "z.sql").write_text("-- --id a\nSELECT 1 AS count")  # Should be first because id is 'a'
    for query_name in "bcdef":
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    assert [layout.widget.name for layout in dashboard.pages[0].layout] == list("abcdef")


def test_dashboard_metadata_orders_widgets_with_overwrite_from_dashboard_yaml(tmp_path):
    content = """
display_name: Ordering

tiles:
  e:
    order: -1
"""
    (tmp_path / "dashboard.yml").write_text(content)
    for query_name in "abcdef":
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    assert [layout.widget.name for layout in dashboard.pages[0].layout] == list("eabcdf")


def test_dashboard_metadata_orders_widget_where_header_takes_precedence(tmp_path):
    content = """
display_name: Ordering

tiles:
  query_1:
    order: -1  # Does not matter because order is defined in query header as well
"""
    (tmp_path / "dashboard.yml").write_text(content)
    for index in range(3):
        (tmp_path / f"query_{index}.sql").write_text(f"-- --order {index}\nSELECT {index} AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    assert [layout.widget.name for layout in dashboard.pages[0].layout] == ["query_0", "query_1", "query_2"]


@pytest.mark.parametrize(
    "query, width, height",
    [
        ("SELECT 1 AS count", 1, 3),
        ("SELECT " + ", ".join(string.ascii_letters), 6, 6),
    ],
)
def test_dashboard_metadata_creates_widget_width_and_height(tmp_path, query, width, height):
    (tmp_path / "query.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    position = dashboard.pages[0].layout[0].position
    assert position.width == width
    assert position.height == height


def test_dashboard_metadata_creates_markdown_widget_width_and_height(tmp_path):
    (tmp_path / "description.md").write_text("# Description")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    position = dashboard.pages[0].layout[0].position
    assert position.width == 6
    assert position.height == 3


def test_dashboard_metadata_creates_markdown_widget_text(tmp_path):
    content = "# Description"
    (tmp_path / "description.md").write_text(content)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)
    dashboard = dashboard_metadata.as_lakeview()
    assert dashboard.pages[0].layout[0].widget.textbox_spec == content


@pytest.mark.parametrize(
    "header",
    [
        "-- --width 6 --height 3",
        "/* --width 6 --height 3 */",
        "/*\n--width 6\n--height 3 */",
    ],
)
def test_dashboard_metadata_creates_widget_with_custom_size(tmp_path, header):
    query = f"{header}\nSELECT 82917019218921 AS big_number_needs_big_widget"
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    position = dashboard.pages[0].layout[0].position
    assert position.width == 6
    assert position.height == 3


def test_dashboard_metadata_creates_widget_with_title(tmp_path):
    query = "-- --title 'Count me in'\nSELECT 2918"
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    frame = dashboard.pages[0].layout[0].widget.spec.frame
    assert frame.title == "Count me in"
    assert frame.show_title


def test_dashboard_metadata_creates_widget_without_title(tmp_path):
    (tmp_path / "counter.sql").write_text("SELECT 109")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    frame = dashboard.pages[0].layout[0].widget.spec.frame
    assert frame.title == ""
    assert not frame.show_title


def test_dashboard_metadata_creates_widget_with_description(tmp_path):
    query = "-- --description 'Only when it counts'\nSELECT 2918"
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    frame = dashboard.pages[0].layout[0].widget.spec.frame
    assert frame.description == "Only when it counts"
    assert frame.show_description


def test_dashboard_metadata_creates_widget_without_description(tmp_path):
    (tmp_path / "counter.sql").write_text("SELECT 190219")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    frame = dashboard.pages[0].layout[0].widget.spec.frame
    assert frame.description == ""
    assert not frame.show_description


def test_dashboard_metadata_creates_widget_with_filter(tmp_path):
    filter_column = "City"
    query = f"-- --filter {filter_column}\nSELECT Address, City, Province, Country FROM europe"
    (tmp_path / "table.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    layouts = dashboard.pages[0].layout
    assert any(f"filter_{filter_column}" in layout.widget.name for layout in layouts)
    filter_query = [layout.widget for layout in layouts if filter_column in layout.widget.name][0].queries[0]
    assert filter_query.name == f"filter_{filter_column}"
    assert len(filter_query.query.fields) == 2
    assert filter_query.query.fields[0].name == filter_column  # Filter column
    assert filter_column in filter_query.query.fields[1].name  # Associativity column


def test_dashboard_metadata_handles_incorrect_query_header(tmp_path, caplog):
    # Typo is on purpose
    query = "-- --widh 6 --height 5 \nSELECT 82917019218921 AS big_number_needs_big_widget"
    query_path = tmp_path / "counter.sql"
    query_path.write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.lsql.dashboards"):
        dashboard = dashboard_metadata.as_lakeview()

    position = dashboard.pages[0].layout[0].position
    assert position.width == 1
    assert position.height == 5
    assert query_path.as_posix() in caplog.text


def test_dashboard_metadata_reads_markdown_header(tmp_path):
    for query_name in "abcdef":
        (tmp_path / f"{query_name}.sql").write_text("SELECT 1 AS count")
    content = "---\norder: -1\nwidth: 6\nheight: 3\n---\n# Description"
    (tmp_path / "widget.md").write_text(content)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    position = dashboard.pages[0].layout[0].position
    assert position.width == 6
    assert position.height == 3


def test_dashboard_metadata_reads_header_above_select_when_query_has_cte(tmp_path):
    query = "WITH data AS (SELECT 1 AS count)\n" "-- --width 6 --height 6\n" "SELECT count FROM data"
    (tmp_path / "widget.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    position = dashboard.pages[0].layout[0].position
    assert position.width == 6
    assert position.height == 6


def test_dashboard_metadata_ignores_first_line_metadata_when_query_has_cte(tmp_path):
    query = "-- --width 6 --height 6\n" "WITH data AS (SELECT 1 AS count)\n" "SELECT count FROM data"
    (tmp_path / "widget.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    dashboard = dashboard_metadata.as_lakeview()

    position = dashboard.pages[0].layout[0].position
    assert position.width != 6
    assert position.height != 6


def test_dashboards_saves_sql_files_to_folder(tmp_path):
    ws = create_autospec(WorkspaceClient)
    dashboard_metadata = DashboardMetadata.from_path(Path(__file__).parent / "queries")
    dashboard = dashboard_metadata.as_lakeview()

    Dashboards(ws).save_to_folder(dashboard, tmp_path)

    assert len(list(tmp_path.glob("*.sql"))) == len(dashboard.datasets)
    ws.assert_not_called()


def test_dashboards_saves_yml_files_to_folder(tmp_path):
    ws = create_autospec(WorkspaceClient)
    dashboard_metadata = DashboardMetadata.from_path(Path(__file__).parent / "queries")
    dashboard = dashboard_metadata.as_lakeview()

    Dashboards(ws).save_to_folder(dashboard, tmp_path)

    assert len(list(tmp_path.glob("*.yml"))) == len(dashboard.pages)
    ws.assert_not_called()


def test_dashboards_calls_create_without_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    dashboard_metadata = DashboardMetadata("test")

    dashboards.create_dashboard(dashboard_metadata, parent_path="/non/existing/path", warehouse_id="warehouse")

    ws.lakeview.create.assert_called_with(
        "test",
        parent_path="/non/existing/path",
        serialized_dashboard=json.dumps({"pages": [{"displayName": "test", "name": "test"}]}),
        warehouse_id="warehouse",
    )
    ws.lakeview.update.assert_not_called()


def test_dashboards_calls_update_with_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    dashboard_metadata = DashboardMetadata("test")

    dashboards.create_dashboard(dashboard_metadata, dashboard_id="id", warehouse_id="warehouse")

    ws.lakeview.create.assert_not_called()
    ws.lakeview.update.assert_called_with(
        "id",
        display_name="test",
        serialized_dashboard=json.dumps({"pages": [{"displayName": "test", "name": "test"}]}),
        warehouse_id="warehouse",
    )


def test_dashboards_calls_publish():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    dashboard_metadata = DashboardMetadata("test")

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, publish=True)

    ws.lakeview.publish.assert_called_once_with(sdk_dashboard.dashboard_id)


def test_dashboard_raises_value_error_when_creating_dashboard_with_invalid_queries(tmp_path):
    (tmp_path / "valid.sql").write_text("SELECT 1")
    (tmp_path / "invalid.sql").write_text("SELECT COUNT(* FROM table")  # Missing closing parenthesis on purpose
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)

    with pytest.raises(ValueError):
        dashboards.create_dashboard(dashboard_metadata, publish=True)
    ws.assert_not_called()


def test_dashboard_warns_deploy_dashboard_is_deprecated(tmp_path):
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    dashboard_metadata = DashboardMetadata("test")

    with pytest.deprecated_call():
        dashboards.deploy_dashboard(dashboard_metadata)
    ws.assert_not_called()


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


def test_dashboards_get_dashboard_url():
    dashboard_url_expected = "https://adb-0123456789.12.azuredatabricks.net/dashboardsv3/1234/published"
    ws = create_autospec(WorkspaceClient)
    ws.config.host = "https://adb-0123456789.12.azuredatabricks.net"
    dashboard_url = Dashboards(ws).get_url("1234")
    assert dashboard_url == dashboard_url_expected
