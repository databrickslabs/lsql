import datetime as dt
import json
import logging
import webbrowser
from pathlib import Path

import pytest
from databricks.labs.blueprint.entrypoint import is_in_debug
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import SchemaInfo
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard

from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards
from databricks.labs.lsql.lakeview.model import Dashboard

logger = logging.getLogger(__name__)
TEST_JOBS_PURGE_TIMEOUT = dt.timedelta(hours=1, minutes=15)


def get_test_purge_time() -> str:
    return (dt.datetime.utcnow() + TEST_JOBS_PURGE_TIMEOUT).strftime("%Y%m%d%H")


@pytest.fixture
def sql_backend(ws, env_or_skip) -> StatementExecutionBackend:
    warehouse_id = env_or_skip("TEST_DEFAULT_WAREHOUSE_ID")
    return StatementExecutionBackend(ws, warehouse_id)


def factory(name, create, remove):
    cleanup = []

    def inner(**kwargs):
        x = create(**kwargs)
        logger.debug(f"added {name} fixture: {x}")
        cleanup.append(x)
        return x

    yield inner
    logger.debug(f"clearing {len(cleanup)} {name} fixtures")
    for x in cleanup:
        try:
            logger.debug(f"removing {name} fixture: {x}")
            remove(x)
        except DatabricksError as e:
            # TODO: fix on the databricks-labs-pytester level
            logger.debug(f"ignoring error while {name} {x} teardown: {e}")


@pytest.fixture
def make_dashboard(ws, make_random):
    """Clean the lakeview dashboard"""

    def create(*, display_name: str = "") -> SDKDashboard:
        if len(display_name) == 0:
            display_name = f"created_by_lsql_{make_random()}"
        else:
            display_name = f"{display_name} ({make_random()})"
        dashboard = ws.lakeview.create(display_name)
        if is_in_debug():
            dashboard_url = f"{ws.config.host}/sql/dashboardsv3/{dashboard.dashboard_id}"
            webbrowser.open(dashboard_url)
        return dashboard

    def delete(dashboard: SDKDashboard) -> None:
        ws.lakeview.trash(dashboard.dashboard_id)

    yield from factory("dashboard", create, delete)


@pytest.fixture
def make_schema(ws, sql_backend, make_random):
    def create(*, catalog_name: str = "hive_metastore", name: str | None = None) -> SchemaInfo:
        if name is None:
            name = f"lsql_S{make_random(4)}".lower()
        full_name = f"{catalog_name}.{name}".lower()
        sql_backend.execute(f"CREATE SCHEMA {full_name} WITH DBPROPERTIES (RemoveAfter={get_test_purge_time()})")
        schema_info = SchemaInfo(catalog_name=catalog_name, name=name, full_name=full_name)
        logger.info(
            f"Schema {schema_info.full_name}: "
            f"{ws.config.host}/explore/data/{schema_info.catalog_name}/{schema_info.name}"
        )
        return schema_info

    def remove(schema_info: SchemaInfo):
        try:
            sql_backend.execute(f"DROP SCHEMA IF EXISTS {schema_info.full_name} CASCADE")
        except RuntimeError as e:
            if "SCHEMA_NOT_FOUND" in str(e):
                logger.warning("Schema was already dropped while executing the test", exc_info=e)
            else:
                raise e

    yield from factory("schema", create, remove)


@pytest.fixture
def tmp_path(tmp_path, make_random):
    """Adds a random subfolder name.

    The folder name becomes the dashboard name, which then becomes the Lakeview file name with the
    `.lvdash.json` extension. `tmp_path` last subfolder contains the test name cut off at thirty characters plus a
    number starting at zero indicating the test run. `tmp_path` adds randomness in the parent folders. Because most test
    start with `test_dashboards_creates_dashboard_`, the dashboard name for most tests ends up being
    `test_dashboard_deploys_dashboa0.lvdash.json`, causing collisions. This is solved by adding a random subfolder name.
    """
    folder = tmp_path / f"created_by_lsql_{make_random()}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def test_dashboards_creates_exported_dashboard_definition(ws, make_dashboard):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()
    dashboard_content = (Path(__file__).parent / "dashboards" / "dashboard.lvdash.json").read_text()

    ws.lakeview.update(sdk_dashboard.dashboard_id, serialized_dashboard=dashboard_content)
    lakeview_dashboard = Dashboard.from_dict(json.loads(dashboard_content))
    new_dashboard = dashboards.get_dashboard(sdk_dashboard.path)

    assert (
        dashboards._with_better_names(lakeview_dashboard).as_dict()
        == dashboards._with_better_names(new_dashboard).as_dict()
    )


def test_dashboard_deploys_dashboard_the_same_as_created_dashboard(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    (tmp_path / "counter.sql").write_text("SELECT 10 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)
    new_dashboard = dashboards.get_dashboard(sdk_dashboard.path)

    assert (
        dashboards._with_better_names(dashboard_metadata.as_lakeview()).as_dict()
        == dashboards._with_better_names(new_dashboard).as_dict()
    )


def test_dashboard_deploys_dashboard_with_ten_counters(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    for i in range(10):
        (tmp_path / f"counter_{i}.sql").write_text(f"SELECT {i} AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_display_name(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard(display_name="Counter")

    (tmp_path / "dashboard.yml").write_text("display_name: Counter")
    (tmp_path / "counter.sql").write_text("SELECT 102132 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_counter_variation(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    (tmp_path / "counter.sql").write_text("SELECT 10 AS `Something Else Than Count`")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_big_widget(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    query = """-- --width 6 --height 3\nSELECT 82917019218921 AS big_number_needs_big_widget"""
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboards_creates_dashboard_with_order_overwrite_in_query_header(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    for query_name in range(6):
        (tmp_path / f"{query_name}.sql").write_text(f"SELECT {query_name} AS count")
    # Move the '4' inbetween '1' and '2' query. Note that the order 1 puts '4' on the same position as '1', but with an
    # order tiebreaker the query name decides the final order.
    (tmp_path / "4.sql").write_text("-- --order 1\nSELECT 4 AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboards_creates_dashboard_with_order_overwrite_in_dashboard_yaml(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    # Move the '4' inbetween '1' and '2' query. Note that the order 1 puts '4' on the same position as '1', but with an
    # order tiebreaker the query name decides the final order.
    content = """
display_name: Counters

tiles:
  query_4:
    order: 1
""".lstrip()
    (tmp_path / "dashboard.yml").write_text(content)
    for query_name in range(6):
        (tmp_path / f"query_{query_name}.sql").write_text(f"SELECT {query_name} AS count")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_table(ws, make_dashboard):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    dashboard_folder = Path(__file__).parent / "dashboards" / "one_table"
    dashboard_metadata = DashboardMetadata.from_path(dashboard_folder)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboards_creates_dashboard_with_markdown_header(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    for count, query_name in enumerate("abcdef"):
        (tmp_path / f"{query_name}.sql").write_text(f"SELECT {count} AS count")
    (tmp_path / "z_description.md").write_text("---\norder: -1\n---\nBelow you see counters.")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboards_creates_dashboard_with_widget_title_and_description(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    description = "-- --title 'Counting' --description 'The answer to life'\nSELECT 42"
    (tmp_path / "counter.sql").write_text(description)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboards_creates_dashboard_from_query_with_cte(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    table_query_path = Path(__file__).parent / "dashboards/one_table/databricks_office_locations.sql"
    office_locations = table_query_path.read_text()
    query_with_cte = (
        f"WITH data AS ({office_locations})\n"
        "-- --title 'Databricks Office Locations'\n"
        "SELECT Address, State, Country FROM data"
    )
    (tmp_path / "table.sql").write_text(query_with_cte)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboards_creates_dashboard_with_filters(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    table_query_path = Path(__file__).parent / "dashboards/one_table/databricks_office_locations.sql"
    office_locations = table_query_path.read_text()
    (tmp_path / "table.sql").write_text(f"-- --width 2 --filter City State Country\n{office_locations}")
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_empty_title(ws, make_dashboard, tmp_path):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    query = '-- --overrides \'{"spec": {"frame": {"showTitle": true}}}\'\nSELECT 102132 AS count'
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


@pytest.mark.skip(reason="Missing permissions to create schema")
def test_dashboards_creates_dashboard_with_replace_database(ws, make_dashboard, tmp_path, sql_backend, make_schema):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()
    schema = make_schema()

    sql_backend.execute(f"CREATE TABLE {schema.full_name}.table AS SELECT 1 AS count")
    query = """
    -- Example query with CTA
    WITH data (
        SELECT count FROM inventory.table -- `inventory` will be replaced with the schema name
    )
    -- --title 'Count'
    SELECT count FROM data
    """
    (tmp_path / "counter.sql").write_text(query)
    dashboard_metadata = DashboardMetadata.from_path(tmp_path).replace_database(database=schema.full_name)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_filters(ws, make_dashboard):
    dashboards = Dashboards(ws)
    sdk_dashboard = make_dashboard()

    dashboard_folder = Path(__file__).parent / "dashboards" / "filter_spec_basic"
    dashboard_metadata = DashboardMetadata.from_path(dashboard_folder)

    sdk_dashboard = dashboards.create_dashboard(dashboard_metadata, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)
