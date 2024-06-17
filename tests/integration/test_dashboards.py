import json
import logging
from pathlib import Path

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard

from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview.model import Dashboard

logger = logging.getLogger(__name__)


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

    def create(display_name: str = "") -> SDKDashboard:
        if len(display_name) == 0:
            display_name = f"created_by_lsql_{make_random()}"
        else:
            display_name = f"{display_name} ({make_random()})"
        dashboard = ws.lakeview.create(display_name)
        return dashboard

    def delete(dashboard: SDKDashboard) -> None:
        ws.lakeview.trash(dashboard.dashboard_id)

    yield from factory("dashboard", create, delete)


def test_dashboards_deploys_exported_dashboard_definition(ws, make_dashboard):
    sdk_dashboard = make_dashboard()

    dashboard_file = Path(__file__).parent / "dashboards" / "dashboard.json"
    with dashboard_file.open("r") as f:
        lakeview_dashboard = Dashboard.from_dict(json.load(f))

    dashboards = Dashboards(ws)
    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_the_same_as_created_dashboard(ws, make_dashboard, tmp_path):
    sdk_dashboard = make_dashboard()

    with (tmp_path / "counter.sql").open("w") as f:
        f.write("SELECT 10 AS count")
    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)
    new_dashboard = dashboards.get_dashboard(sdk_dashboard.path)

    assert (
        dashboards._with_better_names(lakeview_dashboard).as_dict()
        == dashboards._with_better_names(new_dashboard).as_dict()
    )


def test_dashboard_deploys_dashboard_with_ten_counters(ws, make_dashboard, tmp_path):
    sdk_dashboard = make_dashboard()

    for i in range(10):
        with (tmp_path / f"counter_{i}.sql").open("w") as f:
            f.write(f"SELECT {i} AS count")
    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_display_name(ws, make_dashboard, tmp_path):
    sdk_dashboard = make_dashboard(display_name="Counter")

    with (tmp_path / "dashboard.yml").open("w") as f:
        f.write("display_name: Counter")
    with (tmp_path / "counter.sql").open("w") as f:
        f.write("SELECT 102132 AS count")

    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_counter_variation(ws, make_dashboard, tmp_path):
    sdk_dashboard = make_dashboard()

    with (tmp_path / "counter.sql").open("w") as f:
        f.write("SELECT 10 AS something_else_than_count")
    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_big_widget(ws, make_dashboard, tmp_path):
    sdk_dashboard = make_dashboard()

    query = """-- --width 6 --height 3\nSELECT 82917019218921 AS big_number_needs_big_widget"""
    with (tmp_path / "counter.sql").open("w") as f:
        f.write(query)
    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboards_deploys_dashboard_with_order_overwrite(ws, make_dashboard, tmp_path):
    sdk_dashboard = make_dashboard()

    for query_name in range(6):
        with (tmp_path / f"{query_name}.sql").open("w") as f:
            f.write(f"SELECT {query_name} AS count")

    # Move the '4' inbetween '1' and '2' query. Note that the order 1 puts '4' on the same position as '1', but with an
    # order tiebreaker the query name decides the final order.
    with (tmp_path / "4.sql").open("w") as f:
        f.write("-- --order 1\nSELECT 4 AS count")

    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboards_deploys_dashboard_with_markdown_header(ws, make_dashboard, tmp_path):
    sdk_dashboard = make_dashboard()

    for count, query_name in enumerate("abcdef"):
        (tmp_path / f"{query_name}.sql").write_text(f"SELECT {count} AS count")

    description = """
    ---
    order: 1
    ---
    Below you see counters.
    """
    (tmp_path / f"z_description.md").write_text(description)

    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)
