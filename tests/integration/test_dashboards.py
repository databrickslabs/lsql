import json
import logging
from pathlib import Path

import pytest

from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview.model import Dashboard
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard


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

    assert dashboards._with_better_names(lakeview_dashboard).as_dict() == dashboards._with_better_names(new_dashboard).as_dict()


def test_dashboard_deploys_dashboard_with_ten_counters(ws, make_dashboard, tmp_path):
    sdk_dashboard = make_dashboard()

    for i in range(10):
        with (tmp_path / f"counter_{i}.sql").open("w") as f:
            f.write(f"SELECT {i} AS count")
    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=sdk_dashboard.dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)


def test_dashboard_deploys_dashboard_with_counter_variation(ws, dashboard_id, tmp_path):
    with (tmp_path / "counter.sql").open("w") as f:
        f.write("SELECT 10 AS something_else_than_count")
    dashboards = Dashboards(ws)
    lakeview_dashboard = dashboards.create_dashboard(tmp_path)

    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard, dashboard_id=dashboard_id)

    assert ws.lakeview.get(sdk_dashboard.dashboard_id)
