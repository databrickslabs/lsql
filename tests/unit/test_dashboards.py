import pytest
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview import Dashboard


def test_dashboard_raises_value_error_with_missing_display_name_and_dashboard_id():
    ws = create_autospec(WorkspaceClient)
    dashboards = Dashboards(ws)
    lakeview_dashboard = Dashboard([], [])
    with pytest.raises(ValueError):
        dashboards.deploy_dashboard(lakeview_dashboard)
    ws.assert_not_called()