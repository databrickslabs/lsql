import logging
from pathlib import Path

from databricks.labs.blueprint.cli import App
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import Dashboards


logger = logging.getLogger(__name__)
lsql = App(__file__)


@lsql.command
def create_dashboard(w: WorkspaceClient, dashboard_folder: Path | str):
    """Create a dashboard from queries"""
    logger.info("Creating dashboard ...")
    dashboards = Dashboards(w)
    lakeview_dashboard = dashboards.create_dashboard(Path(dashboard_folder))
    sdk_dashboard = dashboards.deploy_dashboard(lakeview_dashboard)
    dashboard_url = f"{w.config.host}/sql/dashboardsv3/{sdk_dashboard.dashboard_id}"
    logger.info(f"Created dashboard: {dashboard_url}.")

