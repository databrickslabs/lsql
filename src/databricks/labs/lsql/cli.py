from pathlib import Path

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import Dashboards


logger = get_logger(__name__)
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
    print(sdk_dashboard.dashboard_id)



@lsql.command
def publish_dashboard(w: WorkspaceClient, dashboard_id: str):
    """Publish a dashboard."""
    logger.info("Creating dashboard ...")
    w.lakeview.publish(dashboard_id)


if __name__ == "__main__":
    lsql()