import webbrowser
from pathlib import Path

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards, QueryTile

logger = get_logger(__file__)
lsql = App(__file__)


@lsql.command
def create_dashboard(
    w: WorkspaceClient,
    folder: Path = Path.cwd(),
    *,
    catalog: str = "",
    database: str = "",
    no_open: bool = False,
):
    """Create a dashboard from queries"""
    logger.debug("Creating dashboard ...")
    lakeview_dashboards = Dashboards(w)
    folder = Path(folder)
    dashboard_metadata = DashboardMetadata.from_path(folder).replace_database(
        catalog=catalog or None,
        database=database or None,
    )
    lakeview_dashboard = dashboard_metadata.as_lakeview()
    sdk_dashboard = lakeview_dashboards.deploy_dashboard(lakeview_dashboard)
    if not no_open:
        assert sdk_dashboard.dashboard_id is not None
        dashboard_url = lakeview_dashboards.get_url(sdk_dashboard.dashboard_id)
        webbrowser.open(dashboard_url)
    print(sdk_dashboard.dashboard_id)


@lsql.command(is_unauthenticated=True)
def fmt(folder: Path = Path.cwd()):
    """Format SQL files in a folder"""
    logger.debug("Formatting SQL files ...")
    folder = Path(folder)
    for sql_file in folder.glob("**/*.sql"):
        sql = sql_file.read_text()
        formatted_sql = QueryTile.format(sql)
        sql_file.write_text(formatted_sql)
        logger.debug(f"Formatted {sql_file}")


if __name__ == "__main__":
    lsql()
