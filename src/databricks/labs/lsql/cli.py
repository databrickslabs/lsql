import webbrowser
from collections.abc import Iterable
from pathlib import Path

from databricks.labs.blueprint.cli import App
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.sdk import WorkspaceClient

from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards, QueryTile

logger = get_logger(__file__)
lsql = App(__file__)

STRING_AFFIRMATIVES = {"yes", "y", "true", "t", "1"}


@lsql.command
def create_dashboard(
    w: WorkspaceClient,
    folder: Path = Path.cwd(),
    *,
    catalog: str = "",
    database: str = "",
    publish: str = "false",
    open_browser: str = "false",
):
    """Create a dashboard from queries"""
    should_publish = publish.lower() in STRING_AFFIRMATIVES
    should_open_browser = open_browser.lower() in STRING_AFFIRMATIVES

    logger.debug("Creating dashboard ...")
    lakeview_dashboards = Dashboards(w)
    folder = Path(folder)
    dashboard_metadata = DashboardMetadata.from_path(folder).replace_database(
        catalog=catalog or None,
        database=database or None,
    )
    sdk_dashboard = lakeview_dashboards.create_dashboard(dashboard_metadata, publish=should_publish)
    if should_open_browser:
        assert sdk_dashboard.dashboard_id is not None
        dashboard_url = lakeview_dashboards.get_url(sdk_dashboard.dashboard_id)
        webbrowser.open(dashboard_url)
    print(sdk_dashboard.dashboard_id)


@lsql.command(is_unauthenticated=True)
def fmt(folder: Path = Path.cwd(), *, normalize_case: str = "true", exclude: Iterable[str] = ()):
    """Format SQL files in a folder"""
    logger.debug("Formatting SQL files ...")
    folder = Path(folder)
    exclusions = list(Path(folder, excluded) for excluded in exclude)
    should_normalize_case = normalize_case in STRING_AFFIRMATIVES
    for sql_file in folder.glob("**/*.sql"):
        if any(sql_file.is_relative_to(exclusion) for exclusion in exclusions):
            continue
        sql = sql_file.read_text()
        formatted_sql = QueryTile.format(sql, normalize_case=should_normalize_case)
        sql_file.write_text(formatted_sql)
        logger.debug(f"Formatted {sql_file}")


if __name__ == "__main__":
    lsql()
