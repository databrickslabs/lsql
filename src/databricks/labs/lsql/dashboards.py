import argparse
import copy
import dataclasses
import json
import logging
import re
import shlex
from argparse import ArgumentParser
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

import sqlglot
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard
from databricks.sdk.service.workspace import ExportFormat

from databricks.labs.lsql.lakeview import (
    ControlFieldEncoding,
    CounterEncodingMap,
    CounterFieldEncoding,
    CounterSpec,
    Dashboard,
    Dataset,
    Field,
    Layout,
    NamedQuery,
    Page,
    Position,
    Query,
    RenderFieldEncoding,
    TableEncodingMap,
    TableV2Spec,
    Widget,
    WidgetFrameSpec,
    WidgetSpec,
)

_MAXIMUM_DASHBOARD_WIDTH = 6
T = TypeVar("T")
logger = logging.getLogger(__name__)


@dataclass
class DashboardMetadata:
    display_name: str

    @classmethod
    def from_dict(cls, raw: dict[str, str]) -> "DashboardMetadata":
        return cls(
            display_name=raw["display_name"],
        )

    def as_dict(self) -> dict[str, str]:
        return dataclasses.asdict(self)

    @classmethod
    def from_path(cls, path: Path) -> "DashboardMetadata":
        """Export dashboard metadata from a YAML file."""
        fallback_metadata = cls(display_name=path.parent.name)

class WidgetMetadata:
    id: str
    order: int
    width: int
    height: int


class QueryHandler(BaseHandler):
    """Handle query files."""

    @staticmethod
    def _get_arguments_parser() -> ArgumentParser:
        parser = ArgumentParser("WidgetMetadata", add_help=False, exit_on_error=False)
        parser.add_argument("--id", type=str)
        parser.add_argument("-o", "--order", type=int)
        parser.add_argument("-w", "--width", type=int)
        parser.add_argument("-h", "--height", type=int)
        parser.add_argument("-t", "--title", type=str)
        parser.add_argument("-d", "--description", type=str)
        return parser

    def _parse_header(self, header: str) -> dict[str, str]:
        """Header is an argparse string."""
        parser = self._get_arguments_parser()
        try:
            return vars(parser.parse_args(shlex.split(header)))
        except (argparse.ArgumentError, SystemExit) as e:
            logger.warning(f"Parsing {arguments}: {e}")
            return dataclasses.replace(self)
        return dataclasses.replace(
            self,
            id=args.id or self.id,
            order=args.order or self.order,
            width=args.width or self.width,
            height=args.height or self.height,
        )


class Dashboards:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def get_dashboard(self, dashboard_path: str) -> Dashboard:
        with self._ws.workspace.download(dashboard_path, format=ExportFormat.SOURCE) as f:
            raw = f.read().decode("utf-8")
            as_dict = json.loads(raw)
            return Dashboard.from_dict(as_dict)

    def save_to_folder(self, dashboard: Dashboard, local_path: Path) -> Dashboard:
        local_path.mkdir(parents=True, exist_ok=True)
        dashboard = self._with_better_names(dashboard)
        for dataset in dashboard.datasets:
            query = self._format_query(dataset.query)
            with (local_path / f"{dataset.name}.sql").open("w") as f:
                f.write(query)
        for page in dashboard.pages:
            with (local_path / f"{page.name}.yml").open("w") as f:
                yaml.safe_dump(page.as_dict(), f)
        return dashboard

    @staticmethod
    def _format_query(query: str) -> str:
        try:
            parsed_query = sqlglot.parse(query)
        except sqlglot.ParseError:
            return query
        statements = []
        for statement in parsed_query:
            if statement is None:
                continue
            # see https://sqlglot.com/sqlglot/generator.html#Generator
            statements.append(
                statement.sql(
                    dialect="databricks",
                    normalize=True,  # normalize identifiers to lowercase
                    pretty=True,  # format the produced SQL string
                    normalize_functions="upper",  # normalize function names to uppercase
                    max_text_width=80,  # wrap text at 120 characters
                )
            )
        formatted_query = ";\n".join(statements)
        return formatted_query

    def create_dashboard(
        self,
        dashboard_folder: Path,
        *,
        query_transformer: Callable[[sqlglot.Expression], sqlglot.Expression] | None = None,
    ) -> Dashboard:
        """Create a dashboard from code, i.e. configuration and queries.

        Parameters :
            dashboard_folder : Path
                The path to the folder with dashboard files.
            query_transformer : Callable[[sqlglot.Expression], sqlglot.Expression] | None (default | None)
                A sqlglot transformer applied on the queries (SQL files) before creating the dashboard. If None, no
                transformation will be applied.

        Source :
            https://sqlglot.com/sqlglot/transforms.html
        """
        dashboard_metadata = DashboardMetadata.from_path(dashboard_folder / "dashboard.yml")
        widgets_metadata = self._parse_widgets_metadata(dashboard_folder)
        tiles = self._get_tiles(widgets_metadata, query_transformer=query_transformer)
        datasets = self._get_datasets(tiles)
        layouts = self._get_layouts(tiles)
        page = Page(
            name=dashboard_metadata.display_name,
            display_name=dashboard_metadata.display_name,
            layout=layouts,
        )
        lakeview_dashboard = Dashboard(datasets=datasets, pages=[page])
        return lakeview_dashboard

    @staticmethod
    def _parse_widgets_metadata(dashboard_folder: Path) -> list[WidgetMetadata]:
        """Parse the widget metadata from each (optional) header."""
        widgets_metadata = []
        for path in dashboard_folder.iterdir():
            if path.suffix in {".sql", ".md"}:
                widget_metadata = WidgetMetadata.from_path(path)
                widgets_metadata.append(widget_metadata)
        return widgets_metadata

    @staticmethod
    def _get_tiles(
        widgets_metadata: list[WidgetMetadata],
        query_transformer: Callable[[sqlglot.Expression], sqlglot.Expression] | None = None,
    ) -> list[Tile]:
        """Create tiles from the widgets metadata.

        The order of the tiles is by default the alphanumerically sorted tile ids, however, the order may be overwritten
        with the `order` key. Hence, the multiple loops to get:
        i) set the order when not specified;
        ii) sort the widgets using the order field.
        """
        widgets_metadata_with_order = []
        for order, widget_metadata in enumerate(sorted(widgets_metadata, key=lambda wm: wm.id)):
            replica = copy.deepcopy(widget_metadata)
            replica.order = widget_metadata.order if widget_metadata.order is not None else order
            widgets_metadata_with_order.append(replica)

        tiles, position = [], Position(0, 0, 0, 0)  # Position of first tile
        for widget_metadata in sorted(widgets_metadata_with_order, key=lambda wm: (wm.order, wm.id)):
            tile = Tile.from_widget_metadata(widget_metadata)
            if isinstance(tile, QueryTile):
                tile.query_transformer = query_transformer
            placed_tile = tile.place_after(position)
            tiles.append(placed_tile)
            position = placed_tile.position
        return tiles

    def _get_layouts(self, widgets: list[Widget], widgets_metadata: list[WidgetMetadata]) -> list[Layout]:
        layouts, position = [], Position(0, 0, 0, 0)  # First widget position
        for widget, widget_metadata in sorted(widgets, key=lambda w: (w[1].order, w[1].id)):
            position = self._get_position(widget_metadata, position)
            layout = Layout(widget=widget, position=position)
            layouts.append(layout)
        return layouts

    @staticmethod
    def _parse_dashboard_metadata(dashboard_folder: Path) -> DashboardMetadata:
        fallback_metadata = DashboardMetadata(display_name=dashboard_folder.name)

        dashboard_metadata_path = dashboard_folder / "dashboard.yml"
        if not dashboard_metadata_path.exists():
            return fallback_metadata

        try:
            raw = yaml.safe_load(dashboard_metadata_path.read_text())
        except yaml.YAMLError as e:
            logger.warning(f"Parsing {dashboard_metadata_path}: {e}")
            return fallback_metadata
        try:
            return DashboardMetadata.from_dict(raw)
        except KeyError as e:
            logger.warning(f"Parsing {dashboard_metadata_path}: {e}")
            return fallback_metadata

    def _parse_widget_metadata(self, path: Path, widget: Widget, order: int) -> WidgetMetadata:
        width, height = self._get_width_and_height(widget)
        fallback_metadata = WidgetMetadata(
            id=path.stem,
            order=order,
            width=width,
            height=height,
        )

        try:
            parsed_query = sqlglot.parse_one(path.read_text(), dialect=sqlglot.dialects.Databricks)
        except sqlglot.ParseError as e:
            logger.warning(f"Parsing {path}: {e}")
            return fallback_metadata

        if parsed_query.comments is None or len(parsed_query.comments) == 0:
            return fallback_metadata

        first_comment = parsed_query.comments[0]
        return fallback_metadata.replace_from_arguments(shlex.split(first_comment))

    @staticmethod
    def _get_text_widget(widget_metadata: WidgetMetadata) -> Widget:
        widget = Widget(name=widget_metadata.id, textbox_spec=widget_metadata.path.read_text())
        return widget

    def _get_counter_widget(self, widget_metadata: WidgetMetadata) -> Widget:
        fields = self._get_fields(widget_metadata.path.read_text())
        query = Query(dataset_name=widget_metadata.id, fields=fields, disaggregated=True)
        # As far as testing went, a NamedQuery should always have "main_query" as name
        named_query = NamedQuery(name="main_query", query=query)
        # Counters are expected to have one field
        counter_field_encoding = CounterFieldEncoding(field_name=fields[0].name, display_name=fields[0].name)
        counter_spec = CounterSpec(CounterEncodingMap(value=counter_field_encoding))
        widget = Widget(name=widget_metadata.id, queries=[named_query], spec=counter_spec)
        return widget

    @staticmethod
    def _get_fields(query: str) -> list[Field]:
        parsed_query = sqlglot.parse_one(query, dialect=sqlglot.dialects.Databricks)
        fields = []
        for projection in parsed_query.find_all(sqlglot.exp.Select):
            if projection.depth > 0:
                continue
            for named_select in projection.named_selects:
                field = Field(name=named_select, expression=f"`{named_select}`")
                fields.append(field)
        return fields

    @staticmethod
    def _get_position(previous_position: Position, widget_metadata: WidgetMetadata) -> Position:
        x = previous_position.x + previous_position.width
        if x + widget_metadata.width > _MAXIMUM_DASHBOARD_WIDTH:
            x = 0
            y = previous_position.y + previous_position.height
        else:
            y = previous_position.y
        position = Position(x=x, y=y, width=widget_metadata.width, height=widget_metadata.height)
        return position

    def deploy_dashboard(self, lakeview_dashboard: Dashboard, *, dashboard_id: str | None = None) -> SDKDashboard:
        """Deploy a lakeview dashboard."""
        if dashboard_id is not None:
            dashboard = self._ws.lakeview.update(
                dashboard_id, serialized_dashboard=json.dumps(lakeview_dashboard.as_dict())
            )
        else:
            display_name = lakeview_dashboard.pages[0].display_name or lakeview_dashboard.pages[0].name
            dashboard = self._ws.lakeview.create(
                display_name, serialized_dashboard=json.dumps(lakeview_dashboard.as_dict())
            )
        return dashboard

    def _with_better_names(self, dashboard: Dashboard) -> Dashboard:
        """Replace names with human-readable names."""
        better_names = {}
        for dataset in dashboard.datasets:
            if dataset.display_name is not None:
                better_names[dataset.name] = dataset.display_name
        for page in dashboard.pages:
            if page.display_name is not None:
                better_names[page.name] = page.display_name
        return self._replace_names(dashboard, better_names)

    def _replace_names(self, node: T, better_names: dict[str, str]) -> T:
        # walk every dataclass instance recursively and replace names
        if dataclasses.is_dataclass(node):
            for field in dataclasses.fields(node):
                value = getattr(node, field.name)
                if isinstance(value, list):
                    setattr(node, field.name, [self._replace_names(item, better_names) for item in value])
                elif dataclasses.is_dataclass(value):
                    setattr(node, field.name, self._replace_names(value, better_names))
        if isinstance(node, Dataset):
            node.name = better_names.get(node.name, node.name)
        elif isinstance(node, Page):
            node.name = better_names.get(node.name, node.name)
        elif isinstance(node, Query):
            node.dataset_name = better_names.get(node.dataset_name, node.dataset_name)
        elif isinstance(node, NamedQuery) and node.query:
            # 'dashboards/01eeb077e38c17e6ba3511036985960c/datasets/01eeb081882017f6a116991d124d3068_...'
            if node.name.startswith("dashboards/"):
                parts = [node.query.dataset_name]
                for query_field in node.query.fields:
                    parts.append(query_field.name)
                new_name = "_".join(parts)
                better_names[node.name] = new_name
            node.name = better_names.get(node.name, node.name)
        elif isinstance(node, ControlFieldEncoding):
            node.query_name = better_names.get(node.query_name, node.query_name)
        elif isinstance(node, Widget):
            if node.spec is not None:
                node.name = node.spec.as_dict().get("widgetType", node.name)
        return node
