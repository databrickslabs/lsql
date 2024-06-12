import argparse
import copy
import dataclasses
import json
import logging
import shlex
from argparse import ArgumentParser
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


class WidgetMetadata:
    def __init__(
        self,
        path: Path,
        order: int = 0,
        width: int = 0,
        height: int = 0,
        _id: str = "",
    ):
        self.path = path
        self.order = order
        self.width = width
        self.height = height
        self.id = _id

        size = self._size
        self.width = self.width or size[0]
        self.height = self.height or size[1]
        self.id = self.id or path.stem

    def is_markdown(self) -> bool:
        return self.path.suffix == ".md"

    @property
    def spec_type(self) -> type[WidgetSpec]:
        # TODO: When supporting more specs, infer spec from query
        return CounterSpec

    @property
    def _size(self) -> tuple[int, int]:
        """Get the width and height for a widget.

        The tiling logic works if:
        - width < _MAXIMUM_DASHBOARD_WIDTH : heights for widgets on the same row should be equal
        - width == _MAXIMUM_DASHBOARD_WIDTH : any height
        """
        if self.is_markdown():
            return _MAXIMUM_DASHBOARD_WIDTH, 2
        if self.spec_type == CounterSpec:
            return 1, 3
        return 0, 0

    def as_dict(self) -> dict[str, str]:
        body = {"path": self.path.as_posix()}
        for attribute in "order", "width", "height", "id":
            if attribute in body:
                continue
            value = getattr(self, attribute)
            if value is not None:
                body[attribute] = str(value)
        return body

    @staticmethod
    def _get_arguments_parser() -> ArgumentParser:
        parser = ArgumentParser("WidgetMetadata", add_help=False, exit_on_error=False)
        parser.add_argument("--id", type=str)
        parser.add_argument("-o", "--order", type=int)
        parser.add_argument("-w", "--width", type=int)
        parser.add_argument("-h", "--height", type=int)
        return parser

    def replace_from_arguments(self, arguments: list[str]) -> "WidgetMetadata":
        replica = copy.deepcopy(self)
        parser = self._get_arguments_parser()
        try:
            args = parser.parse_args(arguments)
        except (argparse.ArgumentError, SystemExit) as e:
            logger.warning(f"Parsing {arguments}: {e}")
            return replica

        replica.order = args.order or self.order
        replica.width = args.width or self.width
        replica.height = args.height or self.height
        replica.id = args.id or self.id
        return replica

    @classmethod
    def from_path(cls, path: Path) -> "WidgetMetadata":
        fallback_metadata = cls(path=path)

        try:
            parsed_query = sqlglot.parse_one(path.read_text(), dialect=sqlglot.dialects.Databricks)
        except sqlglot.ParseError as e:
            logger.warning(f"Parsing {path}: {e}")
            return fallback_metadata

        if parsed_query.comments is None or len(parsed_query.comments) == 0:
            return fallback_metadata

        first_comment = parsed_query.comments[0]
        return fallback_metadata.replace_from_arguments(shlex.split(first_comment))


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

    def create_dashboard(self, dashboard_folder: Path) -> Dashboard:
        """Create a dashboard from code, i.e. configuration and queries."""
        dashboard_metadata = self._parse_dashboard_metadata(dashboard_folder)
        widgets_metadata = self._get_widgets_metadata(dashboard_folder)
        datasets = self._get_datasets(dashboard_folder)
        widgets = self._get_widgets(widgets_metadata)
        layouts = self._get_layouts(widgets, widgets_metadata)
        page = Page(
            name=dashboard_metadata.display_name,
            display_name=dashboard_metadata.display_name,
            layout=layouts,
        )
        lakeview_dashboard = Dashboard(datasets=datasets, pages=[page])
        return lakeview_dashboard

    @staticmethod
    def _get_datasets(dashboard_folder: Path) -> list[Dataset]:
        datasets = []
        for query_path in sorted(dashboard_folder.glob("*.sql")):
            with query_path.open("r") as query_file:
                raw_query = query_file.read()
            dataset = Dataset(name=query_path.stem, display_name=query_path.stem, query=raw_query)
            datasets.append(dataset)
        return datasets

    @staticmethod
    def _get_widgets_metadata(dashboard_folder: Path) -> list[WidgetMetadata]:
        """Read and parse the widget metadata from each (optional) header.

        The order is by default the alphanumerically sorted files, however, the order may be overwritten in the file
        header with the `order` key. Hence, the multiple loops to get:
        i) the optional order from the file header;
        ii) set the order when not specified;
        iii) sort the widgets using the order field.
        """
        widgets_metadata = []
        for path in sorted(dashboard_folder.iterdir()):
            if path.suffix not in {".sql", ".md"}:
                continue
            widget_metadata = WidgetMetadata.from_path(path)
            widgets_metadata.append(widget_metadata)
        widgets_metadata_with_order = []
        for order, widget_metadata in enumerate(sorted(widgets_metadata, key=lambda wm: wm.id)):
            replica = copy.deepcopy(widget_metadata)
            replica.order = widget_metadata.order or order
            widgets_metadata_with_order.append(replica)
        widgets_metadata_sorted = list(sorted(widgets_metadata_with_order, key=lambda wm: (wm.order, wm.id)))
        return widgets_metadata_sorted

    def _get_widgets(self, widgets_metadata: list[WidgetMetadata]) -> list[Widget]:
        widgets = []
        for widget_metadata in widgets_metadata:
            try:
                widget = self._get_widget(widget_metadata)
            except sqlglot.ParseError as e:
                logger.warning(f"Parsing {widget_metadata.path}: {e}")
                continue
            widgets.append(widget)
        return widgets

    def _get_layouts(self, widgets: list[Widget], widgets_metadata: list[WidgetMetadata]) -> list[Layout]:
        layouts, position = [], Position(0, 0, 0, 0)  # First widget position
        for widget, widget_metadata in zip(widgets, widgets_metadata):
            position = self._get_position(position, widget_metadata)
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

    def _get_widget(self, widget_metadata: WidgetMetadata) -> Widget:
        if widget_metadata.is_markdown():
            return self._get_text_widget(widget_metadata)
        return self._get_counter_widget(widget_metadata)

    @staticmethod
    def _get_text_widget(widget_metadata: WidgetMetadata) -> Widget:
        widget = Widget(name=widget_metadata.id, textbox_spec=widget_metadata.path.read_text())
        return widget

    def _get_counter_widget(self, widget_metadata: WidgetMetadata) -> Widget:
        fields = self._get_fields(widget_metadata.path.read_text())
        query = Query(dataset_name=widget_metadata.id, fields=fields, disaggregated=True)
        # As far as testing went, a NamedQuery should always have "main_query" as name
        named_query = NamedQuery(name="main_query", query=query)
        spec: WidgetSpec
        if len(fields) == 0:  # Counters are expected to have one field
            counter_encodings = CounterFieldEncoding(field_name=fields[0].name, display_name=fields[0].name)
            spec = CounterSpec(CounterEncodingMap(value=counter_encodings))
        else:
            field_encodings = [RenderFieldEncoding(field_name=field.name) for field in fields]
            table_encodings = TableEncodingMap(field_encodings)
            spec = TableV2Spec(encodings=table_encodings)
        widget = Widget(name=widget_metadata.id, queries=[named_query], spec=spec)
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

    def _get_width_and_height(self, widget: Widget) -> tuple[int, int]:
        """Get the width and height for a widget.

        The tiling logic works if:
        - width < self._MAXIMUM_DASHBOARD_WIDTH : heights for widgets on the same row should be equal
        - width == self._MAXIMUM_DASHBOARD_WIDTH : any height
        """
        if widget.textbox_spec is not None:
            return _MAXIMUM_DASHBOARD_WIDTH, 2

        if isinstance(widget.spec, CounterSpec):
            return 1, 3
        if isinstance(widget.spec, TableV2Spec):
            return self._MAXIMUM_DASHBOARD_WIDTH, 6
        raise NotImplementedError(f"No default width and height defined for spec: {widget.spec}")

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
