import argparse
import copy
import dataclasses
import json
import logging
import re
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

    @classmethod
    def from_path(cls, path: Path) -> "DashboardMetadata":
        """Export dashboard metadata from a YAML file."""
        fallback_metadata = cls(display_name=path.parent.name)

        if not path.exists():
            return fallback_metadata

        try:
            raw = yaml.safe_load(path.read_text())
        except yaml.YAMLError as e:
            logger.warning(f"Parsing {path}: {e}")
            return fallback_metadata
        try:
            return cls.from_dict(raw)
        except KeyError as e:
            logger.warning(f"Parsing {path}: {e}")
            return fallback_metadata


class BaseHandler:
    """Base file handler.

    Handlers are based on a Python implementation for FrontMatter.

    Sources:
        https://frontmatter.codes/docs/markdown
        https://github.com/eyeseast/python-frontmatter/blob/main/frontmatter/default_handlers.py
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def _content(self) -> str:
        return self._path.read_text()

    def parse_header(self) -> dict[str, str]:
        """Parse the header of the file."""
        header, _ = self.split()
        return self._parse_header(header)

    def _parse_header(self, header: str) -> dict[str, str]:
        _ = self, header
        return {}

    def split(self) -> tuple[str, str]:
        """Split the file header from the content.

        Returns
            str : The file header possibly containing arguments.
            str : The file contents.
        """
        return "", self._content


class QueryHandler(BaseHandler):
    """Handle query files."""

    @staticmethod
    def _get_arguments_parser() -> ArgumentParser:
        parser = ArgumentParser("WidgetMetadata", add_help=False, exit_on_error=False)
        parser.add_argument("--id", type=str)
        parser.add_argument("-o", "--order", type=int)
        parser.add_argument("-w", "--width", type=int)
        parser.add_argument("-h", "--height", type=int)
        return parser

    def _parse_header(self, header: str) -> dict[str, str]:
        """Header is an argparse string."""
        parser = self._get_arguments_parser()
        try:
            return vars(parser.parse_args(shlex.split(header)))
        except (argparse.ArgumentError, SystemExit) as e:
            logger.warning(f"Parsing {self._path}: {e}")
            return {}

    def split(self) -> tuple[str, str]:
        """Split the query file header from the contents.

        The optional header is the first comment at the top of the file.
        """
        try:
            parsed_query = sqlglot.parse_one(self._content, dialect=sqlglot.dialects.Databricks)
        except sqlglot.ParseError as e:
            logger.warning(f"Parsing {self._path}: {e}")
            return "", self._content

        if parsed_query.comments is None or len(parsed_query.comments) == 0:
            return "", self._content

        first_comment = parsed_query.comments[0]
        return first_comment, self._content


class MarkdownHandler(BaseHandler):
    """Handle Markdown files."""

    _FRONT_MATTER_BOUNDARY = re.compile(r"^-{3,}\s*$", re.MULTILINE)

    def _parse_header(self, header: str) -> dict[str, str]:
        """Markdown frontmatter header is a YAML."""
        _ = self
        return yaml.safe_load(header) or {}

    def split(self) -> tuple[str, str]:
        """Split the markdown file header from the contents.

        The header is enclosed by the boundaries (line with '---').
        """
        splits = self._FRONT_MATTER_BOUNDARY.split(self._content, 2)
        if len(splits) == 3:
            _, header, content = splits
            return header, content
        if len(splits) == 1:
            logger.warning(f"Parsing {self._path}: Missing closing header boundary.")
        return "", self._content


class WidgetMetadata:
    def __init__(
        self,
        path: Path,
        order: int | None = None,
        width: int = 0,
        height: int = 0,
        _id: str = "",
    ):
        self._path = path
        self.order = order
        self.width = width
        self.height = height
        self.id = _id or path.stem

        size = self._size
        self.width = self.width or size[0]
        self.height = self.height or size[1]
        self.id = self.id or path.stem

    def is_markdown(self) -> bool:
        return self._path.suffix == ".md"

    def is_query(self) -> bool:
        return self._path.suffix == ".sql"

    @property
    def handler(self) -> BaseHandler:
        handler = BaseHandler
        if self.is_markdown():
            handler = MarkdownHandler
        elif self.is_query():
            handler = QueryHandler
        return handler(self._path)

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

    @classmethod
    def from_dict(cls, path: str | Path, **optionals) -> "WidgetMetadata":
        path = Path(path)
        if "id" in optionals:
            optionals["_id"] = optionals["id"]
            del optionals["id"]
        return cls(path, **optionals)

    def as_dict(self) -> dict[str, str]:
        body = {"path": self._path.as_posix()}
        for attribute in "order", "width", "height", "id":
            if attribute in body:
                continue
            value = getattr(self, attribute)
            if value is not None:
                body[attribute] = str(value)
        return body

    def size(self) -> tuple[int, int]:
        return self.width, self.height

    def is_markdown(self) -> bool:
        return self.path.suffix == ".md"

    def is_query(self) -> bool:
        return self.path.suffix == ".sql"

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

        replica.order = args.order if args.order is not None else self.order
        replica.width = args.width or self.width
        replica.height = args.height or self.height
        replica.id = args.id or self.id
        return replica

    @classmethod
    def from_path(cls, path: Path) -> "WidgetMetadata":
        widget_metadata = cls(path=path)
        header = widget_metadata.handler.parse_header()
        return cls.from_dict(path, **header)

    def __repr__(self):
        return f"WidgetMetdata<{self._path}>"


class Tile:
    """A dashboard tile."""

    def __init__(self, widget_metadata: WidgetMetadata) -> None:
        self._widget_metadata = widget_metadata

        default_width, default_height = self._default_size()
        width = self._widget_metadata.width or default_width
        height = self._widget_metadata.height or default_height
        self.position = Position(0, 0, width, height)

    def _default_size(self) -> tuple[int, int]:
        return 0, 0

    def place_after(self, position: Position) -> "Tile":
        """Place the tile after another tile:

        The tiling logic works if:
        - `position.width < _MAXIMUM_DASHBOARD_WIDTH` : tiles in a single row should have the same size
        - `position.width == _MAXIMUM_DASHBOARD_WIDTH` : any height
        """
        x = position.x + position.width
        if x + self.position.width > _MAXIMUM_DASHBOARD_WIDTH:
            x = 0
            y = position.y + position.height
        else:
            y = position.y
        new_position = dataclasses.replace(self.position, x=x, y=y)

        replica = copy.deepcopy(self)
        replica.position = new_position
        return replica

    @property
    def widget(self) -> Widget:
        widget = Widget(name=self._widget_metadata.id, textbox_spec=self._widget_metadata.path.read_text())
        return widget

    @classmethod
    def from_widget_metadata(cls, widget_metadata: WidgetMetadata) -> "Tile":
        """Create a tile given the widget metadata."""
        if widget_metadata.is_markdown():
            return MarkdownTile(widget_metadata)
        query_tile = QueryTile(widget_metadata)
        spec_type = query_tile.infer_spec_type()
        if spec_type is None:
            return MarkdownTile(widget_metadata)
        if spec_type == CounterSpec:
            return CounterTile(widget_metadata)
        return TableTile(widget_metadata)


class MarkdownTile(Tile):
    def _default_size(self) -> tuple[int, int]:
        return _MAXIMUM_DASHBOARD_WIDTH, 2


class QueryTile(Tile):
    def _get_abstract_syntax_tree(self) -> sqlglot.Expression | None:
        query = self._widget_metadata.path.read_text()
        try:
            return sqlglot.parse_one(query, dialect=sqlglot.dialects.Databricks)
        except sqlglot.ParseError as e:
            logger.warning(f"Parsing {query}: {e}")
            return None

    def _find_fields(self) -> list[Field]:
        """Find the fields in a query.

        The fields are the projections in the query's top level SELECT.
        """
        abstract_syntax_tree = self._get_abstract_syntax_tree()
        if abstract_syntax_tree is None:
            return []

        fields = []
        for projection in abstract_syntax_tree.find_all(sqlglot.exp.Select):
            if projection.depth > 0:
                continue
            for named_select in projection.named_selects:
                field = Field(name=named_select, expression=f"`{named_select}`")
                fields.append(field)
        return fields

    @property
    def widget(self) -> Widget:
        fields = self._find_fields()
        named_query = self._get_named_query(fields)
        spec = self._get_spec(fields)
        widget = Widget(name=self._widget_metadata.id, queries=[named_query], spec=spec)
        return widget

    def _get_named_query(self, fields: list[Field]) -> NamedQuery:
        query = Query(dataset_name=self._widget_metadata.id, fields=fields, disaggregated=True)
        # As far as testing went, a NamedQuery should always have "main_query" as name
        named_query = NamedQuery(name="main_query", query=query)
        return named_query

    @staticmethod
    def _get_spec(fields: list[Field]) -> WidgetSpec:
        field_encodings = [RenderFieldEncoding(field_name=field.name) for field in fields]
        table_encodings = TableEncodingMap(field_encodings)
        spec = TableV2Spec(encodings=table_encodings)
        return spec

    def infer_spec_type(self) -> type[WidgetSpec] | None:
        """Infer the spec type from the query."""
        fields = self._find_fields()
        if len(fields) == 0:
            return None
        if len(fields) == 1:
            return CounterSpec
        return TableV2Spec


class TableTile(QueryTile):
    def _default_size(self) -> tuple[int, int]:
        return 6, 6


class CounterTile(QueryTile):
    def _default_size(self) -> tuple[int, int]:
        return 1, 3

    @staticmethod
    def _get_spec(fields: list[Field]) -> CounterSpec:
        counter_encodings = CounterFieldEncoding(field_name=fields[0].name, display_name=fields[0].name)
        spec = CounterSpec(CounterEncodingMap(value=counter_encodings))
        return spec


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
        dashboard_metadata = DashboardMetadata.from_path(dashboard_folder / "dashboard.yml")
        widgets_metadata = self._parse_widgets_metadata(dashboard_folder)
        datasets = self._get_datasets(dashboard_folder)
        layouts = self._get_layouts(widgets_metadata)
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
    def _get_datasets(dashboard_folder: Path) -> list[Dataset]:
        datasets = []
        for query_path in sorted(dashboard_folder.glob("*.sql")):
            dataset = Dataset(name=query_path.stem, display_name=query_path.stem, query=query_path.read_text())
            datasets.append(dataset)
        return datasets

    @staticmethod
    def _get_layouts(widgets_metadata: list[WidgetMetadata]) -> list[Layout]:
        """Create layouts from the widgets metadata.

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

        layouts, position = [], Position(0, 0, 0, 0)  # Position of first tile
        for widget_metadata in sorted(widgets_metadata_with_order, key=lambda wm: (wm.order, wm.id)):
            tile = Tile.from_widget_metadata(widget_metadata)
            placed_tile = tile.place_after(position)
            layout = Layout(widget=placed_tile.widget, position=placed_tile.position)
            layouts.append(layout)
            position = placed_tile.position
        return layouts

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
