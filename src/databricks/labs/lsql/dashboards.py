import argparse
import copy
import dataclasses
import json
import logging
import math
import re
import shlex
from argparse import ArgumentParser
from collections import defaultdict
from collections.abc import Callable, Iterable, Sized
from dataclasses import dataclass
from enum import Enum, unique
from pathlib import Path
from typing import TypeVar

import sqlglot
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard
from databricks.sdk.service.workspace import ExportFormat

from databricks.labs.lsql.lakeview import (
    ColumnType,
    ControlEncoding,
    ControlEncodingMap,
    ControlFieldEncoding,
    CounterEncodingMap,
    CounterFieldEncoding,
    CounterSpec,
    Dashboard,
    Dataset,
    DisplayType,
    Field,
    Layout,
    MultiSelectSpec,
    NamedQuery,
    Page,
    Position,
    Query,
    TableV1ColumnEncoding,
    TableV1EncodingMap,
    TableV1Spec,
    Widget,
    WidgetFrameSpec,
    WidgetSpec,
)

_MAXIMUM_DASHBOARD_WIDTH = 6
T = TypeVar("T")
logger = logging.getLogger(__name__)


class BaseHandler:
    """Base file handler.

    Handlers are based on a Python implementation for FrontMatter.

    Sources:
        https://frontmatter.codes/docs/markdown
        https://github.com/eyeseast/python-frontmatter/blob/main/frontmatter/default_handlers.py
    """

    def __init__(self, path: Path | None) -> None:
        self._path = path

    @property
    def _content(self) -> str:
        if self._path is None:
            return ""
        return self._path.read_text()

    def parse_header(self) -> dict:
        """Parse the header of the file."""
        header, _ = self.split()
        return self._parse_header(header)

    def _parse_header(self, header: str) -> dict:
        _ = self, header
        return {}

    def split(self) -> tuple[str, str]:
        """Split the file header from the content.

        Returns :
            str : The file header possibly containing arguments.
            str : The file contents.
        """
        return "", self._content


class QueryHandler(BaseHandler):
    """Handle query files."""

    @staticmethod
    def _get_arguments_parser() -> ArgumentParser:
        parser = ArgumentParser("TileMetadata", add_help=False, exit_on_error=False)
        parser.add_argument("--id", type=str)
        parser.add_argument("-o", "--order", type=int)
        parser.add_argument("-w", "--width", type=int)
        parser.add_argument("-h", "--height", type=int)
        parser.add_argument("-t", "--title", type=str, default="")
        parser.add_argument("-d", "--description", type=str, default="")
        parser.add_argument(
            "--type",
            type=lambda v: WidgetType(v.upper()),
            default=WidgetType.AUTO,
            help=(
                "The widget type to use, see classes with WidgetSpec as parent class in "
                "databricks.labs.lsql.lakeview.model."
            ),
            dest="widget_type",
        )
        parser.add_argument(
            "-f",
            "--filter",
            type=str,
            action="extend",
            dest="filters",
            nargs="*",
        )
        parser.add_argument(
            "--overrides",
            type=json.loads,
            help="Override the low-level Lakeview entities with a json payload.",
        )
        return parser

    def _parse_header(self, header: str) -> dict:
        """Header is an argparse string."""
        header_split = shlex.split(header)
        parser = self._get_arguments_parser()
        try:
            return vars(parser.parse_args(header_split))
        except (argparse.ArgumentError, SystemExit) as e:
            logger.warning(f"Parsing {self._path}: {e}")
            return vars(parser.parse_known_args(header_split)[0])

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
        return first_comment.strip(), self._content


class MarkdownHandler(BaseHandler):
    """Handle Markdown files."""

    _FRONT_MATTER_BOUNDARY = re.compile(r"^-{3,}\s*$", re.MULTILINE)

    def _parse_header(self, header: str) -> dict:
        """Markdown configuration header is a YAML."""
        _ = self
        return yaml.safe_load(header) or {}

    def split(self) -> tuple[str, str]:
        """Split the markdown file header from the contents.

        The header is enclosed by a horizontal line marked with three dashes '---'.
        """
        splits = self._FRONT_MATTER_BOUNDARY.split(self._content, 2)
        if len(splits) == 3:
            _, header, content = splits
            return header.strip(), content.lstrip()
        if len(splits) == 2:
            logger.warning(f"Parsing {self._path}: Missing closing header boundary.")
        return "", self._content


@unique
class WidgetType(str, Enum):
    """The query widget type"""

    AUTO = "AUTO"
    TABLE = "TABLE"
    COUNTER = "COUNTER"

    def as_widget_spec(self) -> type[WidgetSpec]:
        widget_spec_mapping: dict[str, type[WidgetSpec]] = {
            "TABLE": TableV1Spec,
            "COUNTER": CounterSpec,
        }
        if self.name not in widget_spec_mapping:
            raise ValueError(f"Can not convert to widget spec: {self}")
        return widget_spec_mapping[self.name]


class TileMetadata:
    def __init__(  # pylint: disable=too-many-arguments
        self,
        path: str | Path | None = None,
        order: int | None = None,
        width: int = 0,
        height: int = 0,
        id: str = "",  # pylint: disable=redefined-builtin
        title: str = "",
        description: str = "",
        widget_type: WidgetType = WidgetType.AUTO,
        filters: list[str] | None = None,
        overrides: dict | None = None,
    ):
        self._path = Path(path) if path is not None else None
        self.order = order
        self.width = width
        self.height = height
        self.id = id
        if not self.id:
            self.id = self._path.stem if self._path is not None else ""
        self.title = title
        self.description = description
        self.widget_type = widget_type
        self.filters = filters or []
        self.overrides = overrides or {}

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TileMetadata):
            return False
        return self.as_dict() == other.as_dict()

    def update(self, other: "TileMetadata") -> None:
        """Update the tile metadata with another tile metadata.

        Precendence:
        - The other takes precendences, similar to merging dictionairies.
        - Unless the others value is a default, then the value from self is taken.

        Resources:
        - https://docs.python.org/3/library/stdtypes.html#dict.update : Similar to the update method of a dictionary.
        """
        if not isinstance(other, TileMetadata):
            raise TypeError(f"Can not merge with {other}")

        widget_type = other.widget_type if other.widget_type != WidgetType.AUTO else self.widget_type

        self._path = other._path or self._path
        self.order = other.order or self.order
        self.width = other.width or self.width
        self.height = other.height or self.height
        self.id = other.id or self.id
        self.title = other.title or self.title
        self.description = other.description or self.description
        self.widget_type = widget_type
        self.filters = other.filters or self.filters
        self.overrides = other.overrides or self.overrides

    def is_markdown(self) -> bool:
        return self._path is not None and self._path.suffix == ".md"

    def is_query(self) -> bool:
        return self._path is not None and self._path.suffix == ".sql"

    @property
    def handler(self) -> BaseHandler:
        handler = BaseHandler
        if self.is_markdown():
            handler = MarkdownHandler
        elif self.is_query():
            handler = QueryHandler
        return handler(self._path)

    @classmethod
    def from_dict(cls, raw: dict) -> "TileMetadata":
        return cls(**raw)

    def as_dict(self) -> dict:
        exclude_attributes = {
            "handler",  # Handler is inferred from file extension
            "path",  # Path is set explicitly below
        }
        body = {}
        if self._path is not None:
            body["path"] = self._path.as_posix()
        for attribute in dir(self):
            if attribute.startswith("_"):
                continue
            if callable(getattr(self, attribute)):
                continue
            if attribute in exclude_attributes:
                continue
            value = getattr(self, attribute)
            if value is None or (isinstance(value, Sized) and len(value) == 0):
                continue
            if hasattr(value, "value"):  # For Enums
                value = value.value
            body[attribute] = value
        return body

    @classmethod
    def from_path(cls, path: Path) -> "TileMetadata":
        tile_metadata = cls(path=path)
        header = tile_metadata.handler.parse_header()
        header["path"] = path
        return cls.from_dict(header)

    def __repr__(self):
        return f"TileMetadata<{self._path}>"


@dataclass
class DashboardMetadata:
    display_name: str
    tiles: dict[str, TileMetadata] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict) -> "DashboardMetadata":
        display_name = raw["display_name"]  # Fail early if missing
        tiles, tiles_raw = {}, raw.get("tiles", {})
        for tile_id, tile_raw in tiles_raw.items():
            if not isinstance(tile_raw, dict):
                logger.warning(f"Parsing invalid tile metadata in dashboard.yml: tiles.{tile_id}.{tile_raw}")
                continue
            tile = TileMetadata(id=tile_id)
            for tile_key, tile_value in tile_raw.items():
                if tile_key == "id":
                    logger.warning(f"Parsing unsupported field in dashboard.yml: tiles.{tile_id}.id")
                    continue
                try:
                    tile_new = TileMetadata.from_dict({tile_key: tile_value})
                    tile.update(tile_new)
                except TypeError:
                    logger.warning(f"Parsing unsupported field in dashboard.yml: tiles.{tile_id}.{tile_key}")
                    continue
            tiles[tile.id] = tile
        return cls(display_name=display_name, tiles=tiles)

    def as_dict(self) -> dict:
        raw: dict = {"display_name": self.display_name}
        if self.tiles:
            raw["tiles"] = {tile.id: tile.as_dict() for tile in self.tiles.values()}
        return raw

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


class Tile:
    """A dashboard tile."""

    def __init__(self, tile_metadata: TileMetadata) -> None:
        self._metadata = tile_metadata

        default_width, default_height = self._default_size()
        width = self._metadata.width or default_width
        height = self._metadata.height or default_height
        self.position = Position(0, 0, width, height)

    def _default_size(self) -> tuple[int, int]:
        return 0, 0

    def get_layouts(self) -> Iterable[Layout]:
        """Get the layout(s) reflecting this tile in the dashboard."""
        _, text = self._metadata.handler.split()
        widget = Widget(name=self._metadata.id, textbox_spec=text)
        layout = Layout(widget=widget, position=self.position)
        yield layout

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

    @classmethod
    def from_tile_metadata(cls, tile_metadata: TileMetadata) -> "Tile":
        """Create a tile given the tile metadata."""
        if tile_metadata.is_markdown():
            return MarkdownTile(tile_metadata)
        query_tile = QueryTile(tile_metadata)
        spec_type = query_tile.infer_spec_type()
        if spec_type is None:
            return MarkdownTile(tile_metadata)
        if spec_type == CounterSpec:
            return CounterTile(tile_metadata)
        return TableTile(tile_metadata)

    def __repr__(self):
        return f"Tile<{self._metadata}>"


class MarkdownTile(Tile):
    def _default_size(self) -> tuple[int, int]:
        return _MAXIMUM_DASHBOARD_WIDTH, 3


def replace_database_in_query(node: sqlglot.Expression, *, database: str) -> sqlglot.Expression:
    """Replace the database in a query."""
    if isinstance(node, sqlglot.exp.Table) and node.args.get("db") is not None:
        node.args["db"].set("this", database)
    return node


class QueryTile(Tile):
    _DIALECT = sqlglot.dialects.Databricks
    _FILTER_HEIGHT = 1

    def __init__(
        self,
        tile_metadata: TileMetadata,
        *,
        query_transformer: Callable[[sqlglot.Expression], sqlglot.Expression] | None = None,
    ) -> None:
        super().__init__(tile_metadata)
        self.query_transformer = query_transformer

    def _get_abstract_syntax_tree(self) -> sqlglot.Expression | None:
        _, query = self._metadata.handler.split()
        try:
            return sqlglot.parse_one(query, dialect=self._DIALECT)
        except sqlglot.ParseError as e:
            logger.warning(f"Parsing {query}: {e}")
            return None

    def _get_query(self) -> str:
        _, query = self._metadata.handler.split()
        if self.query_transformer is None:
            return query
        syntax_tree = self._get_abstract_syntax_tree()
        if syntax_tree is None:
            return query
        query_transformed = syntax_tree.transform(self.query_transformer).sql(
            dialect=self._DIALECT,
            # A transformer requires to (re)define how to output SQL
            normalize=True,  # normalize identifiers to lowercase
            pretty=True,  # format the produced SQL string
            normalize_functions="upper",  # normalize function names to uppercase
            max_text_width=80,  # wrap text at 80 characters
        )
        return query_transformed

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
                if named_select == "*":
                    raise NotImplementedError(f"Select with `*`, please define column explicitly: {self}")
                fields.append(field)
        return fields

    def infer_spec_type(self) -> type[WidgetSpec] | None:
        """Infer the spec type from the query."""
        if self._metadata.widget_type != WidgetType.AUTO:
            return self._metadata.widget_type.as_widget_spec()
        fields = self._find_fields()
        if len(fields) == 0:
            return None
        if len(fields) == 1:
            return CounterSpec
        return TableV1Spec

    def get_datasets(self) -> Iterable[Dataset]:
        """Get the dataset belonging to the query."""
        query = self._get_query()
        dataset = Dataset(name=self._metadata.id, display_name=self._metadata.id, query=query)
        yield dataset

    def _merge_nested_dictionaries(self, left: dict, right: dict) -> dict:
        """Merge nested dictionairies."""
        out: dict = defaultdict(dict)
        out.update(left)
        for key, value in right.items():
            if isinstance(value, dict):
                value_merged = self._merge_nested_dictionaries(out[key], value)
                out[key].update(value_merged)
            else:
                out[key] = value
        return dict(out)

    def _merge_widget_with_overrides(self, widget: Widget) -> Widget:
        """Merge the widget with (optional) overrides."""
        if not self._metadata.overrides:
            return widget
        updated = self._merge_nested_dictionaries(widget.as_dict(), self._metadata.overrides)
        widget = widget.from_dict(updated)
        return widget

    def _get_query_layouts(self) -> Iterable[Layout]:
        """Get the layout visualizing the dataset.

        This is the main layout within the tile as it visualizes the dataset.
        """
        fields = self._find_fields()
        query = Query(dataset_name=self._metadata.id, fields=fields, disaggregated=True)
        # As far as testing went, a NamedQuery should always have "main_query" as name
        named_query = NamedQuery(name="main_query", query=query)
        frame = WidgetFrameSpec(
            title=self._metadata.title,
            show_title=len(self._metadata.title) > 0,
            description=self._metadata.description,
            show_description=len(self._metadata.description) > 0,
        )
        spec = self._get_query_widget_spec(fields, frame=frame)
        widget = Widget(name=self._metadata.id, queries=[named_query], spec=spec)
        widget = self._merge_widget_with_overrides(widget)
        height = self.position.height
        if len(self._metadata.filters) > 0 and self.position.width > 0:
            height -= self._FILTER_HEIGHT * math.ceil(len(self._metadata.filters) / self.position.width)
        height = max(height, 0)
        y = self.position.y + self.position.height - height
        position = dataclasses.replace(self.position, y=y, height=height)
        layout = Layout(widget=widget, position=position)
        yield layout

    def _get_filter_widget(self, column: str) -> Widget:
        """Get the widget for visualizing the (optional) filter.

        For a filter, an additional query column implements the filter (see _associativity below). Note that this column
        does not need to be encoded.
        """
        fields = [
            Field(name=column, expression=f"`{column}`"),
            Field(name=f"{column}_associativity", expression="COUNT_IF(`associative_filter_predicate_group`)"),
        ]
        query = Query(dataset_name=self._metadata.id, fields=fields, disaggregated=False)
        named_query = NamedQuery(name=f"filter_{column}", query=query)
        control_encodings: list[ControlEncoding] = [ControlFieldEncoding(column, named_query.name, display_name=column)]
        control_encoding_map = ControlEncodingMap(control_encodings)
        spec = MultiSelectSpec(encodings=control_encoding_map)
        widget = Widget(name=f"{self._metadata.id}_filter_{column}", queries=[named_query], spec=spec)
        return widget

    def _get_filter_positions(self) -> Iterable[Position]:
        """Get the filter positions.

        The positioning of filters works as follows:
        0) Filters fit **within** the tile
        1) Filters are placed above the query widget
        2) Many filters:
           i) fill up a row first, by adjusting their width so that the total width of the filters in a row match the
              width of the tile whilst having a minimum filter width of one
           ii) occupy an additional row if the previous one is filled completely.
        """
        filters_size = len(self._metadata.filters) * self._FILTER_HEIGHT
        if filters_size > self.position.width * (self.position.height - 1):  # At least one row for the query widget
            raise ValueError(f"Too many filters defined for {self}")

        # The bottom row requires bookkeeping to adjust the filters width to fill it completely
        bottom_row_index = len(self._metadata.filters) // self.position.width
        bottom_row_filter_count = len(self._metadata.filters) % self.position.width or self.position.width
        bottom_row_filter_width = self.position.width // bottom_row_filter_count
        bottom_row_remainder_width = self.position.width - bottom_row_filter_width * bottom_row_filter_count

        for filter_index in range(len(self._metadata.filters)):
            if filter_index % self.position.width == 0:
                x_offset = 0  # Reset on new row
            x = self.position.x + x_offset
            y = self.position.y + self._FILTER_HEIGHT * (filter_index // self.position.width)
            width = 1
            if filter_index // self.position.width == bottom_row_index:  # Reached bottom row
                width = bottom_row_filter_width
                if filter_index % self.position.width < bottom_row_remainder_width:
                    width += 1  # Fills up the remainder width if self.position.width % bottom_row_filter_count != 0
            position = Position(x, y, width, self._FILTER_HEIGHT)
            yield position
            x_offset += width

    def _get_filters_layouts(self) -> Iterable[Layout]:
        """Get the layout visualizing the (optional) filters."""
        if len(self._metadata.filters) == 0:
            return
        for filter_index, position in enumerate(self._get_filter_positions()):
            widget = self._get_filter_widget(self._metadata.filters[filter_index])
            layout = Layout(widget=widget, position=position)
            yield layout

    def get_layouts(self) -> Iterable[Layout]:
        """Get the layout(s) reflecting this tile in the dashboard."""
        yield from self._get_query_layouts()
        yield from self._get_filters_layouts()

    @staticmethod
    def _get_query_widget_spec(fields: list[Field], *, frame: WidgetFrameSpec | None = None) -> WidgetSpec:
        """Get the query widget spec.

        In most cases, overwriting this method with a Tile specific spec is sufficient for support other widget types.
        """
        column_encodings = []
        for field in fields:
            column_encoding = TableV1ColumnEncoding(
                boolean_values=["false", "true"],
                display_as=DisplayType.STRING,
                field_name=field.name,
                title=field.name,
                type=ColumnType.STRING,
            )
            column_encodings.append(column_encoding)
        table_encodings = TableV1EncodingMap(column_encodings)
        spec = TableV1Spec(
            allow_html_by_default=False,
            condensed=True,
            encodings=table_encodings,
            invisible_columns=[],
            items_per_page=25,
            frame=frame,
        )
        return spec


class TableTile(QueryTile):
    def _default_size(self) -> tuple[int, int]:
        return 6, 6


class CounterTile(QueryTile):
    def _default_size(self) -> tuple[int, int]:
        return 1, 3

    @staticmethod
    def _get_query_widget_spec(fields: list[Field], *, frame: WidgetFrameSpec | None = None) -> CounterSpec:
        counter_encodings = CounterFieldEncoding(field_name=fields[0].name, display_name=fields[0].name)
        spec = CounterSpec(CounterEncodingMap(value=counter_encodings), frame=frame)
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
                    max_text_width=80,  # wrap text at 80 characters
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
        tiles_metadata = self._parse_tiles_metadata(dashboard_folder, dashboard_metadata)
        tiles = self._get_tiles(tiles_metadata, query_transformer=query_transformer)
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
    def _parse_tiles_metadata(dashboard_folder: Path, dashboard_metadata: DashboardMetadata) -> list[TileMetadata]:
        """Parse the tile metadata from each (optional) header."""
        tiles_metadata = []
        for path in dashboard_folder.iterdir():
            if path.suffix in {".sql", ".md"}:
                tile_metadata = TileMetadata.from_path(path)
                if tile_metadata.id in dashboard_metadata.tiles:
                    # The line below implements the precedence for metadata in the file header over dashboard.yml
                    dashboard_metadata.tiles[tile_metadata.id].update(tile_metadata)
                    tile_metadata = dashboard_metadata.tiles[tile_metadata.id]
                tiles_metadata.append(tile_metadata)
        return tiles_metadata

    @staticmethod
    def _get_tiles(
        tiles_metadata: list[TileMetadata],
        query_transformer: Callable[[sqlglot.Expression], sqlglot.Expression] | None = None,
    ) -> list[Tile]:
        """Create tiles from the tiles metadata.

        The order of the tiles is by default the alphanumerically sorted tile ids, however, the order may be overwritten
        with the `order` key. Hence, the multiple loops to get:
        i) set the order when not specified;
        ii) sort the tiles using the order field.
        """
        tiles_metadata_with_order = []
        for order, tile_metadata in enumerate(sorted(tiles_metadata, key=lambda wm: wm.id)):
            replica = copy.deepcopy(tile_metadata)
            replica.order = tile_metadata.order if tile_metadata.order is not None else order
            tiles_metadata_with_order.append(replica)

        tiles, position = [], Position(0, 0, 0, 0)  # Position of first tile
        for tile_metadata in sorted(tiles_metadata_with_order, key=lambda wm: (wm.order, wm.id)):
            tile = Tile.from_tile_metadata(tile_metadata)
            if isinstance(tile, QueryTile):
                tile.query_transformer = query_transformer
            placed_tile = tile.place_after(position)
            tiles.append(placed_tile)
            position = placed_tile.position
        return tiles

    @staticmethod
    def _get_datasets(tiles: list[Tile]) -> list[Dataset]:
        datasets: list[Dataset] = []
        for tile in tiles:
            if isinstance(tile, QueryTile):
                datasets.extend(tile.get_datasets())
        return datasets

    @staticmethod
    def _get_layouts(tiles: list[Tile]) -> list[Layout]:
        """Create layouts from the tiles."""
        layouts: list[Layout] = []
        for tile in tiles:
            layouts.extend(tile.get_layouts())
        return layouts

    def deploy_dashboard(
        self,
        lakeview_dashboard: Dashboard,
        *,
        parent_path: str | None = None,
        dashboard_id: str | None = None,
        warehouse_id: str | None = None,
    ) -> SDKDashboard:
        """Deploy a lakeview dashboard."""
        serialized_dashboard = json.dumps(lakeview_dashboard.as_dict())
        display_name = lakeview_dashboard.pages[0].display_name or lakeview_dashboard.pages[0].name
        if dashboard_id is not None:
            dashboard = self._ws.lakeview.update(
                dashboard_id,
                display_name=display_name,
                serialized_dashboard=serialized_dashboard,
                warehouse_id=warehouse_id,
            )
        else:
            dashboard = self._ws.lakeview.create(
                display_name,
                parent_path=parent_path,
                serialized_dashboard=serialized_dashboard,
                warehouse_id=warehouse_id,
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
