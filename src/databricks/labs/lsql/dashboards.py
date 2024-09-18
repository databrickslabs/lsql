import argparse
import collections
import csv
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
from io import StringIO
from pathlib import Path
from typing import TypeVar
from zipfile import ZipFile

import sqlglot
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard
from databricks.sdk.service.workspace import ExportFormat

from databricks.labs.lsql.backends import SqlBackend
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
    DateRangePickerSpec,
    DisplayType,
    DropdownSpec,
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
_SQL_DIALECT = sqlglot.dialects.Databricks
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
        parser.add_argument("-w", "--width", type=int, default=0)
        parser.add_argument("-h", "--height", type=int, default=0)
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
            default=[],
        )
        parser.add_argument(
            "--overrides",
            type=json.loads,
            help="Override the low-level Lakeview entities with a json payload.",
            default={},
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

        The optional header is the first comment at the top of the file or above the first SELECT below the CTEs.
        """
        comments = self._find_comments()
        if len(comments) == 0:
            return "", self._content
        return comments[0].strip(), self._content

    def _find_comments(self) -> list[str]:
        """Find the comments in a query."""
        try:
            parsed_query = sqlglot.parse_one(self._content, dialect=_SQL_DIALECT)
        except sqlglot.ParseError as e:
            logger.warning(f"Parsing {self._path}: {e}")
            return []
        comments = parsed_query.comments or []
        # The comments might be above a CTE's, for example, after formatting a query
        # https://github.com/tobymao/sqlglot/issues/3810
        with_expression = parsed_query.find(sqlglot.exp.With)
        if with_expression is not None:
            comments.extend(with_expression.comments or [])
        return comments


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


class FilterHandler(BaseHandler):
    """Handle filter files."""

    def _parse_header(self, header: str) -> dict:
        if not header:
            return {}
        metadata = yaml.safe_load(header) or {}
        # The user can either provide a single filter column as a string or a list of filter columns
        # Only one of column or columns should be set
        filter_col = metadata.pop("column", None)
        filter_cols = metadata.pop("columns", None)
        if not filter_col and not filter_cols:
            raise ValueError(f"Neither column nor columns set in {self._path}")
        if filter_col and filter_cols:
            raise ValueError(f"Both column and columns set in {self._path}")
        # If a single column is provided, convert it to a list of one column
        # Please note that column/columns key in .filter.yml files are mapped to the filters key in the TileMetadata
        metadata["filters"] = [filter_col] if filter_col else filter_cols
        metadata["widget_type"] = WidgetType(metadata.pop("type", "DROPDOWN").upper())
        return metadata

    def split(self) -> tuple[str, str]:
        trimmed_content = self._content.strip()
        return trimmed_content, ""


@unique
class WidgetType(str, Enum):
    """The query widget type"""

    AUTO = "AUTO"
    TABLE = "TABLE"
    COUNTER = "COUNTER"
    DATE_RANGE_PICKER = "DATE_RANGE_PICKER"
    MULTI_SELECT = "MULTI_SELECT"
    DROPDOWN = "DROPDOWN"

    def as_widget_spec(self) -> type[WidgetSpec]:
        widget_spec_mapping: dict[str, type[WidgetSpec]] = {
            "TABLE": TableV1Spec,
            "COUNTER": CounterSpec,
            "DATE_RANGE_PICKER": DateRangePickerSpec,
            "MULTI_SELECT": MultiSelectSpec,
            "DROPDOWN": DropdownSpec,
        }
        if self.name not in widget_spec_mapping:
            raise ValueError(f"Can not convert to widget spec: {self}")
        return widget_spec_mapping[self.name]


@dataclass
class TileMetadata:
    """The metadata defining a :class:Tile"""

    path: Path | None = None
    order: int | None = None
    width: int = 0
    height: int = 0
    id: str = ""
    title: str = ""
    description: str = ""
    widget_type: WidgetType = WidgetType.AUTO
    filters: list[str] = dataclasses.field(default_factory=list)
    overrides: dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = self.path.stem if self.path is not None else ""

    def merge(self, other: "TileMetadata") -> "TileMetadata":
        """Merge the tile metadata with another tile metadata.

        Precedence:
        - The other takes precedences, similar to merging dictionaries.
        - Unless the others value is a default, then the value from self is taken.

        Resources:
        - https://docs.python.org/3/library/stdtypes.html#dict.update : Similar to the update method of a dictionary.
        """
        if not isinstance(other, TileMetadata):
            raise TypeError(f"Can not merge with {other}")

        widget_type = other.widget_type if other.widget_type != WidgetType.AUTO else self.widget_type

        merged = dataclasses.replace(
            self,
            path=other.path or self.path,
            order=other.order if other.order is not None else self.order,
            width=other.width or self.width,
            height=other.height or self.height,
            id=other.id or self.id,
            title=other.title or self.title,
            description=other.description or self.description,
            widget_type=widget_type,
            filters=other.filters or self.filters,
            overrides=other.overrides or self.overrides,
        )
        return merged

    def is_markdown(self) -> bool:
        return self.path is not None and self.path.suffix == ".md"

    def is_query(self) -> bool:
        return self.path is not None and self.path.suffix == ".sql"

    def is_filter(self) -> bool:
        return self.path is not None and self.path.name.endswith(".filter.yml")

    @property
    def handler(self) -> BaseHandler:
        handler = BaseHandler
        if self.is_markdown():
            handler = MarkdownHandler
        elif self.is_query():
            handler = QueryHandler
        elif self.is_filter():
            handler = FilterHandler
        return handler(self.path)

    @classmethod
    def from_dict(cls, raw: dict) -> "TileMetadata":
        path = raw.pop("path", None)
        path = Path(path) if path is not None else None
        return cls(path=path, **raw)

    def as_dict(self) -> dict:
        exclude_attributes = {
            "handler",  # Handler is inferred from file extension
            "path",  # Path is set explicitly below
        }
        body = {}
        if self.path is not None:
            body["path"] = self.path.as_posix()
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
        return f"TileMetadata<{self.id}>"


@dataclass
class Tile:
    """A dashboard tile."""

    metadata: TileMetadata

    _content: str = ""
    _position: Position = dataclasses.field(default_factory=lambda: Position(0, 0, 0, 0))

    @property
    def content(self) -> str:
        if len(self._content) == 0:
            _, content = self.metadata.handler.split()
            self._content = content
        return self._content

    @property
    def position(self) -> Position:
        width = self.metadata.width or self._position.width
        height = self.metadata.height or self._position.height
        return Position(self._position.x, self._position.y, width, height)

    def validate(self) -> None:
        """Validate the tile

        Raises:
            ValueError : If the tile is invalid.
        """
        if len(self.content) == 0:
            raise ValueError(f"Tile has empty content: {self}")

    def get_layouts(self, dashboard_metadata: "DashboardMetadata") -> Iterable[Layout]:
        """Get the layout(s) reflecting this tile in the dashboard."""
        _ = dashboard_metadata  # Not using dashboard_metadata for default implementation
        widget = Widget(name=f"{self.metadata.id}_widget", textbox_spec=self.content)
        layout = Layout(widget=widget, position=self.position)
        yield layout

    def place_after(self, position: Position) -> None:
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
        self._position = dataclasses.replace(self.position, x=x, y=y)

    @classmethod
    def from_tile_metadata(cls, tile_metadata: TileMetadata) -> "Tile":
        """Create a tile given the tile metadata."""
        if tile_metadata.is_markdown():
            return MarkdownTile(tile_metadata)
        if tile_metadata.is_filter():
            return FilterTile(tile_metadata)
        query_tile = QueryTile(tile_metadata)
        spec_type = query_tile.infer_spec_type()
        if spec_type is None:
            return MarkdownTile(tile_metadata)
        if spec_type == CounterSpec:
            return CounterTile(tile_metadata)
        return TableTile(tile_metadata)

    def __repr__(self):
        return f"Tile<{self.metadata.id}>"


@dataclass
class MarkdownTile(Tile):
    _position: Position = dataclasses.field(default_factory=lambda: Position(0, 0, _MAXIMUM_DASHBOARD_WIDTH, 3))

    def validate(self) -> None:
        """Validate the tile

        Raises:
            ValueError : If the tile is invalid.
        """
        super().validate()
        if not self.metadata.is_markdown():
            raise ValueError(f"Tile is not a markdown file: {self}")


@dataclass
class QueryTile(Tile):
    """A tile based on a sql query."""

    query_transformer: Callable[[sqlglot.Expression], sqlglot.Expression] | None = None

    _FILTER_HEIGHT = 1

    def validate(self) -> None:
        """Validate the tile

        Raises:
            ValueError : If the tile is invalid.
        """
        super().validate()
        if not self.metadata.is_query():
            raise ValueError(f"Tile is not a query file: {self}")
        try:
            sqlglot.parse_one(self.content, dialect=_SQL_DIALECT)
        except sqlglot.ParseError as e:
            raise ValueError(f"Invalid query content: {self.content}") from e

    @staticmethod
    def format(content: str, *, max_text_width: int = 120, normalize_case: bool = True) -> str:
        """Format the content

        Args:
            content : str
                The content to format
            max_text_width : int, optional (default: 120)
                The maximum text width to wrap at
            normalize_case : bool, optional (default: True)
                If the query identifiers should be normalized to lower case
        """
        has_eol = content.endswith("\n")
        try:
            parsed_query = sqlglot.parse(content, dialect=_SQL_DIALECT)
        except sqlglot.ParseError:
            return content
        statements = []
        for statement in parsed_query:
            if statement is None:
                continue
            # TODO: CASE .. WHEN .. THEN .. formatting is a bit less readable after reformatting.
            # See https://github.com/tobymao/sqlglot/issues/3770
            # see https://sqlglot.com/sqlglot/generator.html#Generator
            statements.append(
                statement.sql(
                    dialect=_SQL_DIALECT,
                    normalize=normalize_case,  # normalize identifiers to lowercase
                    pretty=True,  # format the produced SQL string
                    normalize_functions="upper",  # normalize function names to uppercase
                    max_text_width=max_text_width,  # wrap text at 120 characters
                )
            )
        formatted_query = ";\n".join(statements)
        if "$" in content:
            # replace ${x} with $x, because we use it in UCX view definitions for now
            formatted_query = re.sub(r"\${(\w+)}", r"$\1", formatted_query)
        return formatted_query + ("\n" if has_eol else "")

    def _get_abstract_syntax_tree(self) -> sqlglot.Expression | None:
        try:
            return sqlglot.parse_one(self.content, dialect=_SQL_DIALECT)
        except sqlglot.ParseError as e:
            logger.warning(f"Parsing {self.content}: {e}")
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
                if named_select == "*":
                    raise NotImplementedError(f"Select with `*`, please define column explicitly: {self}")
                fields.append(field)
        return fields

    def replace_database(
        self,
        catalog: str | None = None,
        database: str | None = None,
        *,
        catalog_to_replace: str | None = None,
        database_to_replace: str | None = None,
    ) -> "QueryTile":
        """Replace the catalog and/or database in the query.

        Parameters :
            catalog : str
                The value to replace the catalog with
            database : str
                The value to replace the database with
            catalog_to_replace : str | None (default: None)
                The catalog to replace, if None, all catalogs are replaced
            database_to_replace : str | None (default: None)
                The database to replace, if None, all databases are replaced

        Source :
            https://sqlglot.com/sqlglot/transforms.html
        """

        def replace_catalog_and_database_in_query(node: sqlglot.Expression) -> sqlglot.Expression:
            if isinstance(node, sqlglot.exp.Table):
                if (
                    node.args.get("catalog") is not None
                    and catalog is not None
                    and (
                        catalog_to_replace is None
                        or getattr(node.args.get("catalog"), "this", "") == catalog_to_replace
                    )
                ):
                    node.args["catalog"].set("this", catalog)
                if (
                    node.args.get("db") is not None
                    and database is not None
                    and (database_to_replace is None or getattr(node.args.get("db"), "this", "") == database_to_replace)
                ):
                    node.args["db"].set("this", database)
            return node

        syntax_tree = self._get_abstract_syntax_tree()
        if syntax_tree is None:
            return dataclasses.replace(self, _content=self.content)
        content_transformed = syntax_tree.transform(replace_catalog_and_database_in_query).sql(
            dialect=_SQL_DIALECT,
            # A transformer requires to (re)define how to output SQL
            normalize=True,  # normalize identifiers to lowercase
            pretty=True,  # format the produced SQL string
            normalize_functions="upper",  # normalize function names to uppercase
            max_text_width=80,  # wrap text at 80 characters
        )
        return dataclasses.replace(self, _content=content_transformed)

    def infer_spec_type(self) -> type[WidgetSpec] | None:
        """Infer the spec type from the query."""
        if self.metadata.widget_type != WidgetType.AUTO:
            return self.metadata.widget_type.as_widget_spec()
        fields = self._find_fields()
        if len(fields) == 0:
            return None
        if len(fields) == 1:
            return CounterSpec
        return TableV1Spec

    def get_datasets(self) -> Iterable[Dataset]:
        """Get the dataset belonging to the query."""
        dataset = Dataset(name=self.metadata.id, display_name=self.metadata.id, query=self.content)
        yield dataset

    def _merge_nested_dictionaries(self, left: dict, right: dict) -> dict:
        """Merge nested dictionaries."""
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
        if not self.metadata.overrides:
            return widget
        updated = self._merge_nested_dictionaries(widget.as_dict(), self.metadata.overrides)
        widget = widget.from_dict(updated)
        return widget

    def _get_query_layouts(self) -> Iterable[Layout]:
        """Get the layout visualizing the dataset.

        This is the main layout within the tile as it visualizes the dataset.
        """
        fields = self._find_fields()
        query = Query(dataset_name=self.metadata.id, fields=fields, disaggregated=True)
        # As far as testing went, a NamedQuery should always have "main_query" as name
        named_query = NamedQuery(name="main_query", query=query)
        frame = WidgetFrameSpec(
            title=self.metadata.title,
            show_title=len(self.metadata.title) > 0,
            description=self.metadata.description,
            show_description=len(self.metadata.description) > 0,
        )
        spec = self._get_query_widget_spec(fields, frame=frame)
        widget = Widget(name=f"{self.metadata.id}_widget", queries=[named_query], spec=spec)
        widget = self._merge_widget_with_overrides(widget)
        height = self.position.height
        if len(self.metadata.filters) > 0 and self.position.width > 0:
            height -= self._FILTER_HEIGHT * math.ceil(len(self.metadata.filters) / self.position.width)
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
        query = Query(dataset_name=self.metadata.id, fields=fields, disaggregated=False)
        named_query = NamedQuery(name=f"filter_{column}", query=query)
        control_encodings: list[ControlEncoding] = [ControlFieldEncoding(column, named_query.name, display_name=column)]
        control_encoding_map = ControlEncodingMap(control_encodings)
        spec = MultiSelectSpec(encodings=control_encoding_map)
        widget = Widget(name=f"{self.metadata.id}_filter_{column}", queries=[named_query], spec=spec)
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
        filters_size = len(self.metadata.filters) * self._FILTER_HEIGHT
        if filters_size > self.position.width * (self.position.height - 1):  # At least one row for the query widget
            raise ValueError(f"Too many filters defined for {self}")

        # The bottom row requires bookkeeping to adjust the filters width to fill it completely
        bottom_row_index = len(self.metadata.filters) // self.position.width
        bottom_row_filter_count = len(self.metadata.filters) % self.position.width or self.position.width
        bottom_row_filter_width = self.position.width // bottom_row_filter_count
        bottom_row_remainder_width = self.position.width - bottom_row_filter_width * bottom_row_filter_count

        for filter_index in range(len(self.metadata.filters)):
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
        if len(self.metadata.filters) == 0:
            return
        for filter_index, position in enumerate(self._get_filter_positions()):
            widget = self._get_filter_widget(self.metadata.filters[filter_index])
            layout = Layout(widget=widget, position=position)
            yield layout

    def get_layouts(self, _) -> Iterable[Layout]:
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


@dataclass
class TableTile(QueryTile):
    _position: Position = dataclasses.field(default_factory=lambda: Position(0, 0, 3, 6))

    @property
    def position(self) -> Position:
        if self.metadata.width:
            width = self.metadata.width
        else:
            fields = self._find_fields()
            width = max(self._position.width, len(fields) // 3)
        width = min(width, _MAXIMUM_DASHBOARD_WIDTH)
        height = self.metadata.height or self._position.height
        return Position(self._position.x, self._position.y, width, height)


@dataclass
class CounterTile(QueryTile):
    _position: Position = dataclasses.field(default_factory=lambda: Position(0, 0, 1, 3))

    @staticmethod
    def _get_query_widget_spec(fields: list[Field], *, frame: WidgetFrameSpec | None = None) -> CounterSpec:
        counter_encodings = CounterFieldEncoding(field_name=fields[0].name, display_name=fields[0].name)
        spec = CounterSpec(CounterEncodingMap(value=counter_encodings), frame=frame)
        return spec


@dataclass
class FilterTile(Tile):
    _position: Position = dataclasses.field(default_factory=lambda: Position(0, 0, 3, 2))

    def validate(self) -> None:
        """Validate the tile

        Raises:
            ValueError : If the tile is invalid.
        """
        if not self.metadata.is_filter():
            raise ValueError(f"Tile is not a filter file: {self}")
        if len(self.metadata.filters) == 0:
            raise ValueError(f"Filter tile has no filters defined: {self}")
        if self.metadata.widget_type not in {
            WidgetType.MULTI_SELECT,
            WidgetType.DATE_RANGE_PICKER,
            WidgetType.DROPDOWN,
        }:
            raise ValueError(f"Filter tile has an invalid widget type: {self}")

    def get_layouts(self, dashboard_metadata: "DashboardMetadata") -> Iterable[Layout]:
        """Get the layout(s) reflecting this tile in the dashboard."""
        datasets = dashboard_metadata.get_datasets()
        widget = self._create_widget(datasets)
        layout = Layout(widget=widget, position=self.position)
        yield layout

    def _create_widget(self, datasets: list[Dataset]) -> Widget:
        dataset_columns = self._get_dataset_columns(datasets)
        # This method is called during get layouts.
        # Metadata validation is done before getting the layouts.
        # That's why dataset_columns is not being validated during metadata validation.
        if len(dataset_columns) == 0:
            err_msg = f"Filter tile has no matching dataset columns: {self}"
            raise ValueError(err_msg)

        filter_type = self.metadata.widget_type
        return self._create_filter_widget(dataset_columns, filter_type.as_widget_spec())

    def _get_dataset_columns(self, datasets: list[Dataset]) -> set[tuple[str, str]]:
        """Get the filter column and dataset name pairs."""
        dataset_columns = set()
        for dataset in datasets:
            for field in self._find_filter_fields(dataset.query):
                dataset_columns.add((field.name, dataset.name))
        return dataset_columns

    def _find_filter_fields(self, query: str) -> list[Field]:
        """Find the fields in a query matching the filter names.
        The fields are the projections in the query's top level SELECT.
        """
        try:
            abstract_syntax_tree = sqlglot.parse_one(query, dialect=_SQL_DIALECT)
        except sqlglot.ParseError as e:
            logger.warning(f"Error while parsing {query}: {e}")
            return []
        filters = {name.lower() for name in self.metadata.filters}
        filter_fields = []
        for projection in abstract_syntax_tree.find_all(sqlglot.exp.Select):
            if projection.depth > 0:
                continue
            for named_select in projection.named_selects:
                if named_select.lower() not in filters:
                    continue
                field = Field(name=named_select, expression=f"`{named_select}`")
                filter_fields.append(field)
        return filter_fields

    def _create_filter_widget(
        self,
        dataset_columns: set[tuple[str, str]],
        spec_type,
    ) -> Widget:
        frame = self._create_widget_frame()
        control_encodings, queries = self._generate_filter_encodings_and_queries(dataset_columns)
        control_encoding_map = ControlEncodingMap(control_encodings)
        spec = spec_type(encodings=control_encoding_map, frame=frame)
        widget = Widget(name=f"{self.metadata.id}_widget", queries=queries, spec=spec)
        return widget

    def _create_widget_frame(self) -> WidgetFrameSpec:
        return WidgetFrameSpec(
            title=self.metadata.title,
            show_title=len(self.metadata.title) > 0,
            description=self.metadata.description,
            show_description=len(self.metadata.description) > 0,
        )

    def _generate_filter_encodings_and_queries(
        self, dataset_columns: set[tuple[str, str]]
    ) -> tuple[list[ControlEncoding], list[NamedQuery]]:
        encodings: list[ControlEncoding] = []
        queries = []

        for column, dataset_name in dataset_columns:
            fields = [
                Field(name=column, expression=f"`{column}`"),
                Field(name=f"{column}_associativity", expression="COUNT_IF(`associative_filter_predicate_group`)"),
            ]
            query = Query(dataset_name=dataset_name, fields=fields, disaggregated=False)
            named_query = NamedQuery(name=f"{self.metadata.widget_type}_{dataset_name}_{column}", query=query)
            queries.append(named_query)
            control_encoding = ControlFieldEncoding(column, named_query.name, display_name=column)
            encodings.append(control_encoding)

        return encodings, queries


@dataclass
class DashboardMetadata:
    """The metadata defining a lakeview dashboard"""

    display_name: str  # The dashboard display name

    _tiles: list[Tile] = dataclasses.field(default_factory=list)  # The dashboard tiles

    @property
    def tiles(self) -> list[Tile]:
        """The ordered dashboard tiles

        The order of the tiles is by default the alphanumerically sorted tile ids, however, the order may be overwritten
        with the `order` key. Hence, the logic to:
        i) set the order when not specified;
        ii) sort the tiles using the order field.
        """
        tiles_with_order = []
        for default_order, tile in enumerate(sorted(self._tiles, key=lambda tile: tile.metadata.id)):
            order = tile.metadata.order if tile.metadata.order is not None else default_order
            tiles_with_order.append((order, tile))
        tiles, position = [], Position(0, 0, 0, 0)  # Position of first tile
        for _, tile in sorted(tiles_with_order, key=lambda el: (el[0], el[1].metadata.id)):
            tile.place_after(position)
            tiles.append(tile)
            position = tile.position
        return tiles

    def validate(self) -> None:
        """Validate the dashboard metadata.

        Raises:
            ValueError : If the dashboard metadata is invalid.
        """
        tile_ids = []
        for tile in self.tiles:
            tile.validate()
            tile_ids.append(tile.metadata.id)
        counter = collections.Counter(tile_ids)
        for tile_id, id_count in counter.items():
            if id_count > 1:
                raise ValueError(f"Duplicate id: {tile_id}")

    @classmethod
    def _merge_tile_metadatas(cls, left: list[TileMetadata], right: list[TileMetadata]) -> list[TileMetadata]:
        """Merge tile metdatas where the right takes precedence over the left."""
        metadata_mapping_left = {metadata.id: metadata for metadata in left}
        metadata_mapping_right = {metadata.id: metadata for metadata in right}
        metadatas = []
        for metadata in right:
            metadata_existing = metadata_mapping_left.get(metadata.id)
            if metadata_existing is not None:
                metadata_merged = metadata_existing.merge(metadata)
                metadatas.append(metadata_merged)
            else:
                metadatas.append(metadata)
        for metadata in left:
            if metadata.id not in metadata_mapping_right:
                metadatas.append(metadata)
        return metadatas

    def replace_database(self, *args, **kwargs) -> "DashboardMetadata":
        """Wrapper around :method:QueryTile.replace_database"""
        tiles: list[Tile] = []
        for tile in self.tiles:
            if isinstance(tile, QueryTile):
                tiles.append(tile.replace_database(*args, **kwargs))
            else:
                tiles.append(tile)
        return dataclasses.replace(self, _tiles=tiles)

    def get_datasets(self) -> list[Dataset]:
        """Get the datasets for the dashboard."""
        datasets: list[Dataset] = []
        for tile in self.tiles:
            if isinstance(tile, QueryTile):
                datasets.extend(tile.get_datasets())
        return datasets

    def _get_layouts(self) -> list[Layout]:
        """Get the layouts for the dashboard."""
        layouts: list[Layout] = []
        for tile in self.tiles:
            layouts.extend(tile.get_layouts(self))
        return layouts

    def as_lakeview(self) -> Dashboard:
        """Create a lakeview dashboard from the dashboard metadata."""
        datasets = self.get_datasets()
        layouts = self._get_layouts()
        page = Page(
            name=self.display_name,
            display_name=self.display_name,
            layout=layouts,
        )
        lakeview_dashboard = Dashboard(datasets=datasets, pages=[page])
        return lakeview_dashboard

    @classmethod
    def from_dict(cls, raw: dict) -> "DashboardMetadata":
        display_name = raw["display_name"]  # Fail early if missing
        tiles, tiles_raw = [], raw.get("tiles", {})
        for tile_id, tile_raw in tiles_raw.items():
            if not isinstance(tile_raw, dict):
                logger.warning(f"Parsing invalid tile metadata in dashboard.yml: tiles.{tile_id}.{tile_raw}")
                continue
            tile_metadata = TileMetadata(id=tile_id)
            # The loop below allows for partial parsing by skipping unsupported fields
            for tile_key, tile_value in tile_raw.items():
                if tile_key == "id":
                    logger.warning(f"Parsing unsupported field in dashboard.yml: tiles.{tile_id}.id")
                    continue
                try:
                    tile_metadata_new = TileMetadata.from_dict({tile_key: tile_value})
                    tile_metadata = tile_metadata.merge(tile_metadata_new)
                except TypeError:
                    logger.warning(f"Parsing unsupported field in dashboard.yml: tiles.{tile_id}.{tile_key}")
                    continue
            tile = Tile.from_tile_metadata(tile_metadata)
            tiles.append(tile)
        return cls(display_name=display_name, _tiles=tiles)

    def as_dict(self) -> dict:
        raw: dict = {"display_name": self.display_name}
        if self.tiles:
            raw["tiles"] = {tile.metadata.id: tile.metadata.as_dict() for tile in self.tiles}
        return raw

    @classmethod
    def from_path(cls, path: Path) -> "DashboardMetadata":
        """Read the dashboard metadata from the dashboard folder."""
        dashboard_metadata_yml = cls._from_dashboard_path(path / "dashboard.yml")
        dashboard_metadata_folder = cls._from_dashboard_folder(path)
        tile_metadatas_yml = [tile.metadata for tile in dashboard_metadata_yml.tiles]
        tile_metadatas_folder = [tile.metadata for tile in dashboard_metadata_folder.tiles]
        tile_metadatas = cls._merge_tile_metadatas(tile_metadatas_yml, tile_metadatas_folder)
        tiles = [Tile.from_tile_metadata(tile_metadata) for tile_metadata in tile_metadatas]
        return cls(dashboard_metadata_yml.display_name, _tiles=tiles)

    @classmethod
    def _from_dashboard_path(cls, path: Path) -> "DashboardMetadata":
        """Read the dashboard metadata from the dashboard yml."""
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

    @classmethod
    def _from_dashboard_folder(cls, folder: Path) -> "DashboardMetadata":
        """Read the dashboard metadata from the tile files."""
        tiles = []
        for path in folder.iterdir():
            if not path.name.endswith((".sql", ".md", ".filter.yml")):
                continue
            tile_metadata = TileMetadata.from_path(path)
            tile = Tile.from_tile_metadata(tile_metadata)
            tiles.append(tile)
        return cls(display_name=folder.name, _tiles=tiles)

    def export_to_zipped_csv(self, sql_backend: SqlBackend, export_path: Path) -> Path:
        """Export the dashboard queries to CSV files directly into a ZIP archive."""
        with ZipFile(export_path, mode="w") as zip_file:
            for tile in self.tiles:
                if not tile.metadata.is_query():
                    continue

                rows = list(sql_backend.fetch(tile.content))
                if not rows:
                    continue

                buffer = StringIO()
                writer = None

                for row in rows:
                    if writer is None:
                        headers = row.asDict().keys()
                        writer = csv.DictWriter(buffer, fieldnames=headers)
                        writer.writeheader()
                    writer.writerow(row.asDict())

                with zip_file.open(f"{tile.metadata.id}.csv", "w") as csv_file:
                    csv_file.write(buffer.getvalue().encode("utf-8"))

        return export_path


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
            query = QueryTile.format(dataset.query)
            (local_path / f"{dataset.name}.sql").write_text(query)
        for page in dashboard.pages:
            with (local_path / f"{page.name}.yml").open("w") as f:
                yaml.safe_dump(page.as_dict(), f)
            for layout in page.layout:
                if layout.widget.textbox_spec is not None:
                    name = layout.widget.name.removesuffix("_widget")
                    (local_path / f"{name}.md").write_text(layout.widget.textbox_spec)
        return dashboard

    def create_dashboard(
        self,
        dashboard_metadata: DashboardMetadata,
        *,
        parent_path: str | None = None,
        dashboard_id: str | None = None,
        warehouse_id: str | None = None,
        publish: bool = False,
    ) -> SDKDashboard:
        """Create a Lakeview dashboard.

        Parameters :
            dashboard_metadata : DashboardMetadata
                The dashboard metadata
            parent_path : str | None (default: None)
                The folder in the Databricks workspace to store the dashboard file in
            dashboard_id : str | None (default: None)
                The id of the dashboard to update
            warehouse_id : str | None (default: None)
                The id of the warehouse to use
            publish : bool (default: False)
                If True, publish the dashboard. If False, save it as a draft.
        """
        dashboard_metadata.validate()
        serialized_dashboard = json.dumps(dashboard_metadata.as_lakeview().as_dict())
        if dashboard_id is not None:
            sdk_dashboard = self._ws.lakeview.update(
                dashboard_id,
                display_name=dashboard_metadata.display_name,
                serialized_dashboard=serialized_dashboard,
                warehouse_id=warehouse_id,
            )
        else:
            sdk_dashboard = self._ws.lakeview.create(
                dashboard_metadata.display_name,
                parent_path=parent_path,
                serialized_dashboard=serialized_dashboard,
                warehouse_id=warehouse_id,
            )
        if publish:
            assert sdk_dashboard.dashboard_id is not None
            self._ws.lakeview.publish(sdk_dashboard.dashboard_id, warehouse_id=warehouse_id)
        return sdk_dashboard

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

    def get_url(self, dashboard_id: str) -> str:
        """Get the dashboard URL.

        Parameters :
            dashboard_id : str
                The dashboard id to get the URL for

        Returns :
            The dashboard URL
        """
        # The /published redirects to the draft if the dashboard is not published
        dashboard_url = f"{self._ws.config.host}/dashboardsv3/{dashboard_id}/published"
        return dashboard_url
