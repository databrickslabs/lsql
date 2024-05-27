import json
import random
import string
from pathlib import Path
from typing import ClassVar, Protocol, runtime_checkable

import sqlglot
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard
from databricks.sdk.service.workspace import ExportFormat

from databricks.labs.lsql.lakeview import (
    ControlFieldEncoding,
    CounterEncodingMap,
    CounterSpec,
)
from databricks.labs.lsql.lakeview import Dashboard as LakeviewDashboard
from databricks.labs.lsql.lakeview import (
    Dataset,
    Field,
    Layout,
    NamedQuery,
    Page,
    Position,
    Query,
    Widget,
)


@runtime_checkable
class _DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


class Dashboards:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def get_dashboard(self, dashboard_path: str):
        with self._ws.workspace.download(dashboard_path, format=ExportFormat.SOURCE) as f:
            raw = f.read().decode("utf-8")
            as_dict = json.loads(raw)
            return LakeviewDashboard.from_dict(as_dict)

    def save_to_folder(self, dashboard_path: str, local_path: Path):
        local_path.mkdir(parents=True, exist_ok=True)
        dashboard = self.get_dashboard(dashboard_path)
        better_names = {}
        for dataset in dashboard.datasets:
            name = dataset.display_name
            better_names[dataset.name] = name
            query_path = local_path / f"{name}.sql"
            sql_query = dataset.query
            self._format_sql_file(sql_query, query_path)
        lvdash_yml = local_path / "lvdash.yml"
        with lvdash_yml.open("w") as f:
            first_page = dashboard.pages[0]
            self._replace_names(first_page, better_names)
            page = first_page.as_dict()
            yaml.safe_dump(page, f)
        assert True

    @staticmethod
    def _create_random_id() -> str:
        charset = string.ascii_lowercase + string.digits
        return "".join(random.choices(charset, k=8))

    def create(self, dashboard_folder: Path) -> LakeviewDashboard:
        """Create a dashboard from code, i.e. configuration and queries."""
        datasets, layouts = [], []
        for query_path in dashboard_folder.glob("*.sql"):
            with query_path.open("r") as query_file:
                raw_query = query_file.read()
            dataset = Dataset(name=self._create_random_id(), display_name=query_path.stem, query=raw_query)
            datasets.append(dataset)

            fields = [Field(name="count", expression="`count`")]
            query = Query(dataset_name=dataset.name, fields=fields)
            named_query = NamedQuery(name=self._create_random_id(), query=query)
            counter_spec = CounterSpec(CounterEncodingMap())
            widget = Widget(name=self._create_random_id(), queries=[named_query], spec=counter_spec)
            position = Position(x=0, y=0, width=1, height=1)
            layout = Layout(widget=widget, position=position)
            layouts.append(layout)

        page = Page(name=dashboard_folder.name, display_name=dashboard_folder.name, layout=layouts)
        lakeview_dashboard = LakeviewDashboard(datasets=datasets, pages=[page])
        return lakeview_dashboard

    def deploy(
        self, lakeview_dashboard: LakeviewDashboard, *, display_name: str | None = None, dashboard_id: str | None = None
    ) -> SDKDashboard:
        """Deploy a lakeview dashboard."""
        if (display_name is None and dashboard_id is None) or (display_name is not None and dashboard_id is not None):
            raise ValueError("Give either display_name or dashboard_id.")
        if display_name is not None:
            dashboard = self._ws.lakeview.create(
                display_name, serialized_dashboard=json.dumps(lakeview_dashboard.as_dict())
            )
        else:
            assert dashboard_id is not None
            dashboard = self._ws.lakeview.update(
                dashboard_id, serialized_dashboard=json.dumps(lakeview_dashboard.as_dict())
            )
        return dashboard

    def _format_sql_file(self, sql_query, query_path):
        with query_path.open("w") as f:
            try:
                for statement in sqlglot.parse(sql_query):
                    # see https://sqlglot.com/sqlglot/generator.html#Generator
                    pretty = statement.sql(
                        dialect="databricks",
                        normalize=True,  # normalize identifiers to lowercase
                        pretty=True,  # format the produced SQL string
                        normalize_functions="upper",  # normalize function names to uppercase
                        max_text_width=80,  # wrap text at 120 characters
                    )
                    f.write(f"{pretty};\n")
            except sqlglot.ParseError:
                f.write(sql_query)

    def _replace_names(self, node: _DataclassInstance, better_names: dict[str, str]):
        # walk evely dataclass instance recursively and replace names
        if isinstance(node, _DataclassInstance):
            for field in node.__dataclass_fields__.values():
                value = getattr(node, field.name)
                if isinstance(value, list):
                    setattr(node, field.name, [self._replace_names(item, better_names) for item in value])
                elif isinstance(value, _DataclassInstance):
                    setattr(node, field.name, self._replace_names(value, better_names))
        if isinstance(node, Query):
            node.dataset_name = better_names.get(node.dataset_name, node.dataset_name)
        elif isinstance(node, NamedQuery) and node.query:
            # 'dashboards/01eeb077e38c17e6ba3511036985960c/datasets/01eeb081882017f6a116991d124d3068_...'
            if node.name.startswith("dashboards/"):
                parts = [node.query.dataset_name]
                for field in node.query.fields:
                    parts.append(field.name)
                new_name = "_".join(parts)
                better_names[node.name] = new_name
            node.name = better_names.get(node.name, node.name)
        elif isinstance(node, ControlFieldEncoding):
            node.query_name = better_names.get(node.query_name, node.query_name)
        return node
