import json
from pathlib import Path
from typing import ClassVar, Protocol, runtime_checkable

import sqlglot
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

from databricks.labs.lsql.lakeview import ControlFieldEncoding
from databricks.labs.lsql.lakeview import Dashboard as LakeviewDashboard
from databricks.labs.lsql.lakeview import NamedQuery, Query


@runtime_checkable
class _DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


class Dashboard:  # TODO: Rename, maybe DashboardClient?
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

    def deploy(self, dashboard_folder: Path):
        """Deploy dashboard from code, i.e. configuration and queries."""

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
