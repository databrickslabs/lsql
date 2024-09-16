import datetime
from dataclasses import dataclass

import pytest

from databricks.labs.lsql.backends import StatementExecutionBackend


@dataclass
class Nested:
    first: str
    second: bool | None


@dataclass
class NestedWithDict:
    name: str
    other: dict[str, str] | None


@dataclass
class Nesting:
    nested: Nested
    since: datetime.date
    created: datetime.datetime
    mapping: dict[str, int]
    int_array: list[int]
    struct_array: list[NestedWithDict]


@pytest.mark.skip(reason="Missing permissions to create table")
def test_appends_complex_types(ws, env_or_skip, make_random) -> None:
    sql_backend = StatementExecutionBackend(ws, env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))
    today = datetime.date.today()
    now = datetime.datetime.now()
    full_name = f"hive_metastore.default.t{make_random(4)}"
    sql_backend.save_table(
        full_name,
        [
            Nesting(
                Nested("a", True),
                today,
                now,
                {"a": 1, "b": 2},
                [1, 2, 3],
                [NestedWithDict("s", {"f1": "v1", "f2": "v2"})],
            ),
            Nesting(Nested("b", False), today, now, {"c": 3, "d": 4}, [4, 5, 6], []),
        ],
        Nesting,
    )
    rows = list(sql_backend.fetch(f"SELECT * FROM {full_name}"))
    assert len(rows) == 2
