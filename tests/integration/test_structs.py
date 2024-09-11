import datetime
from dataclasses import dataclass

from databricks.labs.lsql.backends import StatementExecutionBackend


@dataclass
class Foo:
    first: str
    second: bool | None


@dataclass
class Nested:
    foo: Foo
    since: datetime.date
    created: datetime.datetime
    mapping: dict[str, int]
    array: list[int]


def test_appends_complex_types(ws, env_or_skip, make_random) -> None:
    sql_backend = StatementExecutionBackend(ws, env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))
    today = datetime.date.today()
    now = datetime.datetime.now()
    full_name = f"hive_metastore.default.t{make_random(4)}"
    sql_backend.save_table(
        full_name,
        [
            Nested(Foo("a", True), today, now, {"a": 1, "b": 2}, [1, 2, 3]),
            Nested(Foo("b", False), today, now, {"c": 3, "d": 4}, [4, 5, 6]),
        ],
        Nested,
    )
    rows = list(sql_backend.fetch(f"SELECT * FROM {full_name}"))
    assert len(rows) == 2
