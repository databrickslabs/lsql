import datetime
from dataclasses import dataclass
from typing import Optional

import pytest

from databricks.labs.lsql.structs import StructInference, StructInferError


@dataclass
class Foo:
    first: str
    second: bool | None


@dataclass
class Nested:
    foo: Foo
    mapping: dict[str, int]
    array: list[int]


class NotDataclass:
    x: int


@pytest.mark.parametrize(
    "type_ref, ddl",
    [
        (int, "LONG"),
        (int | None, "LONG"),
        (Optional[int], "LONG"),
        (float, "FLOAT"),
        (str, "STRING"),
        (bool, "BOOLEAN"),
        (datetime.date, "DATE"),
        (datetime.datetime, "TIMESTAMP"),
        (list[str], "ARRAY<STRING>"),
        (set[str], "ARRAY<STRING>"),
        (dict[str, int], "MAP<STRING,LONG>"),
        (dict[int, list[str]], "MAP<LONG,ARRAY<STRING>>"),
        (Foo, "STRUCT<first:STRING,second:BOOLEAN>"),
        (Nested, "STRUCT<foo:STRUCT<first:STRING,second:BOOLEAN>,mapping:MAP<STRING,LONG>,array:ARRAY<LONG>>"),
    ],
)
def test_struct_inference(type_ref, ddl) -> None:
    inference = StructInference()
    assert inference.as_ddl(type_ref) == ddl


@pytest.mark.parametrize("type_ref", [type(None), list, set, tuple, dict, object, NotDataclass])
def test_struct_inference_raises_on_unknown_type(type_ref) -> None:
    inference = StructInference()
    with pytest.raises(StructInferError):
        inference.as_ddl(type_ref)


@pytest.mark.parametrize(
    "type_ref,ddl",
    [
        (Foo, "first STRING NOT NULL, second BOOLEAN"),
        (
            Nested,
            "foo STRUCT<first:STRING,second:BOOLEAN> NOT NULL, "
            "mapping MAP<STRING,LONG> NOT NULL, array ARRAY<LONG> NOT NULL",
        ),
    ],
)
def test_as_schema(type_ref, ddl) -> None:
    inference = StructInference()
    assert inference.as_schema(type_ref) == ddl
