from databricks.labs.lsql.lakeview.model import (
    Dataset,
    PaginationSize,
    TableV1EncodingMap,
    TableV1Spec,
)


def test_dataset_serialisation_round_trip() -> None:
    dataset = Dataset("a_name", display_name="A Name", query="SELECT a FROM name")
    serialized_first = dataset.as_dict()
    restored = Dataset.from_dict(serialized_first)
    assert dataset == restored
    serialized_second = restored.as_dict()
    assert serialized_first == serialized_second


def test_dataset_from_dict_reads_query_lines() -> None:
    dataset = Dataset.from_dict({"name": "d", "queryLines": ["SELECT ", "1"]})
    assert dataset.query == "SELECT 1"


def test_dataset_from_dict_query_is_none_when_absent() -> None:
    dataset = Dataset.from_dict({"name": "d"})
    assert dataset.query is None


def test_dataset_from_dict_reads_query() -> None:
    dataset = Dataset.from_dict({"name": "d", "query": "SELECT 1"})
    assert dataset.query == "SELECT 1"


def test_table_v1_spec_adds_invisible_columns_to_dict():
    table_encodings = TableV1EncodingMap(None)
    spec = TableV1Spec(
        allow_html_by_default=False,
        condensed=True,
        encodings=table_encodings,
        invisible_columns=[],
        items_per_page=25,
        pagination_size=PaginationSize.DEFAULT,
        with_row_number=False,
    )

    assert "invisibleColumns" in spec.as_dict()
