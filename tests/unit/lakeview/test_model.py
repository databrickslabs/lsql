from databricks.labs.lsql.lakeview.model import (
    PaginationSize,
    TableV1EncodingMap,
    TableV1Spec,
)


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