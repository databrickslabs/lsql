import pytest

from databricks.labs.lsql.escapes import escape_full_name, escape_name


@pytest.mark.parametrize(
    "path,expected",
    [
        ("a", "`a`"),
        ("a.b", "`a`.`b`"),
        ("a.b.c", "`a`.`b`.`c`"),
        ("`a`.b.c", "`a`.`b`.`c`"),
        ("a.`b`.c", "`a`.`b`.`c`"),
        ("a.b.`c`", "`a`.`b`.`c`"),
        ("`a.b`.c", "`a`.`b`.`c`"),
        ("a.`b.c`", "`a`.`b`.`c`"),
        ("`a.b`.`c`", "`a`.`b`.`c`"),
        ("`a`.`b.c`", "`a`.`b`.`c`"),
        ("`a`.`b`.`c`", "`a`.`b`.`c`"),
        ("a.b.c.d", "`a`.`b`.`c.d`"),
        ("a-b.c.d", "`a-b`.`c`.`d`"),
        ("a.b-c.d", "`a`.`b-c`.`d`"),
        ("a.b.c-d", "`a`.`b`.`c-d`"),
        ("a.b.c`d", "`a`.`b`.`c``d`"),
        ("✨.🍰.✨", "`✨`.`🍰`.`✨`"),
        ("", ""),
    ],
)
def test_escaped_path(path: str, expected: str) -> None:
    assert escape_full_name(path) == expected


def test_escaped_when_column_contains_period() -> None:
    assert escape_name("column.with.periods") == "`column.with.periods`"
