from pathlib import Path

from databricks.labs.lsql.discovery import Scanner


def test_finds():
    s = Scanner(Path('/Users/serge.smertin/git/labs/lsql/tests'))
    x = list(s.find_all())
    assert x == ['Nested']