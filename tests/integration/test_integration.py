import logging

import pytest
from databricks.sdk.service.sql import Disposition

from databricks.labs.lsql.core import StatementExecutionExt

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("disposition", [None, Disposition.INLINE, Disposition.EXTERNAL_LINKS])
def test_sql_execution_chunked(ws, disposition):
    see = StatementExecutionExt(ws, disposition=disposition)
    total = 0
    for (x,) in see("SELECT id FROM range(2000000)"):
        total += x
    assert total == 1999999000000


def test_sql_execution(ws, env_or_skip):
    results = []
    see = StatementExecutionExt(ws, warehouse_id=env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))
    for pickup_zip, dropoff_zip in see.fetch_all(
        "SELECT pickup_zip, dropoff_zip FROM nyctaxi.trips LIMIT 10", catalog="samples"
    ):
        results.append((pickup_zip, dropoff_zip))
    assert results == [
        (10282, 10171),
        (10110, 10110),
        (10103, 10023),
        (10022, 10017),
        (10110, 10282),
        (10009, 10065),
        (10153, 10199),
        (10112, 10069),
        (10023, 10153),
        (10012, 10003),
    ]


def test_sql_execution_partial(ws, env_or_skip):
    results = []
    see = StatementExecutionExt(ws, warehouse_id=env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"), catalog="samples")
    for row in see("SELECT * FROM nyctaxi.trips LIMIT 10"):
        pickup_time, dropoff_time = row[0], row[1]
        pickup_zip = row.pickup_zip
        dropoff_zip = row["dropoff_zip"]
        all_fields = row.as_dict()
        logger.info(f"{pickup_zip}@{pickup_time} -> {dropoff_zip}@{dropoff_time}: {all_fields}")
        results.append((pickup_zip, dropoff_zip))
    assert results == [
        (10282, 10171),
        (10110, 10110),
        (10103, 10023),
        (10022, 10017),
        (10110, 10282),
        (10009, 10065),
        (10153, 10199),
        (10112, 10069),
        (10023, 10153),
        (10012, 10003),
    ]


def test_fetch_one(ws):
    see = StatementExecutionExt(ws)
    assert see.fetch_one("SELECT 1") == (1,)


def test_fetch_one_fails_if_limit_is_bigger(ws):
    see = StatementExecutionExt(ws)
    with pytest.raises(ValueError):
        see.fetch_one("SELECT * FROM samples.nyctaxi.trips LIMIT 100")


def test_fetch_one_works(ws):
    see = StatementExecutionExt(ws)
    row = see.fetch_one("SELECT * FROM samples.nyctaxi.trips LIMIT 1")
    assert row.pickup_zip == 10282


def test_fetch_value(ws):
    see = StatementExecutionExt(ws)
    count = see.fetch_value("SELECT COUNT(*) FROM samples.nyctaxi.trips")
    assert count == 21932
