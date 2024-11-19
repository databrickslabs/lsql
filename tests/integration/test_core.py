import logging

import pytest
from databricks.sdk.service.sql import Disposition

from databricks.labs.lsql.core import Row, StatementExecutionExt

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("disposition", [None, Disposition.INLINE, Disposition.EXTERNAL_LINKS])
def test_sql_execution_chunked(ws, disposition):
    see = StatementExecutionExt(ws, disposition=disposition)
    total = 0
    for (x,) in see("SELECT id FROM range(2000000)"):
        total += x
    assert total == 1999999000000


NYC_TAXI_TRIPS_LIMITED = """
WITH zipcodes AS (
  SELECT DISTINCT pickup_zip, dropoff_zip 
  FROM samples.nyctaxi.trips 
  WHERE pickup_zip = 10282 AND dropoff_zip <= 10005
)

SELECT 
  trips.pickup_zip, 
  trips.dropoff_zip, 
  trips.tpep_pickup_datetime, 
  trips.tpep_dropoff_datetime 
FROM 
    zipcodes 
  JOIN 
    samples.nyctaxi.trips AS trips 
    ON zipcodes.pickup_zip = trips.pickup_zip AND zipcodes.dropoff_zip = trips.dropoff_zip
ORDER BY trips.dropoff_zip, trips.tpep_pickup_datetime, trips.tpep_dropoff_datetime 
"""


def test_sql_execution(ws, env_or_skip) -> None:
    results = set()
    see = StatementExecutionExt(ws, warehouse_id=env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"))
    for pickup_zip, dropoff_zip, *_ in see.fetch_all(NYC_TAXI_TRIPS_LIMITED, catalog="samples"):
        results.add((pickup_zip, dropoff_zip))
    assert results == {
        (10282, 7114),
        (10282, 10001),
        (10282, 10002),
        (10282, 10003),
        (10282, 10005),
    }


def test_sql_execution_partial(ws, env_or_skip) -> None:
    results = set()
    see = StatementExecutionExt(ws, warehouse_id=env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"), catalog="samples")
    for row in see(NYC_TAXI_TRIPS_LIMITED):
        pickup_zip, dropoff_zip, pickup_time, dropoff_time = row[0], row[1], row[2], row[3]
        all_fields = row.asDict()
        logger.info(f"{pickup_zip}@{pickup_time} -> {dropoff_zip}@{dropoff_time}: {all_fields}")
        results.add((pickup_zip, dropoff_zip))
    assert results == {
        (10282, 7114),
        (10282, 10001),
        (10282, 10002),
        (10282, 10003),
        (10282, 10005),
    }


def test_fetch_one(ws):
    see = StatementExecutionExt(ws)
    assert see.fetch_one("SELECT 1 AS id") == Row(id=1)


def test_fetch_one_fails_if_limit_is_bigger(ws):
    see = StatementExecutionExt(ws)
    with pytest.raises(ValueError):
        see.fetch_one("SELECT * FROM samples.nyctaxi.trips LIMIT 100")


def test_fetch_one_works(ws) -> None:
    see = StatementExecutionExt(ws)
    row = see.fetch_one("SELECT pickup_zip FROM samples.nyctaxi.trips WHERE pickup_zip == 10282 LIMIT 1")
    assert row is not None
    assert row.pickup_zip == 10282


def test_fetch_value(ws):
    see = StatementExecutionExt(ws)
    count = see.fetch_value("SELECT COUNT(*) FROM samples.nyctaxi.trips")
    assert count == 21932


def test_row_as_dict_deprecated(ws) -> None:
    see = StatementExecutionExt(ws)
    row = see.fetch_one("SELECT 1")
    assert row is not None
    with pytest.deprecated_call():
        _ = row.as_dict()
