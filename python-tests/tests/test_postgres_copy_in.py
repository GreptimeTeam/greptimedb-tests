# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Integration tests for GreptimeDB PostgreSQL `COPY ... FROM STDIN`
(the PostgreSQL copy-in sub-protocol, added in greptimedb#9297).

Both Python PostgreSQL drivers are covered:
- psycopg2  (`cursor.copy_expert`)
- psycopg3  (`cursor.copy()` block)

NOTE: GreptimeDB currently implements only `WITH (FORMAT csv)`; the
default PostgreSQL text/binary formats are rejected with guidance.
"""

import io
import logging
from datetime import datetime, timedelta

import pytest

import psycopg2
import psycopg

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PostgresCopyInTest:
    """COPY ... FROM STDIN test harness shared by psycopg2 and psycopg3."""

    def __init__(self):
        self.conn = None
        self.driver = None

    def teardown(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def connect(self, driver_type: str):
        import os

        self.driver = driver_type
        host = os.environ.get("POSTGRES_HOST", "").strip() or "127.0.0.1"
        port = int(os.environ.get("POSTGRES_PORT", "").strip() or "4003")
        db = os.environ.get("DB_NAME", "").strip() or "public"
        username = os.environ.get("GREPTIME_USERNAME", "").strip()
        password = os.environ.get("GREPTIME_PASSWORD", "").strip()

        if driver_type == "postgresql":
            logger.info(f"Connecting to PostgreSQL (psycopg2): {host}:{port}/{db}")
            self.conn = psycopg2.connect(
                host=host, port=port, database=db, user=username, password=password
            )
        elif driver_type == "postgresql3":
            logger.info(f"Connecting to PostgreSQL (psycopg3): {host}:{port}/{db}")
            self.conn = psycopg.connect(
                host=host, port=port, dbname=db, user=username, password=password
            )
        else:
            raise ValueError(f"Unknown driver: {driver_type}")
        self.conn.autocommit = False

    def execute(self, sql: str):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
        self.conn.commit()

    def query_all(self, sql: str):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def drop_table(self, table: str):
        self.execute(f"DROP TABLE IF EXISTS {table}")

    def rollback(self):
        try:
            self.conn.rollback()
        except Exception:
            pass

    def run_copy_in(self, sql: str, data: str) -> int:
        """
        Streams `data` through the copy-in sub-protocol and returns the
        number of rows the server reports written.
        """
        if self.driver == "postgresql":
            with self.conn.cursor() as cursor:
                cursor.copy_expert(sql, io.StringIO(data))
                rowcount = cursor.rowcount
            self.conn.commit()
            return rowcount

        # psycopg3
        with self.conn.cursor() as cursor:
            with cursor.copy(sql) as copy:
                copy.write(data)
            rowcount = cursor.rowcount
        self.conn.commit()
        return rowcount

    def run_copy_in_fail(self, sql: str, data: str):
        """Runs a copy expected to fail; returns the raised exception."""
        try:
            if self.driver == "postgresql":
                with self.conn.cursor() as cursor:
                    cursor.copy_expert(sql, io.StringIO(data))
            else:
                with self.conn.cursor() as cursor:
                    with cursor.copy(sql) as copy:
                        copy.write(data)
            raise AssertionError(f"expected COPY to fail: {sql}")
        except AssertionError:
            raise
        except Exception as e:  # noqa: BLE001 - driver-specific error types
            self.rollback()
            return e
        finally:
            self.rollback()


@pytest.fixture
def copy_test():
    instance = PostgresCopyInTest()
    yield instance
    instance.teardown()


def _metrics_table(driver: str) -> str:
    return f"test_copy_in_metrics_{driver.replace('postgresql', 'pg')}"


METRICS_SCHEMA = "host STRING, val DOUBLE, note STRING, ts TIMESTAMP, TIME INDEX(ts)"


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_in_basic(copy_test, driver):
    """
    Basic CSV COPY: plain rows, an unquoted empty field (NULL), and a
    quoted field containing a comma. Verifies the reported row count and
    the stored values.
    """
    copy_test.connect(driver)
    table = _metrics_table(driver)
    copy_test.drop_table(table)
    copy_test.execute(f"CREATE TABLE {table} ({METRICS_SCHEMA})")

    data = (
        "host1,1.5,hello,2023-11-14 22:13:20\n"
        'host2,,"quoted, comma",2023-11-14 22:13:21\n'
        'host3,3.5,"say ""hi""",2023-11-14 22:13:22\n'
    )
    rowcount = copy_test.run_copy_in(f"COPY {table} FROM STDIN WITH (FORMAT csv)", data)
    assert rowcount == 3

    rows = copy_test.query_all(f"SELECT host, val, note, ts FROM {table} ORDER BY ts")
    assert len(rows) == 3
    assert rows[0][0] == "host1"
    assert rows[0][1] == 1.5
    assert rows[0][2] == "hello"
    assert rows[0][3] == datetime(2023, 11, 14, 22, 13, 20)
    # Unquoted empty field is NULL.
    assert rows[1][1] is None
    assert rows[1][2] == "quoted, comma"
    # Escaped quotes decode to a single quote.
    assert rows[2][2] == 'say "hi"'

    copy_test.drop_table(table)


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_in_column_list(copy_test, driver):
    """
    A column list may target a subset of columns in any order; columns
    left out are NULL. The time index column must be included.
    """
    copy_test.connect(driver)
    table = _metrics_table(driver)
    copy_test.drop_table(table)
    copy_test.execute(f"CREATE TABLE {table} ({METRICS_SCHEMA})")

    rowcount = copy_test.run_copy_in(
        f"COPY {table} (note, ts, val) FROM STDIN WITH (FORMAT csv)",
        "from_column_list,2023-11-14 22:13:24,7.25\n",
    )
    assert rowcount == 1

    rows = copy_test.query_all(f"SELECT host, val, note FROM {table}")
    assert len(rows) == 1
    assert rows[0][0] is None  # host not targeted: NULL
    assert rows[0][1] == 7.25
    assert rows[0][2] == "from_column_list"

    # A column list that omits the time index column is rejected.
    err = copy_test.run_copy_in_fail(
        f"COPY {table} (host) FROM STDIN WITH (FORMAT csv)",
        "host1\n",
    )
    assert "time index" in str(err), str(err)

    copy_test.drop_table(table)


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_in_csv_options(copy_test, driver):
    """
    PostgreSQL CSV options: HEADER (skip the first record), a custom
    single-byte DELIMITER, CRLF record separators, and quoted fields
    with embedded newlines. The WITH keyword itself is optional.
    """
    copy_test.connect(driver)
    table = _metrics_table(driver)
    copy_test.drop_table(table)
    copy_test.execute(f"CREATE TABLE {table} ({METRICS_SCHEMA})")

    data = (
        "host;val;note;ts\r\n"
        'h1;1.5;"line one\nline two";2023-11-14 22:13:20\r\n'
        "h2;2.5;plain;2023-11-14 22:13:21\r\n"
    )
    rowcount = copy_test.run_copy_in(
        f"COPY {table} FROM STDIN (FORMAT csv, HEADER, DELIMITER ';')",
        data,
    )
    assert rowcount == 2  # the header record is skipped

    rows = copy_test.query_all(f"SELECT host, val, note FROM {table} ORDER BY ts")
    assert rows[0][0] == "h1"
    assert rows[0][1] == 1.5
    assert rows[0][2] == "line one\nline two"
    assert rows[1][0] == "h2"
    assert rows[1][2] == "plain"

    copy_test.drop_table(table)


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_in_empty_field_is_null(copy_test, driver):
    """
    An empty field is NULL whether quoted or not, matching the
    file-based `COPY ... FROM 'file.csv'` behavior.
    """
    copy_test.connect(driver)
    table = f"test_copy_in_empty_{driver.replace('postgresql', 'pg')}"
    copy_test.drop_table(table)
    copy_test.execute(
        f"CREATE TABLE {table} (note STRING, ts TIMESTAMP, TIME INDEX(ts))"
    )

    rowcount = copy_test.run_copy_in(
        f"COPY {table} (note, ts) FROM STDIN WITH (FORMAT csv)",
        "plain,2024-01-01 00:00:00\n"
        '"",2024-01-01 00:00:01\n'
        ",2024-01-01 00:00:02\n",
    )
    assert rowcount == 3

    rows = copy_test.query_all(f"SELECT note IS NULL, note FROM {table} ORDER BY ts")
    assert rows == [(False, "plain"), (True, None), (True, None)]

    copy_test.drop_table(table)


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_in_boolean_spellings(copy_test, driver):
    """
    COPY IN accepts PostgreSQL's full boolean input grammar (`t`/`f`,
    `true`/`false`, `yes`/`no`, `on`/`off`, `1`/`0`), which keeps the
    bytes produced by COPY OUT importable unchanged.
    """
    copy_test.connect(driver)
    table = f"test_copy_in_bool_{driver.replace('postgresql', 'pg')}"
    copy_test.drop_table(table)
    copy_test.execute(
        f"CREATE TABLE {table} (flag BOOLEAN, ts TIMESTAMP, TIME INDEX(ts))"
    )

    rowcount = copy_test.run_copy_in(
        f"COPY {table} (flag, ts) FROM STDIN WITH (FORMAT csv)",
        "t,2024-01-01 00:00:00\n"
        "f,2024-01-01 00:00:01\n"
        "TRUE,2024-01-01 00:00:02\n"
        "no,2024-01-01 00:00:03\n"
        "on,2024-01-01 00:00:04\n"
        "0,2024-01-01 00:00:05\n",
    )
    assert rowcount == 6

    rows = copy_test.query_all(f"SELECT flag FROM {table} ORDER BY ts")
    assert [r[0] for r in rows] == [True, False, True, False, True, False]

    # A non-boolean spelling is rejected and nothing is written.
    err = copy_test.run_copy_in_fail(
        f"COPY {table} (flag, ts) FROM STDIN WITH (FORMAT csv)",
        "maybe,2024-01-01 00:00:06\n",
    )
    assert "invalid input syntax for type boolean" in str(err), str(err)
    assert copy_test.query_all(f"SELECT COUNT(*) FROM {table}")[0][0] == 6

    copy_test.drop_table(table)


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_in_large_batch(copy_test, driver):
    """
    A stream larger than the server's flush batch (32768 rows) must be
    ingested completely across batch boundaries.
    """
    copy_test.connect(driver)
    table = f"test_copy_in_large_{driver.replace('postgresql', 'pg')}"
    copy_test.drop_table(table)
    copy_test.execute(
        f"CREATE TABLE {table} (host STRING, seq BIGINT, ts TIMESTAMP, TIME INDEX(ts))"
    )

    total = 40000
    buf = io.StringIO()
    base = datetime(2024, 1, 1)
    for i in range(total):
        # A unique millisecond timestamp per row: without primary keys a
        # repeated time index would upsert instead of appending.
        ts = (base + timedelta(milliseconds=i)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        buf.write(f"host_{i},{i},{ts}\n")
    rowcount = copy_test.run_copy_in(
        f"COPY {table} (host, seq, ts) FROM STDIN WITH (FORMAT csv)",
        buf.getvalue(),
    )
    assert rowcount == total

    count = copy_test.query_all(f"SELECT COUNT(*) FROM {table}")[0][0]
    assert count == total

    # Spot-check first and last rows through the whole pipeline.
    first = copy_test.query_all(f"SELECT seq FROM {table} WHERE host = 'host_0'")[0][0]
    last = copy_test.query_all(
        f"SELECT seq FROM {table} WHERE host = 'host_{total - 1}'"
    )[0][0]
    assert first == 0
    assert last == total - 1

    copy_test.drop_table(table)


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_in_session_timezone_parity(copy_test, driver):
    """
    COPY and SQL INSERT must agree on offset-free timestamp strings:
    both interpret them in the session timezone.
    """
    copy_test.connect(driver)
    table = f"test_copy_in_tz_{driver.replace('postgresql', 'pg')}"
    copy_test.drop_table(table)
    copy_test.execute(
        f"CREATE TABLE {table} (case_id STRING PRIMARY KEY, ts TIMESTAMP, TIME INDEX(ts))"
    )
    copy_test.execute("SET TIME ZONE = 'Asia/Shanghai'")

    copy_test.execute(
        f"INSERT INTO {table} (case_id, ts) VALUES ('insert', '2024-01-01 00:00:00')"
    )
    rowcount = copy_test.run_copy_in(
        f"COPY {table} (case_id, ts) FROM STDIN WITH (FORMAT csv)",
        "copy,2024-01-01 00:00:00\n",
    )
    assert rowcount == 1

    rows = copy_test.query_all(
        f"SELECT case_id, CAST(ts AS BIGINT) FROM {table} ORDER BY case_id"
    )
    assert rows[0][0] == "copy"
    assert rows[1][0] == "insert"
    assert rows[0][1] == rows[1][1], "COPY and INSERT must agree on naive timestamps"
    # The Asia/Shanghai interpretation, not UTC.
    assert rows[0][1] == 1704038400000

    copy_test.execute("SET TIME ZONE = 'UTC'")
    copy_test.drop_table(table)


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_in_errors(copy_test, driver):
    """
    Statement-level and data-level failures: the error messages guide
    the user, a failed copy writes nothing, and the connection stays
    usable for a subsequent COPY.
    """
    copy_test.connect(driver)
    table = _metrics_table(driver)
    copy_test.drop_table(table)
    copy_test.execute(f"CREATE TABLE {table} ({METRICS_SCHEMA})")

    # COPY without an explicit format is rejected with guidance.
    err = copy_test.run_copy_in_fail(
        f"COPY {table} FROM STDIN", "host1,1.5,x,2023-11-14 22:13:20\n"
    )
    assert "requires an explicit format" in str(err), str(err)

    # The PostgreSQL text and binary formats are not implemented yet.
    for fmt in ("text", "txt", "binary"):
        err = copy_test.run_copy_in_fail(
            f"COPY {table} FROM STDIN WITH (FORMAT {fmt})",
            "host1,1.5,x,2023-11-14 22:13:20\n",
        )
        assert "not supported" in str(err), str(err)

    # Unknown format.
    err = copy_test.run_copy_in_fail(
        f"COPY {table} FROM STDIN WITH (FORMAT parquet)",
        "host1,1.5,x,2023-11-14 22:13:20\n",
    )
    assert "unknown COPY format" in str(err), str(err)

    # Unknown table.
    err = copy_test.run_copy_in_fail(
        "COPY copy_in_does_not_exist FROM STDIN WITH (FORMAT csv)",
        "host1,1.5,x,2023-11-14 22:13:20\n",
    )
    assert "does not exist" in str(err), str(err)

    # Options the csv format does not understand are rejected.
    err = copy_test.run_copy_in_fail(
        f"COPY {table} FROM STDIN WITH (FORMAT csv, PRECISION 'ns')",
        "host1,1.5,x,2023-11-14 22:13:20\n",
    )
    assert "not supported by format csv" in str(err), str(err)

    # A custom NULL string is not supported (an empty field is NULL).
    err = copy_test.run_copy_in_fail(
        f"COPY {table} FROM STDIN WITH (FORMAT csv, NULL 'N')",
        "host1,1.5,x,2023-11-14 22:13:20\n",
    )
    assert "custom NULL string" in str(err), str(err)

    # COPY FROM STDIN must run alone.
    err = copy_test.run_copy_in_fail(
        f"COPY {table} FROM STDIN WITH (FORMAT csv); SELECT 1",
        "host1,1.5,x,2023-11-14 22:13:20\n",
    )
    assert "alone" in str(err), str(err)

    # Unknown column in the column list.
    err = copy_test.run_copy_in_fail(
        f"COPY {table} (nope, ts) FROM STDIN WITH (FORMAT csv)",
        "host1,2023-11-14 22:13:20\n",
    )
    assert "does not exist" in str(err), str(err)

    # A JSON column cannot be ingested by the csv format.
    json_table = f"test_copy_in_json_{driver.replace('postgresql', 'pg')}"
    copy_test.drop_table(json_table)
    copy_test.execute(
        f"CREATE TABLE {json_table} (payload JSON, ts TIMESTAMP, TIME INDEX(ts))"
    )
    err = copy_test.run_copy_in_fail(
        f"COPY {json_table} (payload, ts) FROM STDIN WITH (FORMAT csv)",
        '{{"a":1}},2023-11-14 22:13:20\n',
    )
    assert "not supported by format csv" in str(err), str(err)
    copy_test.drop_table(json_table)

    # Bad data aborts the whole copy and writes nothing.
    err = copy_test.run_copy_in_fail(
        f"COPY {table} FROM STDIN WITH (FORMAT csv)",
        "host1,1.5,ok,2023-11-14 22:13:20\n"
        "host2,not_a_double,ok,2023-11-14 22:13:21\n",
    )
    assert "failed to parse" in str(err), str(err)
    assert copy_test.query_all(f"SELECT COUNT(*) FROM {table}")[0][0] == 0

    # The connection is still usable: a new COPY succeeds on it.
    rowcount = copy_test.run_copy_in(
        f"COPY {table} FROM STDIN WITH (FORMAT csv)",
        "host1,1.5,after failure,2023-11-14 22:13:20\n",
    )
    assert rowcount == 1
    assert copy_test.query_all(f"SELECT COUNT(*) FROM {table}")[0][0] == 1

    copy_test.drop_table(table)


@pytest.mark.parametrize("driver", ["postgresql", "postgresql3"])
def test_copy_out_copy_in_round_trip(copy_test, driver):
    """
    Bytes produced by COPY OUT ... (FORMAT csv) must import back
    unchanged: booleans come back as `t`/`f` and timestamps carry the
    session-timezone offset.
    """
    copy_test.connect(driver)
    src = f"test_copy_rt_src_{driver.replace('postgresql', 'pg')}"
    dst = f"test_copy_rt_dst_{driver.replace('postgresql', 'pg')}"
    copy_test.drop_table(src)
    copy_test.drop_table(dst)
    copy_test.execute(
        f"CREATE TABLE {src} (host STRING, flag BOOLEAN, ts TIMESTAMP, TIME INDEX(ts))"
    )
    copy_test.execute(
        f"CREATE TABLE {dst} (host STRING, flag BOOLEAN, ts TIMESTAMP, TIME INDEX(ts))"
    )
    copy_test.execute(
        f"INSERT INTO {src} VALUES "
        "('a', true, '2024-01-01 00:00:00'), "
        "('b', false, '2024-07-01 12:30:00')"
    )

    # COPY OUT to a string.
    copy_test.execute("SET TIME ZONE = 'Asia/Shanghai'")
    exported = io.StringIO()
    with copy_test.conn.cursor() as cursor:
        if driver == "postgresql":
            cursor.copy_expert(
                f"COPY (SELECT host, flag, ts FROM {src} ORDER BY ts) "
                "TO STDOUT WITH (FORMAT csv)",
                exported,
            )
        else:
            with cursor.copy(
                f"COPY (SELECT host, flag, ts FROM {src} ORDER BY ts) "
                "TO STDOUT WITH (FORMAT csv)"
            ) as copy:
                for chunk in copy:
                    if isinstance(chunk, memoryview):
                        chunk = chunk.tobytes()
                    exported.write(
                        chunk.decode() if isinstance(chunk, bytes) else chunk
                    )
    data = exported.getvalue()
    copy_test.conn.commit()

    logger.info("[%s] COPY OUT produced: %r", driver, data)
    assert "t" in data and "f" in data
    assert "+08" in data  # timestamps rendered with the session-tz offset

    # COPY the exact bytes back in.
    rowcount = copy_test.run_copy_in(
        f"COPY {dst} (host, flag, ts) FROM STDIN WITH (FORMAT csv)", data
    )
    assert rowcount == 2

    rows = copy_test.query_all(
        f"SELECT CAST(src.ts AS BIGINT) = CAST(dst.ts AS BIGINT) "
        f"FROM {src} src JOIN {dst} dst ON src.host = dst.host ORDER BY src.host"
    )
    assert rows == [(True,), (True,)], "timestamps must round-trip exactly"
    flags = copy_test.query_all(f"SELECT flag FROM {dst} ORDER BY ts")
    assert [r[0] for r in flags] == [True, False]

    copy_test.execute("SET TIME ZONE = 'UTC'")
    copy_test.drop_table(src)
    copy_test.drop_table(dst)
