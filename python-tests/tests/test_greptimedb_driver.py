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
Integration tests for GreptimeDB drivers (MySQL and PostgreSQL).
Tests various database operations including CRUD, timezone handling, and batch inserts.
"""

import os
import logging
from datetime import datetime, date, timezone
from typing import Optional
import pytest
import mysql.connector
import psycopg2
import psycopg2.extras

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GreptimeDBDriverTest:
    """Integration test suite for GreptimeDB drivers"""

    def __init__(self):
        self.conn = None
        self.driver = None
        self.connection_timezone = None

    def teardown(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def connect(self, driver_type: str, timezone: Optional[str] = None):
        """Connect to GreptimeDB using mysql or postgresql driver."""
        self.driver = driver_type
        self.connection_timezone = timezone or "UTC"

        username = self._get_env("GREPTIME_USERNAME", "")
        password = self._get_env("GREPTIME_PASSWORD", "")

        if driver_type == "mysql":
            host = self._get_env("MYSQL_HOST", "127.0.0.1")
            port = int(self._get_env("MYSQL_PORT", "4002"))
            db = self._get_env("DB_NAME", "public")

            connect_args = {
                "host": host,
                "port": port,
                "database": db,
                "user": username,
                "password": password,
                "charset": "utf8mb4",
                "use_pure": True,
            }

            if timezone:
                connect_args["time_zone"] = timezone

            logger.info(
                f"Connecting to MySQL: {host}:{port}/{db} (timezone={timezone})"
            )
            self.conn = mysql.connector.connect(**connect_args)

        elif driver_type == "postgresql":
            host = self._get_env("POSTGRES_HOST", "127.0.0.1")
            port = int(self._get_env("POSTGRES_PORT", "4003"))
            db = self._get_env("DB_NAME", "public")

            logger.info(
                f"Connecting to PostgreSQL: {host}:{port}/{db} (timezone={timezone})"
            )
            self.conn = psycopg2.connect(
                host=host, port=port, database=db, user=username, password=password
            )

            if timezone:
                with self.conn.cursor() as cursor:
                    cursor.execute(f"SET TIME ZONE = '{timezone}'")
                self.conn.commit()
        else:
            raise ValueError(f"Unknown driver: {driver_type}")

        assert self.conn is not None

    def _get_env(self, name: str, default: str) -> str:
        return os.environ.get(name, "").strip() or default

    def execute(self, cursor, sql: str):
        cursor.execute(sql)
        self.conn.commit()

    def table_name(self) -> str:
        return f"test_all_types_{self.driver}"

    def drop_table(self, table: str):
        with self.conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.conn.commit()

    def create_table(self, table: str, schema: str):
        with self.conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema})")
        self.conn.commit()

    def format_timestamp_as_utc(self, ts) -> str:
        """
        Format timestamp as UTC string.
        NOTE: mysql-connector returns naive datetimes in connection timezone -
        must localize first before converting to UTC.
        """
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

        if ts.tzinfo is None:
            if self.driver == "mysql" and self.connection_timezone:
                import pytz

                try:
                    conn_tz = pytz.timezone(self.connection_timezone)
                    ts_with_tz = conn_tz.localize(ts)
                    utc_ts = ts_with_tz.astimezone(pytz.utc).replace(tzinfo=None)
                except Exception:
                    utc_ts = ts
            else:
                utc_ts = ts
        else:
            utc_ts = ts.astimezone(timezone.utc).replace(tzinfo=None)

        return utc_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def get_cursor(self, prepared=False):
        """Get cursor. Use prepared=True for MySQL parameterized queries."""
        return (
            self.conn.cursor(prepared=True)
            if self.driver == "mysql" and prepared
            else self.conn.cursor()
        )

    def parse_binary_result(self, value, driver):
        """Parse binary data. NOTE: PostgreSQL returns hex string format."""
        if driver == "postgresql":
            if isinstance(value, memoryview):
                value = bytes(value)
            if isinstance(value, bytes):
                try:
                    value_str = value.decode("utf-8")
                    if value_str.startswith("\\\\x") or value_str.startswith("\\x"):
                        hex_str = value_str.replace("\\\\x", "").replace("\\x", "")
                        return bytes.fromhex(hex_str)
                except (UnicodeDecodeError, ValueError):
                    pass
            return value
        return value  # MySQL returns bytes directly


@pytest.fixture
def test_instance():
    instance = GreptimeDBDriverTest()
    yield instance
    instance.teardown()


@pytest.mark.parametrize("driver", ["mysql", "postgresql"])
def test_crud_operations(test_instance, driver):
    """
    Test comprehensive CRUD operations on a single table with all supported GreptimeDB data types.

    Tests:
    - CREATE TABLE with all data types
    - INSERT using SQL literals (direct SQL with values)
    - INSERT using parameterized queries (prepared statements)
    - SELECT and verify both rows
    - UPDATE by overwriting with same primary key and timestamp
    - SELECT with WHERE clause
    - DELETE (DROP TABLE)
    """
    logger.info(f"Starting CRUD test for driver: {driver}")
    test_instance.connect(driver)
    table = test_instance.table_name()

    try:
        logger.info(f"[{driver}] Dropping table if exists: {table}")
        test_instance.drop_table(table)

        logger.info(f"[{driver}] Creating table: {table}")
        test_instance.create_table(
            table,
            "ts TIMESTAMP TIME INDEX, "
            "row_id STRING PRIMARY KEY, "
            "int_col INTEGER, "
            "double_col DOUBLE, "
            "float_col FLOAT, "
            "string_col STRING, "
            "date_col DATE, "
            "timestamp_col TIMESTAMP, "
            "bool_col BOOLEAN, "
            "binary_col BINARY",
        )

        # INSERT with SQL literal
        logger.info(f"[{driver}] Inserting row with SQL literal")
        literal_insert = (
            f"INSERT INTO {table} (ts, row_id, int_col, double_col, float_col, string_col, "
            f"date_col, timestamp_col, bool_col, binary_col) "
            f"VALUES ('2024-11-24 10:00:00', 'row1', 42, 3.14159, 2.718, 'Hello GreptimeDB! 你好🚀', "
            f"'2024-01-15', '2024-11-24 10:00:00', true, X'0102030405FFFE')"
        )
        with test_instance.conn.cursor() as cursor:
            test_instance.execute(cursor, literal_insert)

        # INSERT with parameterized query (PreparedStatement equivalent)
        logger.info(f"[{driver}] Inserting row with parameterized query")
        prepared_insert = (
            f"INSERT INTO {table} (ts, row_id, int_col, double_col, float_col, string_col, "
            f"date_col, timestamp_col, bool_col, binary_col) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        ts = datetime(2024, 11, 24, 11, 0, 0)
        date_val = date(2024, 6, 20)
        binary_val = bytes([0x0A, 0x0B, 0x0C, 0xAB, 0xCD])

        with test_instance.get_cursor(prepared=True) as cursor:
            cursor.execute(
                prepared_insert,
                (
                    ts,
                    "row2",
                    999,
                    123.456,
                    78.9,
                    "PreparedStatement test 测试",
                    date_val,
                    ts,
                    False,
                    binary_val,
                ),
            )
        test_instance.conn.commit()

        # SELECT and verify
        logger.info(f"[{driver}] Selecting and verifying inserted rows")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table} ORDER BY row_id")
            rows = cursor.fetchall()

            # Verify row1
            assert len(rows) >= 1
            row1 = rows[0]
            if driver == "mysql":
                # MySQL returns tuples: (ts, row_id, int_col, ...)
                assert row1[1] == "row1"
                assert row1[2] == 42
                assert abs(row1[3] - 3.14159) < 0.00001
                assert abs(row1[4] - 2.718) < 0.001
                assert row1[5] == "Hello GreptimeDB! 你好🚀"
                assert str(row1[6]) == "2024-01-15"
                assert row1[8] == 1 or row1[8] is True
                assert row1[9] == bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0xFF, 0xFE])
            else:
                # PostgreSQL returns tuples
                assert row1[1] == "row1"
                assert row1[2] == 42
                assert abs(row1[3] - 3.14159) < 0.00001
                assert abs(row1[4] - 2.718) < 0.001
                assert row1[5] == "Hello GreptimeDB! 你好🚀"
                assert str(row1[6]) == "2024-01-15"
                assert row1[8] is True
                binary_result = test_instance.parse_binary_result(row1[9], driver)
                assert binary_result == bytes(
                    [0x01, 0x02, 0x03, 0x04, 0x05, 0xFF, 0xFE]
                )

            # Verify row2
            assert len(rows) >= 2
            row2 = rows[1]
            if driver == "mysql":
                assert row2[1] == "row2"
                assert row2[2] == 999
                assert abs(row2[3] - 123.456) < 0.001
                assert abs(row2[4] - 78.9) < 0.01
                assert row2[5] == "PreparedStatement test 测试"
                assert str(row2[6]) == "2024-06-20"
                assert row2[8] == 0 or row2[8] is False
                assert row2[9] == binary_val
            else:
                assert row2[1] == "row2"
                assert row2[2] == 999
                assert abs(row2[3] - 123.456) < 0.001
                assert abs(row2[4] - 78.9) < 0.01
                assert row2[5] == "PreparedStatement test 测试"
                assert str(row2[6]) == "2024-06-20"
                assert row2[8] is False
                binary_result = test_instance.parse_binary_result(row2[9], driver)
                assert binary_result == binary_val

            assert len(rows) == 2

        # UPDATE by inserting with SAME primary key + SAME time index
        # This should overwrite the first row (row1 at 10:00:00)
        logger.info(
            f"[{driver}] Updating row by inserting with same primary key + time index"
        )
        same_ts = datetime(2024, 11, 24, 10, 0, 0)
        update_date = date(2024, 12, 25)
        with test_instance.get_cursor(prepared=True) as cursor:
            cursor.execute(
                prepared_insert,
                (
                    same_ts,
                    "row1",
                    100,
                    9.999,
                    1.234,
                    "Updated value",
                    update_date,
                    same_ts,
                    False,
                    bytes([0x11, 0x22]),
                ),
            )
        test_instance.conn.commit()

        # Verify the row was overwritten (not added)
        logger.info(f"[{driver}] Verifying row was overwritten (not added)")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table} ORDER BY row_id, ts")
            rows = cursor.fetchall()

            # Should only have 2 rows total (row1 was overwritten, not added)
            assert (
                len(rows) == 2
            ), f"Should have exactly 2 rows (row1 overwritten, not added), got {len(rows)}"

            row1 = rows[0]
            assert row1[1] == "row1"
            assert row1[2] == 100, "row1 should be updated"
            assert abs(row1[3] - 9.999) < 0.001
            assert row1[5] == "Updated value"

            row2 = rows[1]
            assert row2[1] == "row2"
            assert row2[2] == 999

        # SELECT with WHERE
        logger.info(f"[{driver}] Testing SELECT with WHERE clause")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(
                f"SELECT * FROM {table} WHERE int_col > %s ORDER BY row_id", (50,)
            )
            rows = cursor.fetchall()
            assert len(rows) > 0

        # DELETE
        logger.info(f"[{driver}] Dropping table")
        test_instance.drop_table(table)
        with test_instance.conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES LIKE '{table}'")
            rows = cursor.fetchall()
            assert len(rows) == 0

        logger.info(f"[{driver}] CRUD test completed successfully")

    except Exception as e:
        logger.error(f"[{driver}] Test failed: {e}", exc_info=True)
        raise


@pytest.mark.parametrize("driver", ["mysql"])
def test_timezone_insert_and_select(test_instance, driver):
    """
    Test timezone behavior for INSERT and SELECT operations.
    Validates that timestamp strings are interpreted using client timezone.

    Tests:
    - Part 1: INSERT from different timezones
    - Part 2: Verify timestamps stored in UTC (connection timezone affects INSERT interpretation)
    - Part 3: Verify formatTimestampAsUtc always returns UTC regardless of connection timezone
    - Part 4: Verify WHERE clause interprets timestamp literals using connection timezone
    """
    logger.info(f"Starting timezone test for driver: {driver}")
    table = f"test_timezone_{driver}"

    try:
        # Part 1: INSERT from different timezones
        test_instance.connect(driver, "UTC")
        test_instance.drop_table(table)
        test_instance.create_table(
            table,
            "ts TIMESTAMP TIME INDEX, row_id STRING PRIMARY KEY, timezone_used STRING",
        )

        # Insert from UTC
        with test_instance.conn.cursor() as cursor:
            test_instance.execute(
                cursor,
                f"INSERT INTO {table} (ts, row_id, timezone_used) "
                f"VALUES ('2024-01-01 12:00:00', 'utc_row', 'UTC')",
            )
        test_instance.teardown()

        # Insert from Asia/Shanghai (UTC+8)
        test_instance.connect(driver, "Asia/Shanghai")
        with test_instance.conn.cursor() as cursor:
            test_instance.execute(
                cursor,
                f"INSERT INTO {table} (ts, row_id, timezone_used) "
                f"VALUES ('2024-01-01 12:00:00', 'shanghai_row', 'Asia/Shanghai')",
            )
        test_instance.teardown()

        # Insert from America/New_York (UTC-5)
        test_instance.connect(driver, "America/New_York")
        with test_instance.conn.cursor() as cursor:
            test_instance.execute(
                cursor,
                f"INSERT INTO {table} (ts, row_id, timezone_used) "
                f"VALUES ('2024-01-01 12:00:00', 'newyork_row', 'America/New_York')",
            )
        test_instance.teardown()

        # Part 2: Verify timestamps stored in UTC
        # (connection timezone affects INSERT interpretation)
        test_instance.connect(driver, "UTC")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(
                f"SELECT row_id, ts, timezone_used FROM {table} ORDER BY row_id"
            )
            rows = cursor.fetchall()

            # New York: 2024-01-01 12:00:00 (local) -> 2024-01-01 17:00:00 (UTC)
            assert rows[0][0] == "newyork_row"
            assert rows[0][2] == "America/New_York"
            ny_ts = test_instance.format_timestamp_as_utc(rows[0][1])
            assert ny_ts.startswith(
                "2024-01-01 17:00:00"
            ), f"Expected UTC 17:00:00, got: {ny_ts}"

            # Shanghai: 2024-01-01 12:00:00 (local) -> 2024-01-01 04:00:00 (UTC)
            assert rows[1][0] == "shanghai_row"
            assert rows[1][2] == "Asia/Shanghai"
            shanghai_ts = test_instance.format_timestamp_as_utc(rows[1][1])
            assert shanghai_ts.startswith(
                "2024-01-01 04:00:00"
            ), f"Expected UTC 04:00:00, got: {shanghai_ts}"

            # UTC: 2024-01-01 12:00:00 (local) -> 2024-01-01 12:00:00 (UTC)
            assert rows[2][0] == "utc_row"
            assert rows[2][2] == "UTC"
            utc_ts = test_instance.format_timestamp_as_utc(rows[2][1])
            assert utc_ts.startswith(
                "2024-01-01 12:00:00"
            ), f"Expected UTC 12:00:00, got: {utc_ts}"

            assert len(rows) == 3
        test_instance.teardown()

        # Part 3: Verify formatTimestampAsUtc always returns UTC regardless of connection timezone
        test_instance.connect(driver, "Asia/Shanghai")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(f"SELECT ts FROM {table} WHERE row_id = 'utc_row'")
            row = cursor.fetchone()
            ts_in_shanghai = test_instance.format_timestamp_as_utc(row[0])
            assert ts_in_shanghai.startswith(
                "2024-01-01 12:00:00"
            ), f"Expected UTC 12:00:00, got: {ts_in_shanghai}"
        test_instance.teardown()

        test_instance.connect(driver, "America/New_York")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(f"SELECT ts FROM {table} WHERE row_id = 'utc_row'")
            row = cursor.fetchone()
            ts_in_ny = test_instance.format_timestamp_as_utc(row[0])
            assert ts_in_ny.startswith(
                "2024-01-01 12:00:00"
            ), f"Expected UTC 12:00:00, got: {ts_in_ny}"
        test_instance.teardown()

        # Part 4: Verify WHERE clause interprets timestamp literals using connection timezone
        test_instance.connect(driver, "Asia/Shanghai")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(
                f"SELECT row_id FROM {table} WHERE ts = '2024-01-01 12:00:00'"
            )
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "shanghai_row"
        test_instance.teardown()

        test_instance.connect(driver, "UTC")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(
                f"SELECT row_id FROM {table} WHERE ts = '2024-01-01 12:00:00'"
            )
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "utc_row"

        test_instance.drop_table(table)
        logger.info(f"[{driver}] Timezone test completed successfully")

    except Exception as e:
        logger.error(f"[{driver}] Timezone test failed: {e}", exc_info=True)
        # Cleanup on failure
        try:
            if (
                test_instance.conn and not test_instance.conn.closed
                if hasattr(test_instance.conn, "closed")
                else True
            ):
                test_instance.drop_table(table)
        except Exception as cleanup_ex:
            logger.warning(f"[{driver}] Cleanup failed: {cleanup_ex}")
        raise


@pytest.mark.parametrize("driver", ["mysql", "postgresql"])
def test_batch_insert(test_instance, driver):
    """
    Test batch insert using executemany().
    Validates that batch operations work correctly for both MySQL and PostgreSQL drivers.

    Tests:
    - Add 5 rows to batch
    - Execute batch insert
    - Verify batch execution results
    - Query and validate all inserted rows
    """
    logger.info(f"Starting batch insert test for driver: {driver}")
    test_instance.connect(driver)
    table = f"test_batch_insert_{driver}"

    try:
        logger.info(f"[{driver}] Dropping table if exists: {table}")
        test_instance.drop_table(table)

        logger.info(f"[{driver}] Creating table: {table}")
        test_instance.create_table(
            table,
            "ts TIMESTAMP TIME INDEX, "
            "row_id STRING PRIMARY KEY, "
            "int_col INTEGER, "
            "double_col DOUBLE, "
            "string_col STRING, "
            "bool_col BOOLEAN",
        )

        # Prepare batch insert with 5 rows
        batch_size = 5
        logger.info(f"[{driver}] Preparing batch insert with {batch_size} rows")

        insert_sql = (
            f"INSERT INTO {table} (ts, row_id, int_col, double_col, string_col, bool_col) "
            f"VALUES (%s, %s, %s, %s, %s, %s)"
        )

        # Prepare batch data
        batch_data = []
        for i in range(1, batch_size + 1):
            ts = datetime(2024, 11, 24, 10 + i, 0, 0)
            row_id = f"batch_row_{i}"
            int_col = i * 100
            double_col = i * 1.5
            string_col = f"Batch test row {i} 测试"
            bool_col = i % 2 == 0

            batch_data.append((ts, row_id, int_col, double_col, string_col, bool_col))
            logger.info(f"[{driver}] Added row {i} to batch: {row_id}, ts={ts}")

        # Execute batch
        logger.info(f"[{driver}] Executing batch insert")
        with test_instance.get_cursor(prepared=True) as cursor:
            cursor.executemany(insert_sql, batch_data)
        test_instance.conn.commit()
        logger.info(f"[{driver}] Batch execution completed")

        # Verify all rows were inserted correctly
        logger.info(f"[{driver}] Verifying inserted rows")
        with test_instance.conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table} ORDER BY row_id")
            rows = cursor.fetchall()

            row_count = 0
            for row in rows:
                row_count += 1
                row_id = row[1]
                int_col = row[2]
                double_col = row[3]
                string_col = row[4]
                bool_col = row[5]

                logger.info(
                    f"[{driver}] Row {row_count}: {row_id} | int={int_col}, "
                    f"double={double_col}, string={string_col}, bool={bool_col}"
                )

                # Verify data matches what we inserted
                assert row_id == f"batch_row_{row_count}"
                assert int_col == row_count * 100
                assert abs(double_col - row_count * 1.5) < 0.01
                assert string_col == f"Batch test row {row_count} 测试"
                expected_bool = row_count % 2 == 0
                if driver == "mysql":
                    assert (
                        bool_col == (1 if expected_bool else 0)
                        or bool_col == expected_bool
                    )
                else:
                    assert bool_col == expected_bool

            assert (
                row_count == batch_size
            ), f"Should have inserted exactly {batch_size} rows"
            logger.info(
                f"[{driver}] ✓ Verified all {batch_size} rows were inserted correctly"
            )

        # Cleanup
        logger.info(f"[{driver}] Dropping table")
        test_instance.drop_table(table)

        logger.info(f"[{driver}] Batch insert test completed successfully")

    except Exception as e:
        logger.error(f"[{driver}] Batch insert test failed: {e}", exc_info=True)
        # Cleanup on failure
        try:
            test_instance.drop_table(table)
        except Exception as cleanup_ex:
            logger.warning(f"[{driver}] Cleanup failed: {cleanup_ex}")
        raise
