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
Integration tests for ADBC PostgreSQL driver with pandas.
Tests table creation, batch inserts using COPY protocol, and DataFrame verification.
"""

import os
import logging
from datetime import datetime
import pytest
import pandas as pd
import adbc_driver_postgresql.dbapi
import adbc_driver_postgresql

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ADBCPostgreSQLTest:
    """Integration test suite for ADBC PostgreSQL driver"""

    def __init__(self):
        self.conn = None
        self.uri = None

    def setup(self):
        """Setup ADBC connection."""
        username = self._get_env("GREPTIME_USERNAME", "bob")
        password = self._get_env("GREPTIME_PASSWORD", "123")
        host = self._get_env("POSTGRES_HOST", "127.0.0.1")
        port = int(self._get_env("POSTGRES_PORT", "4003"))
        db = self._get_env("DB_NAME", "public")

        self.uri = f"postgresql://{username}:{password}@{host}:{port}/{db}"
        logger.info(f"Connecting to ADBC PostgreSQL: {host}:{port}/{db}")
        self.conn = adbc_driver_postgresql.dbapi.connect(self.uri)

    def teardown(self):
        """Teardown connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _get_env(self, name: str, default: str) -> str:
        return os.environ.get(name, "").strip() or default

    def table_name(self) -> str:
        return "test_adbc_app_logs"

    def drop_table(self, table: str):
        """Drop table if exists."""
        with self.conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.conn.commit()
        logger.info(f"Dropped table: {table}")

    def create_table(self, table: str, schema: str):
        """Create table with schema."""
        with self.conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema})")
        self.conn.commit()
        logger.info(f"Created table: {table}")


@pytest.fixture
def test_instance():
    instance = ADBCPostgreSQLTest()
    instance.setup()
    yield instance
    instance.teardown()


def test_adbc_postgres_crud(test_instance):
    """
    Test ADBC PostgreSQL driver with pandas DataFrame verification.
    Tests:
    - CREATE TABLE with indexes
    - INSERT via executemany with COPY protocol
    - SELECT and verify results via DBAPI
    - Query via pandas.read_sql and verify DataFrame values
    - DROP TABLE for cleanup
    """
    table = test_instance.table_name()

    try:
        logger.info(f"Dropping table if exists: {table}")
        test_instance.drop_table(table)

        logger.info(f"Creating table: {table}")
        test_instance.create_table(
            table,
            "ts TIMESTAMP TIME INDEX, "
            "host STRING INVERTED INDEX, "
            "api_path STRING, "
            "log_level STRING, "
            "log_msg STRING FULLTEXT INDEX WITH('case_sensitive' = 'false'), "
            "PRIMARY KEY (host, log_level)",
        )

        logger.info("Inserting rows via executemany with COPY protocol")
        insert_sql = f"""INSERT INTO {table} (ts, host, api_path, log_level, log_msg)
        VALUES ($1, $2, $3, $4, $5)"""

        data = [
            (datetime(2024, 7, 11, 20, 2, 0), "host4", "/order", "INFO", "order ok"),
            (
                datetime(2024, 7, 11, 20, 2, 1),
                "host4",
                "/order",
                "ERROR",
                "order failed",
            ),
            (datetime(2024, 7, 11, 20, 2, 2), "host5", "/cart", "INFO", "cart ok"),
        ]

        with test_instance.conn.cursor(
            adbc_stmt_kwargs={
                adbc_driver_postgresql.StatementOptions.USE_COPY.value: True
            }
        ) as cur:
            cur.executemany(insert_sql, data)
        test_instance.conn.commit()
        logger.info(f"Inserted {len(data)} rows")

        logger.info("Querying and verifying results via DBAPI")
        with test_instance.conn.cursor() as cur:
            cur.execute(
                f"SELECT ts, host, api_path, log_level, log_msg FROM {table} ORDER BY ts, host"
            )
            rows = cur.fetchall()

            assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"

            row0 = rows[0]
            assert row0[1] == "host4"
            assert row0[2] == "/order"
            assert row0[3] == "INFO"

            row1 = rows[1]
            assert row1[1] == "host4"
            assert row1[2] == "/order"
            assert row1[3] == "ERROR"

            row2 = rows[2]
            assert row2[1] == "host5"
            assert row2[2] == "/cart"
            assert row2[3] == "INFO"

        logger.info("Querying via pandas.read_sql and verifying DataFrame")
        df = pd.read_sql(
            f"""SELECT ts, host, api_path, log_level, log_msg FROM {table}
            WHERE log_level = 'INFO' ORDER BY ts, host""",
            test_instance.conn,
        )

        logger.info(f"DataFrame:\n{df}")

        assert len(df) == 2, f"Expected 2 INFO rows, got {len(df)}"
        assert list(df.columns) == ["ts", "host", "api_path", "log_level", "log_msg"]

        assert df.iloc[0]["host"] == "host4"
        assert df.iloc[0]["api_path"] == "/order"
        assert df.iloc[0]["log_level"] == "INFO"
        assert df.iloc[0]["log_msg"] == "order ok"

        assert df.iloc[1]["host"] == "host5"
        assert df.iloc[1]["api_path"] == "/cart"
        assert df.iloc[1]["log_level"] == "INFO"
        assert df.iloc[1]["log_msg"] == "cart ok"

        logger.info("Dropping table for cleanup")
        test_instance.drop_table(table)

        logger.info("ADBC PostgreSQL CRUD test completed successfully")

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        test_instance.drop_table(table)
        raise
