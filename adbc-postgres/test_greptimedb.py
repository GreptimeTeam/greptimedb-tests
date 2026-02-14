import os
from datetime import datetime

import pandas as pd
import adbc_driver_postgresql.dbapi
import adbc_driver_postgresql


# export ADBC_POSTGRESQL_TEST_URI="postgresql://bob:123@127.0.0.1:4003/public"
uri = "postgresql://bob:123@127.0.0.1:4003/public"

conn = adbc_driver_postgresql.dbapi.connect(uri)

with conn.cursor(
    adbc_stmt_kwargs={
        adbc_driver_postgresql.StatementOptions.USE_COPY.value: True,
    }
) as cur:
    # 1️⃣ Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_logs (
          ts TIMESTAMP TIME INDEX,
          host STRING INVERTED INDEX,
          api_path STRING,
          log_level STRING,
          log_msg STRING FULLTEXT INDEX WITH('case_sensitive' = 'false'),
          PRIMARY KEY (host, log_level)
        ) WITH('append_mode'='true');
    """)

    # 2️⃣ Insert via bind + executemany (PG-compatible path)
    insert_sql = """
        INSERT INTO app_logs (ts, host, api_path, log_level, log_msg)
        VALUES ($1, $2, $3, $4, $5)
    """

    data = [
        (datetime(2024, 7, 11, 20, 2, 0), "host4", "/order", "INFO", "order ok"),
        (datetime(2024, 7, 11, 20, 2, 1), "host4", "/order", "ERROR", "order failed"),
        (datetime(2024, 7, 11, 20, 2, 2), "host5", "/cart", "INFO", "cart ok"),
    ]

    cur.executemany(insert_sql, data)

    # 3 SQL query verification (DBAPI path)
    cur.execute("""
        SELECT ts, host, api_path, log_level, log_msg
        FROM app_logs
        ORDER BY ts, host
    """)

    print("\n=== Query results via DBAPI ===")
    for r in cur.fetchall():
        print(r)

    # 4 === pandas query via ADBC connection ===
    print("\n=== Query via pandas.read_sql ===")

    df = pd.read_sql(
        "SELECT * FROM app_logs WHERE log_level = 'INFO'",
        conn,
    )

    print(df)

conn.close()
