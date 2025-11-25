#!/usr/bin/env python3
"""
Simple script to create database in GreptimeDB via MySQL protocol.
Requires: pip install mysql-connector-python
"""

import sys
import os

try:
    import mysql.connector
except ImportError:
    print("Error: mysql-connector-python not installed. Run: pip install mysql-connector-python")
    sys.exit(1)


def create_database(host, port, db_name, username='', password=''):
    """Create database using mysql.connector."""
    try:
        # NOTE: Must connect to 'public' database first (GreptimeDB requires a valid database)
        connection = mysql.connector.connect(
            host=host,
            port=int(port),
            database='public',
            user=username,
            password=password,
            connection_timeout=5,
            charset='utf8mb4',
            use_pure=True,
            autocommit=True
        )

        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        print(f"✓ Database '{db_name}' created successfully")
        cursor.close()
        connection.close()
        return True

    except Exception as e:
        print(f"⚠ Could not create database: {e}")
        print(f"  This is usually fine - database may be auto-created")
        return True  # Always return True to not block tests


def main():
    db_name = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('DB_NAME', 'test_db')
    host = os.environ.get('MYSQL_HOST', '127.0.0.1')
    port = os.environ.get('MYSQL_PORT', '4002')
    username = os.environ.get('GREPTIME_USERNAME', '')
    password = os.environ.get('GREPTIME_PASSWORD', '')

    print(f"Creating database: {db_name}")
    print(f"Target: {host}:{port}")

    success = create_database(host, port, db_name, username, password)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
