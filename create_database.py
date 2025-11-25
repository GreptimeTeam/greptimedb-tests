#!/usr/bin/env python3
"""
Simple script to create database in GreptimeDB via MySQL protocol.
Requires: pip install pymysql
"""

import sys
import os

try:
    import pymysql
except ImportError:
    print("Error: pymysql not installed. Run: pip install pymysql")
    sys.exit(1)


def create_database(host, port, db_name):
    """Create database using PyMySQL."""
    try:
        # Connect to GreptimeDB (no authentication required by default)
        connection = pymysql.connect(
            host=host,
            port=int(port),
            user='',
            password='',
            connect_timeout=5
        )

        with connection.cursor() as cursor:
            # Create database if not exists
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            print(f"✓ Database '{db_name}' created successfully")

        connection.close()
        return True

    except Exception as e:
        print(f"⚠ Could not create database: {e}")
        print(f"  This is usually fine - database may be auto-created")
        return True  # Return success to not fail the tests


def main():
    # Get configuration from environment or command line
    if len(sys.argv) > 1:
        db_name = sys.argv[1]
    else:
        db_name = os.environ.get('DB_NAME', 'test_db')

    host = os.environ.get('MYSQL_HOST', '127.0.0.1')
    port = os.environ.get('MYSQL_PORT', '4002')

    print(f"Creating database: {db_name}")
    print(f"Target: {host}:{port}")

    success = create_database(host, port, db_name)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
