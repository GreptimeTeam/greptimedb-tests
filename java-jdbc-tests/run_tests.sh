#!/bin/bash
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

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get database name from environment or derive from directory name
if [ -z "$DB_NAME" ]; then
    DIR_NAME=$(basename "$SCRIPT_DIR")
    DB_NAME=$(echo "$DIR_NAME" | tr '-' '_')
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo_error "Maven is not installed. Please install Maven first."
    exit 1
fi

# Set JAVA_HOME to Java 11 or later if available
if [ -x /usr/libexec/java_home ]; then
    # On macOS, find Java 11 or later
    JAVA_11_HOME=$(/usr/libexec/java_home -v 11+ 2>/dev/null)
    if [ -n "$JAVA_11_HOME" ]; then
        export JAVA_HOME="$JAVA_11_HOME"
        export PATH="$JAVA_HOME/bin:$PATH"
        echo_info "Using Java from: $JAVA_HOME"
    fi
fi

# Check if Java is installed and is version 11+
if ! command -v java &> /dev/null; then
    echo_error "Java is not installed. Please install Java 11 or later."
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')
echo_info "Java version: $JAVA_VERSION"

# Check if GreptimeDB is running by checking the health endpoint
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-4002}"
POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
POSTGRES_PORT="${POSTGRES_PORT:-4003}"
HTTP_HOST="${HTTP_HOST:-127.0.0.1}"
HTTP_PORT="${HTTP_PORT:-4000}"

if ! curl -s "http://${HTTP_HOST}:${HTTP_PORT}/health" > /dev/null 2>&1; then
    echo_error "GreptimeDB is not running on ${HTTP_HOST}:${HTTP_PORT}"
    echo_error "Please start GreptimeDB first with:"
    echo_error "  cargo run --bin greptime -- standalone start"
    exit 1
fi

echo_info "GreptimeDB is running"
echo_info "Using database: $DB_NAME"

# Set environment variables for tests
export DB_NAME
export MYSQL_HOST
export MYSQL_PORT
export POSTGRES_HOST
export POSTGRES_PORT
export MYSQL_URL="${MYSQL_URL:-jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${DB_NAME}}"
export POSTGRES_URL="${POSTGRES_URL:-jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${DB_NAME}}"

# Run Maven tests
echo_info "Running JDBC integration tests..."
cd "$SCRIPT_DIR"

if mvn clean test; then
    echo_info "All tests passed successfully!"
    exit 0
else
    echo_error "Some tests failed!"
    exit 1
fi
