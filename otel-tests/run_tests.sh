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

# OpenTelemetry Integration Tests Runner (Node.js only)
# Note: Java, Go, and Python OTel tests have been merged into their respective
# language test directories (java-tests, go-tests, python-tests).
# This script only runs the Node.js OTel tests.
# Database name: otel_tests_nodejs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

echo_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

# Configuration from environment
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-4002}"
HTTP_HOST="${HTTP_HOST:-127.0.0.1}"
HTTP_PORT="${HTTP_PORT:-4000}"

# Check if GreptimeDB is running
echo_header "Checking GreptimeDB Status"

if ! curl -s "http://${HTTP_HOST}:${HTTP_PORT}/health" > /dev/null 2>&1; then
    echo_error "GreptimeDB is not running on ${HTTP_HOST}:${HTTP_PORT}"
    echo_error "Please start GreptimeDB first with:"
    echo_error "  cargo run --bin greptime -- standalone start"
    echo_error ""
    echo_error "With authentication:"
    echo_error "  cargo run --bin greptime -- standalone start \\"
    echo_error "    --user-provider=static_user_provider:cmd:greptime_user=greptime_pwd"
    exit 1
fi

echo_info "GreptimeDB is running"
echo_info "OTLP endpoint: http://${HTTP_HOST}:${HTTP_PORT}/v1/otlp"
echo_info "MySQL endpoint: ${MYSQL_HOST}:${MYSQL_PORT}"

# Export environment variables for test suites
export MYSQL_HOST
export MYSQL_PORT
export HTTP_HOST
export HTTP_PORT

# Test suite directories (only nodejs remains here)
TEST_SUITES=("nodejs")

# Track results
PASSED=0
FAILED=0
SKIPPED=0

# Run each test suite
for suite in "${TEST_SUITES[@]}"; do
    suite_dir="$SCRIPT_DIR/$suite"

    if [ -d "$suite_dir" ]; then
        if [ -f "$suite_dir/run_tests.sh" ]; then
            echo_header "Running $suite OpenTelemetry Tests"

            # Create isolated database for this language
            # Database name: otel_tests_<language>
            SUITE_DB_NAME="otel_tests_${suite}"
            echo_info "Creating database: $SUITE_DB_NAME"

            # Create database using the root create_database.py script
            if [ -f "$ROOT_DIR/create_database.py" ]; then
                python3 "$ROOT_DIR/create_database.py" "$SUITE_DB_NAME" || true
            else
                echo_warning "create_database.py not found, assuming database exists"
            fi

            # Export DB_NAME for this test suite
            export DB_NAME="$SUITE_DB_NAME"
            echo_info "Using database: $DB_NAME"

            # Make script executable if not already
            chmod +x "$suite_dir/run_tests.sh"

            # Run the test suite
            if "$suite_dir/run_tests.sh"; then
                echo_info "$suite tests: PASSED"
                ((PASSED++))
            else
                echo_error "$suite tests: FAILED"
                ((FAILED++))
            fi
        else
            echo_warning "No run_tests.sh found in $suite_dir, skipping..."
            ((SKIPPED++))
        fi
    else
        echo_warning "Directory $suite_dir does not exist, skipping..."
        ((SKIPPED++))
    fi
done

# Print summary
echo_header "Test Results Summary"
echo_info "Passed:  $PASSED"
if [ $FAILED -gt 0 ]; then
    echo_error "Failed:  $FAILED"
else
    echo_info "Failed:  $FAILED"
fi
if [ $SKIPPED -gt 0 ]; then
    echo_warning "Skipped: $SKIPPED"
else
    echo_info "Skipped: $SKIPPED"
fi
echo ""

# Exit with appropriate code
if [ $FAILED -gt 0 ]; then
    echo_error "Some test suites failed!"
    exit 1
else
    echo_info "All OpenTelemetry tests passed successfully!"
    exit 0
fi
