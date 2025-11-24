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

echo_section() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

# Check if GreptimeDB is running by checking the health endpoint
HTTP_PORT="${HTTP_PORT:-4000}"

if ! curl -s "http://localhost:${HTTP_PORT}/health" > /dev/null 2>&1; then
    echo_error "GreptimeDB is not running on port ${HTTP_PORT}"
    echo_error "Please start GreptimeDB first with:"
    echo_error "  cargo run --bin greptime -- standalone start"
    exit 1
fi

echo_info "GreptimeDB is running on port ${HTTP_PORT}"

# Arrays to track test results
declare -a PASSED_TESTS
declare -a FAILED_TESTS

# Find all test directories (directories containing run_tests.sh or run.sh)
echo_section "Discovering Test Suites"

TEST_DIRS=()
for dir in "$SCRIPT_DIR"/*/; do
    if [ -d "$dir" ]; then
        dir_name=$(basename "$dir")

        # Skip hidden directories and common non-test directories
        if [[ "$dir_name" == .* ]] || [[ "$dir_name" == "target" ]] || [[ "$dir_name" == "build" ]]; then
            continue
        fi

        # Look for run_tests.sh or run.sh
        if [ -f "$dir/run_tests.sh" ] || [ -f "$dir/run.sh" ]; then
            TEST_DIRS+=("$dir")
            echo_info "Found test suite: $dir_name"
        fi
    fi
done

if [ ${#TEST_DIRS[@]} -eq 0 ]; then
    echo_warning "No test suites found"
    echo_info "Test suites should contain either 'run_tests.sh' or 'run.sh'"
    exit 0
fi

echo_info "Total test suites found: ${#TEST_DIRS[@]}"

# Run tests in each directory
echo_section "Running Test Suites"

for test_dir in "${TEST_DIRS[@]}"; do
    dir_name=$(basename "$test_dir")

    echo_section "Running: $dir_name"

    # Create database for this test suite (convert hyphens to underscores)
    db_name=$(echo "$dir_name" | tr '-' '_')
    echo_info "Creating database: $db_name"

    MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
    MYSQL_PORT="${MYSQL_PORT:-4002}"
    if command -v mysql &> /dev/null; then
        mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -e "CREATE DATABASE IF NOT EXISTS ${db_name};" 2>/dev/null || {
            echo_warning "Failed to create database, will rely on auto-creation"
        }
    else
        echo_warning "mysql client not found, will rely on auto-creation"
    fi

    # Export environment variables for the test suite
    export DB_NAME="$db_name"
    export MYSQL_HOST
    export MYSQL_PORT
    export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
    export POSTGRES_PORT="${POSTGRES_PORT:-4003}"

    # Determine which script to run
    if [ -f "$test_dir/run_tests.sh" ]; then
        test_script="$test_dir/run_tests.sh"
    else
        test_script="$test_dir/run.sh"
    fi

    # Make sure the script is executable
    chmod +x "$test_script"

    # Run the test script in a subshell to avoid changing working directory
    # The script must be run from its own directory
    script_name="$(basename "$test_script")"
    if (cd "$test_dir" && bash "./$script_name"); then
        PASSED_TESTS+=("$dir_name")
        echo_info "✓ $dir_name: PASSED"
    else
        FAILED_TESTS+=("$dir_name")
        echo_error "✗ $dir_name: FAILED"
    fi

    echo ""
done

# Print summary
echo_section "Test Summary"

echo_info "Passed: ${#PASSED_TESTS[@]}"
for test in "${PASSED_TESTS[@]}"; do
    echo -e "  ${GREEN}✓${NC} $test"
done

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    echo ""
    echo_error "Failed: ${#FAILED_TESTS[@]}"
    for test in "${FAILED_TESTS[@]}"; do
        echo -e "  ${RED}✗${NC} $test"
    done
fi

echo ""
echo_section "Final Result"

if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo_info "All test suites passed! (${#PASSED_TESTS[@]}/${#TEST_DIRS[@]})"
    exit 0
else
    echo_error "Some test suites failed! (${#FAILED_TESTS[@]}/${#TEST_DIRS[@]} failed)"
    exit 1
fi
