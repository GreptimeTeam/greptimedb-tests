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
# When run standalone: otel_tests_nodejs
# When run from parent run_tests.sh: DB_NAME is already set
if [ -z "$DB_NAME" ]; then
    LANG_NAME=$(basename "$SCRIPT_DIR")
    DB_NAME="otel_tests_${LANG_NAME}"
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

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo_error "Node.js is not installed. Please install Node.js 18 or later."
    exit 1
fi

NODE_VERSION=$(node --version)
echo_info "Node.js version: $NODE_VERSION"

# Check if npm is installed
if ! command -v npm &> /dev/null; then
    echo_error "npm is not installed. Please install npm."
    exit 1
fi

NPM_VERSION=$(npm --version)
echo_info "npm version: $NPM_VERSION"

# Configuration from environment
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-4002}"
HTTP_HOST="${HTTP_HOST:-127.0.0.1}"
HTTP_PORT="${HTTP_PORT:-4000}"

# Check if GreptimeDB is running by checking the health endpoint
if ! curl -s "http://${HTTP_HOST}:${HTTP_PORT}/health" > /dev/null 2>&1; then
    echo_error "GreptimeDB is not running on ${HTTP_HOST}:${HTTP_PORT}"
    echo_error "Please start GreptimeDB first with:"
    echo_error "  cargo run --bin greptime -- standalone start"
    exit 1
fi

echo_info "GreptimeDB is running"
echo_info "Using database: $DB_NAME"
echo_info "OTLP endpoint: http://${HTTP_HOST}:${HTTP_PORT}/v1/otlp"

# Set environment variables for tests
export DB_NAME
export MYSQL_HOST
export MYSQL_PORT
export HTTP_HOST
export HTTP_PORT

# Install dependencies
echo_info "Installing Node.js dependencies..."
cd "$SCRIPT_DIR"
npm install --silent

# Run Jest tests
echo_info "Running OpenTelemetry integration tests (Node.js)..."

if npm test; then
    echo_info "All Node.js OpenTelemetry tests passed successfully!"
    exit 0
else
    echo_error "Some Node.js OpenTelemetry tests failed!"
    exit 1
fi
