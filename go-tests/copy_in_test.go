// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for GreptimeDB PostgreSQL COPY ... FROM STDIN (the copy-in
// sub-protocol, greptimedb#9297) through lib/pq.
//
// lib/pq dispatches any statement starting with "COPY" to its copy-in
// implementation: it sends the statement verbatim over the simple query
// protocol, then streams tab-delimited rows sent via stmt.Exec.
// GreptimeDB currently implements only WITH (FORMAT csv), so the tests
// declare the tab character (E'\t') as the CSV delimiter. lib/pq always
// encodes NULL as the literal "\N" and escapes backslashes/tabs in the
// PostgreSQL text style, which the CSV decoder does not understand, so
// rows streamed through stmt.Exec must avoid NULLs and special
// characters.

// TestPostgresCopyInCsv streams rows through COPY ... FROM STDIN with
// (FORMAT csv, DELIMITER E'\t') and verifies the data written.
func TestPostgresCopyInCsv(t *testing.T) {
	db := connectDriver(t, "postgresql", "")
	defer db.Close()

	tableName := "test_copy_in_metrics_pg"
	dropTableDriver(t, db, tableName)

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		host STRING,
		val DOUBLE,
		note STRING,
		ts TIMESTAMP,
		TIME INDEX(ts)
	)`, tableName)
	_, err := db.Exec(createSQL)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	// lib/pq requires COPY to run inside a transaction.
	stmt, err := tx.Prepare(fmt.Sprintf(
		"COPY %s (host, val, note, ts) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')", tableName))
	require.NoError(t, err)

	rows := []struct {
		host string
		val  float64
		note string
		ts   time.Time
	}{
		{"host1", 1.5, "hello", time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)},
		{"host2", 2.5, "plain", time.Date(2023, 11, 14, 22, 13, 21, 0, time.UTC)},
		{"host3", 3.5, "last", time.Date(2023, 11, 14, 22, 13, 22, 0, time.UTC)},
	}
	for _, r := range rows {
		_, err = stmt.Exec(r.host, r.val, r.note, r.ts)
		require.NoError(t, err)
	}

	// Exec with no arguments finishes the copy (sends CopyDone). Errors
	// detected by the server surface here.
	_, err = stmt.Exec()
	require.NoError(t, err)
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	// lib/pq reports 0 rows affected for COPY, so verify through SELECT.
	var count int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count))
	assert.Equal(t, len(rows), count)

	var host, note string
	var val float64
	var ts time.Time
	require.NoError(t, db.QueryRow(
		fmt.Sprintf("SELECT host, val, note, ts FROM %s WHERE host = 'host1'", tableName)).
		Scan(&host, &val, &note, &ts))
	assert.Equal(t, "host1", host)
	assert.InDelta(t, 1.5, val, 0.0001)
	assert.Equal(t, "hello", note)
	assert.Equal(t, "2023-11-14 22:13:20", ts.UTC().Format("2006-01-02 15:04:05"))

	dropTableDriver(t, db, tableName)
}

// TestPostgresCopyInLargeBatch copies more rows than one server flush
// batch (32768) in a single copy stream.
func TestPostgresCopyInLargeBatch(t *testing.T) {
	db := connectDriver(t, "postgresql", "")
	defer db.Close()

	tableName := "test_copy_in_large_pg"
	dropTableDriver(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (host STRING, seq BIGINT, ts TIMESTAMP, TIME INDEX(ts))", tableName))
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	stmt, err := tx.Prepare(fmt.Sprintf(
		"COPY %s (host, seq, ts) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')", tableName))
	require.NoError(t, err)

	total := 40000
	for i := 0; i < total; i++ {
		// A unique millisecond timestamp per row: without primary keys a
		// repeated time index would upsert instead of appending.
		_, err = stmt.Exec(
			fmt.Sprintf("host_%d", i),
			int64(i),
			time.Date(2024, 1, 1, 0, 0, 0, i*1_000_000, time.UTC))
		require.NoError(t, err)
	}
	_, err = stmt.Exec()
	require.NoError(t, err)
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	var count int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count))
	assert.Equal(t, total, count)

	var first, last int64
	require.NoError(t, db.QueryRow(
		fmt.Sprintf("SELECT seq FROM %s WHERE host = 'host_0'", tableName)).Scan(&first))
	require.NoError(t, db.QueryRow(
		fmt.Sprintf("SELECT seq FROM %s WHERE host = 'host_%d'", tableName, total-1)).Scan(&last))
	assert.Equal(t, int64(0), first)
	assert.Equal(t, int64(total-1), last)

	dropTableDriver(t, db, tableName)
}

// TestPostgresCopyInStatementRejections covers statement-level failures:
// COPY without an explicit format (what pq.CopyIn produces), the
// unimplemented PostgreSQL text/binary formats, unknown tables and
// options the csv format does not understand.
func TestPostgresCopyInStatementRejections(t *testing.T) {
	db := connectDriver(t, "postgresql", "")
	defer db.Close()

	tableName := "test_copy_in_reject_pg"
	dropTableDriver(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (host STRING, ts TIMESTAMP, TIME INDEX(ts))", tableName))
	require.NoError(t, err)

	// pq.CopyIn builds a bare "COPY ... FROM STDIN" without any format,
	// which GreptimeDB rejects with guidance in this version.
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Prepare(pq.CopyIn(tableName, "host", "ts"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires an explicit format")
	require.NoError(t, tx.Rollback())

	// The PostgreSQL text and binary formats are not implemented yet.
	for _, format := range []string{"text", "txt", "binary"} {
		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Prepare(fmt.Sprintf(
			"COPY %s FROM STDIN WITH (FORMAT %s)", tableName, format))
		require.Error(t, err, "FORMAT %s should be rejected", format)
		assert.Contains(t, err.Error(), "not supported")
		require.NoError(t, tx.Rollback())
	}

	// Unknown table.
	tx, err = db.Begin()
	require.NoError(t, err)
	_, err = tx.Prepare("COPY copy_in_does_not_exist FROM STDIN WITH (FORMAT csv)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
	require.NoError(t, tx.Rollback())

	// Options the csv format does not understand.
	tx, err = db.Begin()
	require.NoError(t, err)
	_, err = tx.Prepare(fmt.Sprintf(
		"COPY %s FROM STDIN WITH (FORMAT csv, PRECISION 'ns')", tableName))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported by format csv")
	require.NoError(t, tx.Rollback())

	// COPY FROM STDIN must run alone.
	tx, err = db.Begin()
	require.NoError(t, err)
	_, err = tx.Prepare(fmt.Sprintf(
		"COPY %s FROM STDIN WITH (FORMAT csv); SELECT 1", tableName))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "alone")
	require.NoError(t, tx.Rollback())

	// A column list omitting the time index column is rejected.
	tx, err = db.Begin()
	require.NoError(t, err)
	_, err = tx.Prepare(fmt.Sprintf(
		"COPY %s (host) FROM STDIN WITH (FORMAT csv)", tableName))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "time index")
	require.NoError(t, tx.Rollback())

	dropTableDriver(t, db, tableName)
}

// TestPostgresCopyInBadDataKeepsTableClean verifies that a copy aborted
// by bad data writes nothing and leaves the connection usable for
// another COPY.
func TestPostgresCopyInBadDataKeepsTableClean(t *testing.T) {
	db := connectDriver(t, "postgresql", "")
	defer db.Close()

	tableName := "test_copy_in_bad_pg"
	dropTableDriver(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (host STRING, val DOUBLE, ts TIMESTAMP, TIME INDEX(ts))", tableName))
	require.NoError(t, err)

	// The second row carries a value that cannot parse as DOUBLE; the
	// error surfaces when the copy is finalized.
	tx, err := db.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(fmt.Sprintf(
		"COPY %s (host, val, ts) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')", tableName))
	require.NoError(t, err)
	_, err = stmt.Exec("host1", 1.5, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	_, err = stmt.Exec("host2", "not_a_double", time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC))
	require.NoError(t, err) // errors are reported asynchronously
	_, err = stmt.Exec()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse")
	require.NoError(t, tx.Rollback())

	var count int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count))
	assert.Equal(t, 0, count, "a failed copy must write nothing")

	// The connection stays usable: a new copy on it succeeds.
	tx, err = db.Begin()
	require.NoError(t, err)
	stmt, err = tx.Prepare(fmt.Sprintf(
		"COPY %s (host, val, ts) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')", tableName))
	require.NoError(t, err)
	_, err = stmt.Exec("host1", 1.5, time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC))
	require.NoError(t, err)
	_, err = stmt.Exec()
	require.NoError(t, err)
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count))
	assert.Equal(t, 1, count)

	dropTableDriver(t, db, tableName)
}
