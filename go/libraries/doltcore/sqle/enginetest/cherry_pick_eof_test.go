// Copyright 2024 Dolthub, Inc.
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

package enginetest

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCherryPickEOFError tests that cherry-pick operations through the SQL interface
// don't generate unexpected EOF errors when conflicts or constraint violations occur.
func TestCherryPickEOFError(t *testing.T) {
	tests := []struct {
		name        string
		setupScript []string
		cherryPick  string
		expectEOF   bool
	}{
		{
			name: "conflict case - should not return EOF",
			setupScript: []string{
				"CREATE TABLE test (pk int primary key, v varchar(10));",
				"INSERT INTO test VALUES (1, 'a');",
				"CALL dolt_commit('-Am', 'create table with data');",
				"CALL dolt_checkout('-b', 'branch1');",
				"UPDATE test SET v = 'b' WHERE pk = 1;",
				"CALL dolt_commit('-am', 'update on branch1');",
				"CALL dolt_checkout('main');",
				"UPDATE test SET v = 'c' WHERE pk = 1;",
				"CALL dolt_commit('-am', 'update on main');",
			},
			cherryPick: "CALL DOLT_CHERRY_PICK('branch1');",
			expectEOF:  false,
		},
		{
			name: "constraint violation case - should not return EOF",
			setupScript: []string{
				"CREATE TABLE parent (pk int primary key, v int);",
				"CREATE TABLE child (pk int primary key, parent_fk int, CONSTRAINT fk FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
				"INSERT INTO parent VALUES (1, 1);",
				"CALL dolt_commit('-Am', 'create tables');",
				"CALL dolt_checkout('-b', 'branch1');",
				"INSERT INTO child VALUES (1, 1);",
				"CALL dolt_commit('-am', 'add valid child');",
				"CALL dolt_checkout('main');",
				"DELETE FROM parent WHERE pk = 1;",
				"CALL dolt_commit('-am', 'delete parent');",
			},
			cherryPick: "CALL DOLT_CHERRY_PICK('branch1');",
			expectEOF:  false,
		},
		{
			name: "empty commit case - should not return EOF",
			setupScript: []string{
				"CREATE TABLE test (pk int primary key, v varchar(10));",
				"CALL dolt_commit('-Am', 'create table');",
				"CALL dolt_checkout('-b', 'branch1');",
				"CALL dolt_commit('--allow-empty', '-m', 'empty commit');",
				"CALL dolt_checkout('main');",
			},
			cherryPick: "CALL DOLT_CHERRY_PICK('branch1');",
			expectEOF:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newDoltHarness(t)
			defer harness.Close()

			// Set up the database first
			harness.Setup([]setup.SetupScript{{
				"CREATE DATABASE testdb;",
				"USE testdb;",
			}})

			engine, err := harness.NewEngine(t)
			require.NoError(t, err)

			ctx := harness.NewContext()

			// Set up the test scenario
			for _, stmt := range tt.setupScript {
				_, _, _, err := engine.Query(ctx, stmt)
				require.NoError(t, err, "Setup statement failed: %s", stmt)
			}

			// Execute the cherry-pick operation
			_, iter, _, err := engine.Query(ctx, tt.cherryPick)
			if err != nil {
				// If there's an error, it should not be EOF
				assert.NotEqual(t, io.EOF, err, "Cherry-pick should not return EOF error")
				return
			}

			// If no error, iterate through results and check for EOF
			var rows []sql.Row
			for {
				row, err := iter.Next(ctx)
				if err == io.EOF {
					break
				}
				if err != nil {
					// Any error other than EOF should be reported
					assert.NotEqual(t, io.EOF, err, "Unexpected EOF during iteration")
					t.Fatalf("Unexpected error during iteration: %v", err)
				}
				rows = append(rows, row)
			}

			// Verify we got expected results
			assert.NotEmpty(t, rows, "Cherry-pick should return at least one row")
			
			// The first row should contain the cherry-pick result
			if len(rows) > 0 {
				row := rows[0]
				assert.Len(t, row, 4, "Cherry-pick result should have 4 columns: hash, data_conflicts, schema_conflicts, constraint_violations")
				
				// Check that the result format is correct
				// hash can be empty string for conflicts/violations
				// conflict counts should be integers
				if len(row) >= 4 {
					_, ok1 := row[1].(int64)
					_, ok2 := row[2].(int64)
					_, ok3 := row[3].(int64)
					assert.True(t, ok1, "data_conflicts should be int64")
					assert.True(t, ok2, "schema_conflicts should be int64")
					assert.True(t, ok3, "constraint_violations should be int64")
				}
			}
		})
	}
}

// TestCherryPickRowIterExhaustion tests that the row iterator from cherry-pick
// operations properly handles exhaustion without returning unexpected EOF errors.
func TestCherryPickRowIterExhaustion(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()

	// Set up the database first
	harness.Setup([]setup.SetupScript{{
		"CREATE DATABASE testdb;",
		"USE testdb;",
	}})

	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	ctx := harness.NewContext()

	// Set up a simple cherry-pick scenario
	setupScript := []string{
		"CREATE TABLE test (pk int primary key, v varchar(10));",
		"CALL dolt_commit('-Am', 'create table');",
		"CALL dolt_checkout('-b', 'branch1');",
		"INSERT INTO test VALUES (1, 'a');",
		"CALL dolt_commit('-am', 'add row');",
		"CALL dolt_checkout('main');",
	}

	for _, stmt := range setupScript {
		_, _, _, err := engine.Query(ctx, stmt)
		require.NoError(t, err)
	}

	// Execute cherry-pick and test row iteration
	_, iter, _, err := engine.Query(ctx, "CALL DOLT_CHERRY_PICK('branch1');")
	require.NoError(t, err)

	// Test multiple calls to Next() to ensure proper EOF handling
	var rowCount int
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "Should not get non-EOF error during iteration")
		rowCount++
		assert.Len(t, row, 4, "Each row should have 4 columns")
	}

	// Should have exactly one row
	assert.Equal(t, 1, rowCount, "Cherry-pick should return exactly one row")

	// Test that subsequent calls to Next() return EOF
	_, err = iter.Next(ctx)
	assert.Equal(t, io.EOF, err, "Subsequent Next() calls should return EOF")
}

// TestCherryPickWithContextCancellation tests that cherry-pick operations
// handle context cancellation gracefully without EOF errors.
func TestCherryPickWithContextCancellation(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()

	// Set up the database first
	harness.Setup([]setup.SetupScript{{
		"CREATE DATABASE testdb;",
		"USE testdb;",
	}})

	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	ctx := harness.NewContext()

	// Set up a cherry-pick scenario
	setupScript := []string{
		"CREATE TABLE test (pk int primary key, v varchar(10));",
		"CALL dolt_commit('-Am', 'create table');",
		"CALL dolt_checkout('-b', 'branch1');",
		"INSERT INTO test VALUES (1, 'a');",
		"CALL dolt_commit('-am', 'add row');",
		"CALL dolt_checkout('main');",
	}

	for _, stmt := range setupScript {
		_, _, _, err := engine.Query(ctx, stmt)
		require.NoError(t, err)
	}

	// Create a cancelled context
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	sqlCtx := sql.NewContext(cancelledCtx)

	// Try to execute cherry-pick with cancelled context
	_, _, _, err = engine.Query(sqlCtx, "CALL DOLT_CHERRY_PICK('branch1');")
	
	// Should get context cancellation error, not EOF
	assert.Error(t, err, "Should get error with cancelled context")
	assert.NotEqual(t, io.EOF, err, "Should not get EOF with cancelled context")
	assert.Contains(t, err.Error(), "context", "Error should be related to context cancellation")
}