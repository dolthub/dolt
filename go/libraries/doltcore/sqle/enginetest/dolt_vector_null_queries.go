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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

// generateNullVectorInserts generates a multi-row INSERT statement where every
// row has a NULL vector column. IDs range from start to start+count-1.
func generateNullVectorInserts(table string, start, count int) string {
	vals := make([]string, count)
	for i := range vals {
		vals[i] = fmt.Sprintf("(%d)", start+i)
	}
	return fmt.Sprintf("INSERT INTO %s (id) VALUES %s", table, strings.Join(vals, ","))
}

// generateVectorInserts generates a multi-row INSERT with non-NULL 2D vectors.
// Each vector is deterministic: [float32(id), float32(id+1)].
// IDs range from start to start+count-1.
func generateVectorInserts(table string, start, count int) string {
	vals := make([]string, count)
	for i := range vals {
		id := start + i
		vals[i] = fmt.Sprintf("(%d, STRING_TO_VECTOR('[%d.0,%d.0]'))", id, id, id+1)
	}
	return fmt.Sprintf("INSERT INTO %s (id, v) VALUES %s", table, strings.Join(vals, ","))
}

// DoltVectorIndexNullScripts exercises vector index behaviour when rows contain
// NULL embeddings. Each scenario inserts more than 83 rows (the 64 KB flush
// threshold) to trigger the batch-flush code path that previously crashed with
// "unable to cast <nil> of type <nil> to vector".
//
// NOTE: Queries using WHERE v IS [NOT] NULL are avoided because the covering
// index scan on the vector index triggers a nil prolly.Map dereference
// (separate bug from the insert-path crash). We use COUNT(*), COUNT(v), and
// ID-based WHERE clauses instead.
var DoltVectorIndexNullScripts = []queries.ScriptTest{
	{
		Name: "vector index: 150 rows all NULL vectors",
		SetUpScript: []string{
			"CREATE TABLE vec_all_null (id INT PRIMARY KEY, v VECTOR(2))",
			"CREATE VECTOR INDEX v_idx ON vec_all_null(v)",
			generateNullVectorInserts("vec_all_null", 1, 150),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Verifies that batch insert of 150 NULL vectors does not crash.
				// This was the original bug: inserts panicked at ~83 rows.
				Query:    "SELECT COUNT(*) FROM vec_all_null",
				Expected: []sql.Row{{150}},
			},
		},
	},
	{
		Name: "vector index: 150 rows all non-NULL vectors",
		SetUpScript: []string{
			"CREATE TABLE vec_all_nonnull (id INT PRIMARY KEY, v VECTOR(2))",
			"CREATE VECTOR INDEX v_idx ON vec_all_nonnull(v)",
			generateVectorInserts("vec_all_nonnull", 1, 150),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM vec_all_nonnull",
				Expected: []sql.Row{{150}},
			},
			{
				// Proximity search: closest to [1.0, 2.0] should be id=1
				// (vector [1.0, 2.0]), then id=2 ([2.0, 3.0]), then id=3 ([3.0, 4.0]).
				Query:    "SELECT id FROM vec_all_nonnull ORDER BY VEC_DISTANCE('[1.0,2.0]', v) LIMIT 3",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
		},
	},
	{
		Name: "vector index: 150 rows mixed NULL and non-NULL vectors",
		SetUpScript: []string{
			"CREATE TABLE vec_mixed (id INT PRIMARY KEY, v VECTOR(2))",
			"CREATE VECTOR INDEX v_idx ON vec_mixed(v)",
			// IDs 1-50: NULL vectors
			generateNullVectorInserts("vec_mixed", 1, 50),
			// IDs 51-150: non-NULL vectors [51.0,52.0], [52.0,53.0], ...
			generateVectorInserts("vec_mixed", 51, 100),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM vec_mixed",
				Expected: []sql.Row{{150}},
			},
			{
				// COUNT(v) counts non-NULL values only.
				Query:    "SELECT COUNT(v) FROM vec_mixed",
				Expected: []sql.Row{{100}},
			},
			{
				// Verify NULL count via arithmetic (avoids WHERE v IS NULL).
				Query:    "SELECT COUNT(*) - COUNT(v) FROM vec_mixed",
				Expected: []sql.Row{{50}},
			},
			{
				// Proximity search over mixed data must not crash.
				// Closest to [51.0, 52.0] is id=51 (exact match).
				Query:    "SELECT id FROM vec_mixed ORDER BY VEC_DISTANCE('[51.0,52.0]', v) LIMIT 1",
				Expected: []sql.Row{{51}},
			},
		},
	},
	{
		Name: "vector index: UPDATE NULL to non-NULL vector",
		SetUpScript: []string{
			"CREATE TABLE vec_update (id INT PRIMARY KEY, v VECTOR(2))",
			"CREATE VECTOR INDEX v_idx ON vec_update(v)",
			// IDs 1-50: NULL vectors
			generateNullVectorInserts("vec_update", 1, 50),
			// IDs 51-150: non-NULL vectors
			generateVectorInserts("vec_update", 51, 100),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM vec_update",
				Expected: []sql.Row{{150}},
			},
			{
				// Initially 100 non-NULL values.
				Query:    "SELECT COUNT(v) FROM vec_update",
				Expected: []sql.Row{{100}},
			},
			{
				// Update 10 NULL rows (IDs 1-10) to have a distinct vector value.
				Query: "UPDATE vec_update SET v = STRING_TO_VECTOR('[999.0,999.0]') WHERE id <= 10",
			},
			{
				// Confirm non-NULL count increased by 10.
				Query:    "SELECT COUNT(v) FROM vec_update",
				Expected: []sql.Row{{110}},
			},
		},
	},
	{
		Name: "vector index: DELETE rows with NULL vectors",
		SetUpScript: []string{
			"CREATE TABLE vec_delete (id INT PRIMARY KEY, v VECTOR(2))",
			"CREATE VECTOR INDEX v_idx ON vec_delete(v)",
			// IDs 1-50: NULL vectors
			generateNullVectorInserts("vec_delete", 1, 50),
			// IDs 51-150: non-NULL vectors
			generateVectorInserts("vec_delete", 51, 100),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM vec_delete",
				Expected: []sql.Row{{150}},
			},
			{
				// Delete NULL vector rows by ID range.
				Query: "DELETE FROM vec_delete WHERE id <= 50",
			},
			{
				Query:    "SELECT COUNT(*) FROM vec_delete",
				Expected: []sql.Row{{100}},
			},
			{
				// All remaining rows should have non-NULL vectors.
				Query:    "SELECT COUNT(v) FROM vec_delete",
				Expected: []sql.Row{{100}},
			},
			{
				// Proximity search still works after NULL rows deleted.
				Query:    "SELECT id FROM vec_delete ORDER BY VEC_DISTANCE('[51.0,52.0]', v) LIMIT 1",
				Expected: []sql.Row{{51}},
			},
		},
	},
}
