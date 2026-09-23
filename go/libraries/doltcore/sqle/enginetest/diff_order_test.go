// Copyright 2026 Dolthub, Inc.
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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestDiffEffectiveKeyOrder(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	defer h.Close()
	e, err := h.NewEngine(t)
	require.NoError(t, err)
	ctx := enginetest.NewContext(h)
	query := func(q string) []sql.Row {
		_, iter, _, err := e.Query(ctx, q)
		require.NoError(t, err, q)
		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err, q)
		return rows
	}
	for _, q := range []string{
		"CREATE TABLE ordered_diff (b INT, a INT, v INT, PRIMARY KEY(a,b))",
		"CREATE TABLE string_diff (id VARCHAR(20) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY, v INT)",
		"CREATE TABLE keyless_diff (v INT)",
		"CREATE TABLE date_diff (id INT, d DATE, PRIMARY KEY(id,d))",
		"INSERT INTO ordered_diff VALUES (2,1,0),(3,1,0),(1,2,0),(1,4,0)",
		"INSERT INTO string_diff VALUES ('z',0),('B',0),('é',0)",
		"INSERT INTO keyless_diff VALUES (1),(1)",
		"INSERT INTO date_diff VALUES (1,'2025-01-01'),(2,'2025-01-01')",
		"CALL DOLT_ADD('.')",
		"CALL DOLT_COMMIT('-m','before')",
		"UPDATE ordered_diff SET v=1 WHERE a=1 AND b=2",
		"DELETE FROM ordered_diff WHERE a=2",
		"UPDATE ordered_diff SET a=3 WHERE a=1 AND b=3",
		"INSERT INTO ordered_diff VALUES (1,0,1)",
		"UPDATE string_diff SET v=1 WHERE id='B'",
		"DELETE FROM string_diff WHERE id='é'",
		"INSERT INTO string_diff VALUES ('a',1),('D',1)",
		"INSERT INTO keyless_diff VALUES (3)",
		"DELETE FROM date_diff WHERE id=1",
		"INSERT INTO date_diff VALUES (0,'2026-01-01')",
	} {
		query(q)
	}
	base := "SELECT to_a,to_b,from_a,from_b,diff_type FROM DOLT_DIFF('HEAD','WORKING','ordered_diff')"
	order := " ORDER BY COALESCE(to_a,from_a),COALESCE(to_b,from_b)"
	want := []sql.Row{{int32(0), int32(1), nil, nil, "added"}, {int32(1), int32(2), int32(1), int32(2), "modified"}, {nil, nil, int32(1), int32(3), "removed"}, {nil, nil, int32(2), int32(1), "removed"}, {int32(3), int32(3), nil, nil, "added"}}
	require.Equal(t, want, query(base+order))
	var pages []sql.Row
	for offset := 0; offset < len(want); offset += 2 {
		pages = append(pages, query(base+order+fmt.Sprintf(" LIMIT 2 OFFSET %d", offset))...)
	}
	require.Equal(t, want, pages)
	for _, tc := range []struct {
		q      string
		sorted bool
	}{
		{base + order + " LIMIT 2", false},
		{base + order, false},
		{base + order + " LIMIT 2 OFFSET 2", false},
		{base + " ORDER BY to_a,from_a,to_b,from_b LIMIT 2", true},
		{base + " ORDER BY COALESCE(to_b,from_b),COALESCE(to_a,from_a) LIMIT 2", true},
		{base + " ORDER BY COALESCE(to_a,from_a) DESC,COALESCE(to_b,from_b) DESC LIMIT 2", true},
		{base + " ORDER BY COALESCE(to_a,from_a) LIMIT 2", true},
		{base + " ORDER BY COALESCE(to_a,from_a)+1,COALESCE(to_b,from_b) LIMIT 2", true},
		{base + " WHERE to_a > 0" + order + " LIMIT 2", true},
		{"SELECT * FROM DOLT_DIFF('HEAD','WORKING','string_diff') ORDER BY COALESCE(to_id,from_id) LIMIT 2", false},
		{"SELECT * FROM DOLT_DIFF('HEAD','WORKING','keyless_diff') ORDER BY COALESCE(to_v,from_v) LIMIT 2", true},
		{"SELECT * FROM DOLT_DIFF('HEAD','WORKING','date_diff') ORDER BY COALESCE(to_id,from_id),COALESCE(to_d,from_d) LIMIT 2", false},
		{"SELECT * FROM DOLT_DIFF('HEAD','WORKING','ordered_diff') d ORDER BY COALESCE(d.to_a,d.from_a),COALESCE(d.to_b,d.from_b) LIMIT 2", false},
	} {
		t.Run(tc.q, func(t *testing.T) {
			rows := query("EXPLAIN FORMAT=TREE " + tc.q)
			var lines []string
			for _, row := range rows {
				lines = append(lines, row[0].(string))
			}
			p := strings.Join(lines, "\n")
			require.Equal(t, tc.sorted, strings.Contains(p, "TopN(") || strings.Contains(p, "Sort("), p)
		})
	}
	query("PREPARE diff_page FROM \"" + base + order + " LIMIT ? OFFSET ?\"")
	query("SET @page_size=2, @page_offset=2")
	require.Equal(t, want[2:4], query("EXECUTE diff_page USING @page_size,@page_offset"))
	require.Equal(t, []sql.Row{{"a"}, {"B"}, {"D"}, {"é"}}, query("SELECT COALESCE(to_id,from_id) FROM DOLT_DIFF('HEAD','WORKING','string_diff') ORDER BY COALESCE(to_id,from_id)"))
	query(strings.Replace(base, "SELECT ", "SELECT SQL_CALC_FOUND_ROWS ", 1) + order + " LIMIT 2")
	require.Equal(t, "[[5]]", fmt.Sprint(query("SELECT FOUND_ROWS()")))
	query("SET @@dolt_override_schema='HEAD'")
	require.Contains(t, fmt.Sprint(query("EXPLAIN FORMAT=TREE "+base+order+" LIMIT 2")), "TopN(")
	query("SET @@dolt_override_schema=''")
	query("ALTER TABLE ordered_diff MODIFY a BIGINT")
	plan := fmt.Sprint(query("EXPLAIN FORMAT=TREE " + base + order + " LIMIT 2"))
	require.Contains(t, plan, "TopN(", "changed PK types must retain the sort")
}

func TestDiffEffectiveKeyOrderAcrossTreeNodes(t *testing.T) {
	h := newDoltEnginetestHarness(t)
	defer h.Close()
	e, err := h.NewEngine(t)
	require.NoError(t, err)
	ctx := enginetest.NewContext(h)
	query := func(q string) []sql.Row {
		_, iter, _, err := e.Query(ctx, q)
		require.NoError(t, err)
		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err)
		return rows
	}
	query("CREATE TABLE large_diff (id INT PRIMARY KEY, v INT)")
	for start := 0; start < 2048; start += 256 {
		var values []string
		for i := start; i < start+256; i++ {
			values = append(values, fmt.Sprintf("(%d,0)", i))
		}
		query("INSERT INTO large_diff VALUES " + strings.Join(values, ","))
	}
	query("CALL DOLT_ADD('.')")
	query("CALL DOLT_COMMIT('-m','before')")
	query("DELETE FROM large_diff WHERE id % 3 = 0")
	query("UPDATE large_diff SET v=1 WHERE id % 3 = 1")
	query("INSERT INTO large_diff VALUES (-1,1),(4096,1)")
	want := []sql.Row{{int32(-1), "added"}}
	for i := 0; i < 2048; i++ {
		switch i % 3 {
		case 0:
			want = append(want, sql.Row{int32(i), "removed"})
		case 1:
			want = append(want, sql.Row{int32(i), "modified"})
		}
	}
	want = append(want, sql.Row{int32(4096), "added"})
	q := "SELECT COALESCE(to_id,from_id),diff_type FROM DOLT_DIFF('HEAD','WORKING','large_diff') ORDER BY COALESCE(to_id,from_id)"
	require.Equal(t, want, query(q))
	for _, offset := range []int{0, 50, 51, 511, 1024, len(want) - 1, len(want)} {
		end := min(offset+51, len(want))
		got := query(q + fmt.Sprintf(" LIMIT 51 OFFSET %d", offset))
		if end == offset {
			require.Empty(t, got)
		} else {
			require.Equal(t, want[offset:end], got)
		}
	}
}
