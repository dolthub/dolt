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

package kvexec

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// TestLeftOuterMergeJoinResidualFilter is a regression test for a pair of
// wrong-result bugs in mergeJoinKvIter for left outer merge joins whose key
// runs contain duplicates and whose join condition carries a residual
// (non-merge-key) filter:
//
//  1. matchedLeft was overwritten (rather than or-ed) on every pairing, so a
//     later failed pairing in the same key run erased the fact that the
//     current left row had already matched, emitting a spurious
//     null-extended row for it.
//  2. when the left iterator hit EOF while advancing past a completed left
//     row, io.EOF was returned immediately, dropping the null-extended row
//     owed to a final unmatched left row.
func TestLeftOuterMergeJoinResidualFilter(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		query    string
		expected []sql.Row
	}{
		{
			name: "matched left row must not re-emit as null-extended; final unmatched left row must emit",
			setup: []string{
				"create table b2 (cat varchar(50) not null, code varchar(16) not null, lang varchar(20) not null, primary key(cat, code, lang), key(code))",
				"create table t2 (id varchar(36) primary key, code varchar(16), lang varchar(16), key(code, lang))",
				"insert into b2 values ('cat0','P1','de:app'),('cat1','P1','fr:app'),('cat2','P1','nl:app')",
				"insert into t2 values ('t1','P1','de'),('t2','P1','es'),('t3','P1','fr')",
			},
			query: "select /*+ MERGE_JOIN(p,w) */ p.lang, w.lang from b2 p left join t2 w on w.code = p.code and w.lang = substring_index(p.lang, ':', 1) order by 1, 2",
			expected: []sql.Row{
				{"de:app", "de"},
				{"fr:app", "fr"},
				{"nl:app", nil},
			},
		},
		{
			name: "final unmatched left row emits when right run exhausts first",
			setup: []string{
				"create table b2 (cat varchar(50) not null, code varchar(16) not null, lang varchar(20) not null, primary key(cat, code, lang), key(code))",
				"create table t2 (id varchar(36) primary key, code varchar(16), lang varchar(16), key(code, lang))",
				"insert into b2 values ('cat0','P1','de:app'),('cat1','P1','nl:app')",
				"insert into t2 values ('t1','P1','de'),('t2','P1','zz')",
			},
			query: "select /*+ MERGE_JOIN(p,w) */ p.lang, w.lang from b2 p left join t2 w on w.code = p.code and w.lang = substring_index(p.lang, ':', 1) order by 1, 2",
			expected: []sql.Row{
				{"de:app", "de"},
				{"nl:app", nil},
			},
		},
		{
			name: "anti-join (IS NULL) over multiple key groups",
			setup: []string{
				"create table b2 (cat varchar(50) not null, code varchar(16) not null, lang varchar(20) not null, primary key(cat, code, lang), key(code))",
				"create table t2 (id varchar(36) primary key, code varchar(16), lang varchar(16), key(code, lang))",
				"insert into b2 values ('cat0','P1','de:app'),('cat1','P1','fr:app'),('cat2','P1','nl:app'),('cat0','P2','de:app'),('cat1','P2','fr:app'),('cat2','P2','nl:app')",
				"insert into t2 values ('t1','P1','de'),('t2','P1','es'),('t3','P1','fr'),('t4','P2','de'),('t5','P2','es'),('t6','P2','fr')",
			},
			query: "select /*+ MERGE_JOIN(p,w) */ p.code, p.lang from b2 p left join t2 w on w.code = p.code and w.lang = substring_index(p.lang, ':', 1) where w.code is null order by 1, 2",
			expected: []sql.Row{
				{"P1", "nl:app"},
				{"P2", "nl:app"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.Close()

			db, err := sqle.NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), editor.Options{})
			require.NoError(t, err)

			engine, sqlCtx, err := sqle.NewTestEngine(dEnv, context.Background(), db)
			require.NoError(t, err)

			// route execution through the kvexec operators, as the CLI and
			// server engines do
			engine.EngineAnalyzer().ExecBuilder = rowexec.NewBuilder(Builder{}, engine.EngineAnalyzer().Overrides)

			err = sqlCtx.Session.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
			require.NoError(t, err)

			for _, q := range tt.setup {
				_, iter, _, err := engine.Query(sqlCtx, q)
				require.NoError(t, err)
				_, err = sql.RowIterToRows(sqlCtx, iter)
				require.NoError(t, err)
			}

			_, iter, _, err := engine.Query(sqlCtx, tt.query)
			require.NoError(t, err)
			rows, err := sql.RowIterToRows(sqlCtx, iter)
			require.NoError(t, err)
			require.Equal(t, tt.expected, rows)
		})
	}
}
