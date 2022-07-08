// Copyright 2019 Dolthub, Inc.
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

package cnfcmds

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
)

type AutoResolveStrategy int

const (
	AutoResolveStrategyOurs AutoResolveStrategy = iota
	AutoResolveStrategyTheirs
)

var ErrConfSchIncompatible = errors.New("the conflict schema's columns are not equal to the current schema's columns, please resolve manually")

func AutoResolveAll(ctx context.Context, dEnv *env.DoltEnv, strategy AutoResolveStrategy) error {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return err
	}

	tbls, err := root.TablesInConflict(ctx)

	if err != nil {
		return err
	}

	return AutoResolveTables(ctx, dEnv, strategy, tbls)
}

func AutoResolveTables(ctx context.Context, dEnv *env.DoltEnv, strategy AutoResolveStrategy, tbls []string) error {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	for _, tblName := range tbls {
		err = ResolveTable(ctx, dEnv, root, tblName, strategy)
		if err != nil {
			return err
		}
	}

	return nil
}

func ResolveTable(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, tblName string, strategy AutoResolveStrategy) error {
	tbl, ok, err := root.GetTable(ctx, tblName)
	if err != nil {
		return err
	}
	if !ok {
		return doltdb.ErrTableNotFound
	}
	has, err := tbl.HasConflicts(ctx)
	if err != nil {
		return err
	}
	if !has {
		return nil
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return err
	}

	_, ourSch, theirSch, err := tbl.GetConflictSchemas(ctx, tblName)
	if err != nil {
		return err
	}

	switch strategy {
	case AutoResolveStrategyOurs:
		if !schema.ColCollsAreEqual(sch.GetAllCols(), ourSch.GetAllCols()) {
			return ErrConfSchIncompatible
		}
	case AutoResolveStrategyTheirs:
		if !schema.ColCollsAreEqual(sch.GetAllCols(), theirSch.GetAllCols()) {
			return ErrConfSchIncompatible
		}
	default:
		panic("unhandled auto resolve strategy")
	}

	before, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	eng, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return err
	}
	sqlCtx, err := engine.NewLocalSqlContext(ctx, eng)
	if err != nil {
		return err
	}

	if !schema.IsKeyless(sch) {
		err = resolvePkTable(sqlCtx, tblName, sch, strategy, eng)
	} else {
		err = resolveKeylessTable(sqlCtx, tblName, sch, strategy, eng)
	}
	if err != nil {
		return err
	}

	after, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	err = validateConstraintViolations(ctx, before, after, tblName)
	if err != nil {
		return err
	}

	return nil
}

func resolvePkTable(sqlCtx *sql.Context, tblName string, sch schema.Schema, strategy AutoResolveStrategy, eng *engine.SqlEngine) error {
	queries := getResolveQueries(strategy, tblName, sch)
	for _, query := range queries {
		err := execute(sqlCtx, eng, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveKeylessTable(sqlCtx *sql.Context, tblName string, sch schema.Schema, strategy AutoResolveStrategy, eng *engine.SqlEngine) error {
	allCols := sch.GetAllCols().GetColumnNames()
	baseCols := strings.Join(withPrefix(allCols, "base_"), ", ")
	ourCols := strings.Join(withPrefix(allCols, "our_"), ", ")
	theirCols := strings.Join(withPrefix(allCols, "their_"), ", ")

	selectConfsQ := fmt.Sprintf(
		`SELECT 
					%s,
					%s,
					%s,
					our_diff_type, 
					their_diff_type, 
					base_cardinality, 
					our_cardinality, 
					their_cardinality
				FROM dolt_conflicts_%s;`, baseCols, ourCols, theirCols, tblName)

	sqlSch, itr, err := eng.Query(sqlCtx, selectConfsQ)
	if err != nil {
		return err
	}
	s, err := sqlutil.FromDoltSchema(tblName, sch)
	if err != nil {
		return err
	}

	confSplitter, err := newConflictSplitter(sqlSch[:len(sqlSch)-3], s.Schema)
	if err != nil {
		return err
	}

	for {
		r, err := itr.Next(sqlCtx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		ourCardinality := r[len(r)-2].(uint64)
		theirCardinality := r[len(r)-1].(uint64)
		split, err := confSplitter.splitConflictRow(r[:len(r)-3])
		if err != nil {
			return err
		}
		// In a keyless conflict, the non-null versions have equivalent rows.
		// The first version in the split is always non-null.
		rowVals := split[0].row

		var rowDelta int64
		switch strategy {
		case AutoResolveStrategyOurs:
			rowDelta = 0
		case AutoResolveStrategyTheirs:
			rowDelta = int64(theirCardinality) - int64(ourCardinality)
		}

		var stmt string
		var n int64
		if rowDelta > 0 {
			stmt, err = sqlfmt.SqlRowAsInsertStmt(rowVals, tblName, sch)
			if err != nil {
				return err
			}
			n = rowDelta
		} else if rowDelta < 0 {
			stmt, err = sqlfmt.SqlRowAsDeleteStmt(rowVals, tblName, sch, 1)
			if err != nil {
				return err
			}
			n = rowDelta * -1
		}

		for i := int64(0); i < n; i++ {
			err = execute(sqlCtx, eng, stmt)
			if err != nil {
				return err
			}
		}

		err = execute(sqlCtx, eng, fmt.Sprintf("DELETE FROM dolt_conflicts_%s", tblName))
		if err != nil {
			return err
		}

		err = execute(sqlCtx, eng, "COMMIT;")
		if err != nil {
			return err
		}
	}

	return nil
}

func execute(ctx *sql.Context, eng *engine.SqlEngine, query string) error {
	_, itr, err := eng.Query(ctx, query)
	if err != nil {
		return err
	}
	_, err = itr.Next(ctx)
	for err != nil && err != io.EOF {
		return err
	}
	return nil
}

func getResolveQueries(strategy AutoResolveStrategy, tblName string, sch schema.Schema) (queries []string) {
	identCols := getIdentifyingColumnNames(sch)
	allCols := sch.GetAllCols().GetColumnNames()

	r := autoResolverMap[strategy]
	queries = r(tblName, allCols, identCols)
	// auto_commit is off
	queries = append(queries, "COMMIT;")

	return
}

func getIdentifyingColumnNames(sch schema.Schema) []string {
	if schema.IsKeyless(sch) {
		return sch.GetAllCols().GetColumnNames()
	} else {
		return sch.GetPKCols().GetColumnNames()
	}
}

var autoResolverMap = map[AutoResolveStrategy]autoResolver{
	AutoResolveStrategyOurs:   ours,
	AutoResolveStrategyTheirs: theirs,
}

type autoResolver func(tblName string, allCols []string, identCols []string) []string

func theirs(tblName string, allCols []string, identCols []string) []string {
	dstCols := strings.Join(allCols, ", ")
	srcCols := strings.Join(withPrefix(allCols, "their_"), ", ")
	q1 := fmt.Sprintf(
		`
REPLACE INTO %s (%s) (
	SELECT %s
	FROM dolt_conflicts_%s
	WHERE their_diff_type = 'modified' OR their_diff_type = 'added'
);
`, tblName, dstCols, srcCols, tblName)

	q2 := fmt.Sprintf(
		`
DELETE t1 
FROM %s t1 
WHERE ( 
	SELECT count(*) from dolt_conflicts_%s t2
	WHERE %s AND t2.their_diff_type = 'removed'
) > 0;
`, tblName, tblName, buildJoinCond(identCols, "base_"))

	q3 := fmt.Sprintf("DELETE FROM dolt_conflicts_%s;", tblName)

	return []string{q1, q2, q3}
}

func ours(tblName string, allCols []string, identCols []string) []string {

	q3 := fmt.Sprintf("DELETE FROM dolt_conflicts_%s;", tblName)

	return []string{q3}
}

func buildJoinCond(identCols []string, prefix string) string {
	b := &strings.Builder{}
	var seenOne bool
	for _, col := range identCols {
		if seenOne {
			_, _ = b.WriteString(" AND ")
		}
		seenOne = true
		_, _ = fmt.Fprintf(b, "t1.%s = t2.%s%s", col, prefix, col)
	}
	return b.String()
}

func withPrefix(arr []string, prefix string) []string {
	out := make([]string, len(arr))
	for i := range arr {
		out[i] = prefix + arr[i]
	}
	return out
}

func validateConstraintViolations(ctx context.Context, before, after *doltdb.RootValue, table string) error {
	tables, err := after.GetTableNames(ctx)
	if err != nil {
		return err
	}

	_, violators, err := merge.AddForeignKeyViolations(ctx, after, before, set.NewStrSet(tables), hash.Of(nil))
	if err != nil {
		return err
	}
	if violators.Size() > 0 {
		return fmt.Errorf("resolving conflicts for table %s created foreign key violations", table)
	}

	return nil
}
