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
)

type AutoResolveStrategy int

const (
	AutoResolveStrategyOurs AutoResolveStrategy = iota
	AutoResolveStrategyTheirs
)

var ErrConfSchIncompatible = errors.New("the conflict schema's columns are not equal to the current schema's columns, please resolve manually")

// AutoResolveAll resolves all conflicts in all tables according to the given
// |strategy|.
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

// AutoResolveTables resolves all conflicts in the given tables according to the
// given |strategy|.
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

// ResolveTable resolves all conflicts in the given table according to the given
// |strategy|. It errors if the schema of the conflict version you are choosing
// differs from the current schema.
func ResolveTable(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, tblName string, strategy AutoResolveStrategy) (err error) {
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

	v, err := getFirstColumn(sqlCtx, eng, "SELECT @@DOLT_ALLOW_COMMIT_CONFLICTS;")
	if err != nil {
		return err
	}
	oldAllowCommitConflicts, ok := v.(int8)
	if !ok {
		return fmt.Errorf("unexpected type of @DOLT_ALLOW_COMMIT_CONFLICTS: %T", v)
	}

	// Resolving conflicts for one table, will not resolve conflicts on another.
	err = execute(sqlCtx, eng, "SET DOLT_ALLOW_COMMIT_CONFLICTS = 1;")
	if err != nil {
		return err
	}
	defer func() {
		err2 := execute(sqlCtx, eng, fmt.Sprintf("SET DOLT_ALLOW_COMMIT_CONFLICTS = %d", oldAllowCommitConflicts))
		if err == nil {
			err = err2
		}
	}()

	if !schema.IsKeyless(sch) {
		err = resolvePkTable(sqlCtx, tblName, sch, strategy, eng)
	} else {
		err = resolveKeylessTable(sqlCtx, tblName, sch, strategy, eng)
	}
	if err != nil {
		return err
	}

	err = execute(sqlCtx, eng, "COMMIT;")
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

func resolvePkTable(ctx *sql.Context, tblName string, sch schema.Schema, strategy AutoResolveStrategy, eng *engine.SqlEngine) error {
	identCols := getIdentifyingColumnNames(sch)
	allCols := sch.GetAllCols().GetColumnNames()

	switch strategy {
	case AutoResolveStrategyOurs:
		err := oursPKResolver(ctx, eng, tblName)
		if err != nil {
			return err
		}
	case AutoResolveStrategyTheirs:
		err := theirsPKResolver(ctx, eng, tblName, allCols, identCols)
		if err != nil {
			return err
		}
	}

	return nil
}

func resolveKeylessTable(sqlCtx *sql.Context, tblName string, sch schema.Schema, strategy AutoResolveStrategy, eng *engine.SqlEngine) error {
	allCols := sch.GetAllCols().GetColumnNames()
	baseCols := strings.Join(quoteWithPrefix(allCols, "base_"), ", ")
	ourCols := strings.Join(quoteWithPrefix(allCols, "our_"), ", ")
	theirCols := strings.Join(quoteWithPrefix(allCols, "their_"), ", ")
	confTblName := fmt.Sprintf("`dolt_conflicts_%s`", tblName)

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
				FROM %s;`, baseCols, ourCols, theirCols, confTblName)

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
	}

	err = execute(sqlCtx, eng, fmt.Sprintf("DELETE FROM %s", confTblName))
	if err != nil {
		return err
	}

	return nil
}

func oursPKResolver(sqlCtx *sql.Context, eng *engine.SqlEngine, tblName string) error {
	del := fmt.Sprintf("DELETE FROM `dolt_conflicts_%s`;", tblName)
	err := execute(sqlCtx, eng, del)
	if err != nil {
		return err
	}
	return nil
}

func getIdentifyingColumnNames(sch schema.Schema) []string {
	if schema.IsKeyless(sch) {
		return sch.GetAllCols().GetColumnNames()
	} else {
		return sch.GetPKCols().GetColumnNames()
	}
}

func theirsPKResolver(sqlCtx *sql.Context, eng *engine.SqlEngine, tblName string, allCols []string, identCols []string) error {
	dstCols := strings.Join(quoted(allCols), ", ")
	srcCols := strings.Join(quoteWithPrefix(allCols, "their_"), ", ")

	cnfTbl := fmt.Sprintf("`dolt_conflicts_%s`", tblName)
	qName := fmt.Sprintf("`%s`", tblName)

	q1 := fmt.Sprintf(
		`
REPLACE INTO %s (%s) (
	SELECT %s
	FROM %s
	WHERE their_diff_type = 'modified' OR their_diff_type = 'added'
);
`, qName, dstCols, srcCols, cnfTbl)
	err := execute(sqlCtx, eng, q1)
	if err != nil {
		return err
	}

	selCols := strings.Join(coalesced(identCols), ", ")
	q2 := fmt.Sprintf("SELECT %s from `dolt_conflicts_%s` WHERE their_diff_type = 'removed';", selCols, tblName)
	sch, itr, err := eng.Query(sqlCtx, q2)
	if err != nil {
		return err
	}

	for {
		row, err := itr.Next(sqlCtx)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		deleteFilter, err := buildFilter(identCols, row, sch)
		if err != nil {
			return err
		}
		del := fmt.Sprintf("DELETE from `%s` WHERE %s;", tblName, deleteFilter)
		err = execute(sqlCtx, eng, del)
		if err != nil {
			return err
		}
	}

	q3 := fmt.Sprintf("DELETE FROM `dolt_conflicts_%s`;", tblName)
	err = execute(sqlCtx, eng, q3)
	if err != nil {
		return err
	}

	return nil
}

func buildFilter(columns []string, row sql.Row, rowSch sql.Schema) (string, error) {
	if len(columns) != len(row) {
		return "", errors.New("cannot build filter since number of columns does not match number of values")
	}
	vals, err := sqlfmt.SqlRowAsStrings(row, rowSch)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	var seen bool
	for i, v := range vals {
		_, _ = fmt.Fprintf(&b, "`%s` = %s", columns[i], v)
		if seen {
			_, _ = fmt.Fprintf(&b, "AND `%s` = %s", columns[i], v)
		}
		seen = true
	}
	return b.String(), nil
}

func coalesced(cols []string) []string {
	out := make([]string, len(cols))
	for i := range out {
		out[i] = fmt.Sprintf("coalesce(`base_%s`, `our_%s`, `their_%s`)", cols[i], cols[i], cols[i])
	}
	return out
}

func quoted(cols []string) []string {
	out := make([]string, len(cols))
	for i := range out {
		out[i] = fmt.Sprintf("`%s`", cols[i])
	}
	return out
}

func quoteWithPrefix(arr []string, prefix string) []string {
	out := make([]string, len(arr))
	for i := range arr {
		out[i] = fmt.Sprintf("`%s%s`", prefix, arr[i])
	}
	return out
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

func getFirstColumn(ctx *sql.Context, eng *engine.SqlEngine, query string) (interface{}, error) {
	_, itr, err := eng.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	r, err := itr.Next(ctx)
	if err != nil {
		return nil, err
	}
	if len(r) == 0 {
		return nil, fmt.Errorf("no columns returned")
	}
	return r[0], nil
}

func validateConstraintViolations(ctx context.Context, before, after *doltdb.RootValue, table string) error {
	tables, err := after.GetTableNames(ctx)
	if err != nil {
		return err
	}

	violators, err := merge.GetForeignKeyViolatedTables(ctx, after, before, set.NewStrSet(tables))
	if err != nil {
		return err
	}
	if violators.Size() > 0 {
		return fmt.Errorf("resolving conflicts for table %s created foreign key violations", table)
	}

	return nil
}
