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

package sqle

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

// resolveOverriddenNonexistentTable checks if there is an overridden schema commit set for this session, and if so
// returns an empty table with that schema if |tblName| exists in the overridden schema commit. If no schema override
// is set, this function returns a nil sql.Table and a false boolean return parameter.
func resolveOverriddenNonexistentTable(ctx *sql.Context, tblName string, db Database) (sql.Table, bool, error) {
	// Check to see if table schemas have been overridden
	schemaRoot, err := resolveOverriddenSchemaRoot(ctx, db)
	if err != nil {
		return nil, false, err
	}
	if schemaRoot == nil {
		return nil, false, nil
	}

	// If schema overrides are in place, see if the table exists in the overridden schema
	t, _, ok, err := schemaRoot.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	// Load the overridden schema and convert it to a sql.Schema
	overriddenSchema, err := t.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}
	overriddenSqlSchema, err := sqlutil.FromDoltSchema(db.Name(), tblName, overriddenSchema)
	if err != nil {
		return nil, false, err
	}

	// Return an empty table with the overridden schema
	emptyTable := plan.NewEmptyTableWithSchema(overriddenSqlSchema.Schema)
	return emptyTable.(sql.Table), true, nil
}

// overrideSchemaForTable loads the schema from |overriddenSchemaRoot| for the table named |tableName| and sets the
// override on |tbl|. If there are any problems loading the overridden schema, this function returns an error.
func overrideSchemaForTable(ctx *sql.Context, tableName string, tbl *doltdb.Table, overriddenSchemaRoot *doltdb.RootValue) error {
	overriddenTable, _, ok, err := overriddenSchemaRoot.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return fmt.Errorf("unable to find table '%s' at overridden schema root: %s", tableName, err.Error())
	}
	if !ok {
		return fmt.Errorf("unable to find table '%s' at overridden schema root", tableName)
	}
	overriddenSchema, err := overriddenTable.GetSchema(ctx)
	if err != nil {
		return fmt.Errorf("unable to load overridden schema for table '%s': %s", tableName, err.Error())
	}

	tbl.OverrideSchema(overriddenSchema)
	return nil
}

// getOverriddenSchemaValue returns a string value of the Dolt schema override session variable. If the
// variable is not set (i.e. NULL or empty string) then this function returns an empty string.
func getOverriddenSchemaValue(ctx *sql.Context) (string, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	varValue, err := doltSession.GetSessionVariable(ctx, dsess.DoltOverrideSchema)
	if err != nil {
		return "", err
	}

	if varValue == nil {
		return "", nil
	}

	varString, ok := varValue.(string)
	if !ok {
		return "", fmt.Errorf("value of %s session variable is not a string", dsess.DoltOverrideSchema)
	}
	return varString, nil
}

// resolveOverriddenSchemaRoot loads the Dolt schema override session variable, resolves the commit reference, and
// loads the RootValue for that commit. If the session variable is not set, this function returns nil. If there are
// any problems resolving the commit or loading the root value, this function returns an error.
func resolveOverriddenSchemaRoot(ctx *sql.Context, db Database) (*doltdb.RootValue, error) {
	overriddenSchemaValue, err := getOverriddenSchemaValue(ctx)
	if err != nil {
		return nil, err
	}

	if overriddenSchemaValue == "" {
		return nil, nil
	}

	commitSpec, err := doltdb.NewCommitSpec(overriddenSchemaValue)
	if err != nil {
		return nil, fmt.Errorf("invalid commit spec specified in %s: %s", dsess.DoltOverrideSchema, err.Error())
	}

	doltSession := dsess.DSessFromSess(ctx.Session)
	headRef, err := doltSession.CWBHeadRef(ctx, db.Name())
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve current working branch head: " + err.Error())
	}

	optionalCommit, err := db.GetDoltDB().Resolve(ctx, commitSpec, headRef)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve schema override value: " + err.Error())
	}

	commit, ok := optionalCommit.ToCommit()
	if !ok {
		return nil, fmt.Errorf("unable to resolve schema override: "+
			"commit '%s' is not present locally in the commit graph", optionalCommit.Addr.String())
	}

	rootValue, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load root value for schema override commit: " + err.Error())
	}

	return rootValue, nil
}

// rowConverterByColTagAndName returns a function that converts a row from |srcSchema| to |targetSchema| using the
// specified |projectedTags| and |projectedColNames|. Projected tags and projected column names are both
// provided so that if a tag changes (such as when a column's type is changed) the mapping can fall back to
// matching by column name.
//
// NOTE: This was forked from the dolt_history system table's rowConverter function, which has slightly different
// behavior. It would be nice to resolve the differences and standardize on how we convert rows between schemas.
// The main differences are:
//  1. The dolt_history_ system tables only maps columns by name and doesn't take into account tags. This
//     implementation prefers mapping by column tags, but will fall back to column names if a column with a specified
//     tag can't be found. This behavior is similar to what we do in the diff system tables. Related to this, the
//     columns to include in the projection are also only specified by name in the dolt_history system tables, but
//     here they need to be specified by tag and then fallback to column name matching if a tag isn't found.
//  2. The dolt_history_ system tables will not map columns unless their types are exactly identical. This is too
//     strict for schema override mapping, so this implementation attempts to convert column values to the target
//     type. If a column value is not compatible with the mapped column type (e.g. a 10-char string in a varchar(5)
//     type), then an error is returned while mapping the schema. This is similar to the behavior of the diff tables
//     but instead of returning an error, they log a warning and return a NULL value.
func rowConverterByColTagAndName(srcSchema, targetSchema schema.Schema, projectedTags []uint64, projectedColNames []string) func(row sql.Row) (sql.Row, error) {
	srcIndexToTargetIndex := make(map[int]int)
	srcIndexToTargetType := make(map[int]typeinfo.TypeInfo)
	for i, targetColumn := range targetSchema.GetAllCols().GetColumns() {
		sourceColumn, found := srcSchema.GetAllCols().GetByTag(targetColumn.Tag)
		if !found {
			sourceColumn, found = srcSchema.GetAllCols().GetByName(targetColumn.Name)
		}

		if found {
			srcIndex := srcSchema.GetAllCols().IndexOf(sourceColumn.Name)
			srcIndexToTargetIndex[srcIndex] = i
			srcIndexToTargetType[srcIndex] = targetColumn.TypeInfo
		}
	}

	return func(row sql.Row) (sql.Row, error) {
		r := make(sql.Row, len(projectedColNames))
		for i, tag := range projectedTags {
			// First try to find the column in the src schema with the matching tag
			// then fallback to a name match, since type changes will change the tag
			srcColumn, found := srcSchema.GetAllCols().GetByTag(tag)
			if !found {
				srcColumn, found = srcSchema.GetAllCols().GetByName(projectedColNames[i])
			}

			if found {
				srcIndex := srcSchema.GetAllCols().IndexOf(srcColumn.Name)
				temp := row[srcIndex]

				temp, _, err := srcIndexToTargetType[srcIndex].ToSqlType().Convert(temp)
				if err != nil {
					return nil, fmt.Errorf("unable to convert value to overridden schema: %s", err.Error())
				}

				r[i] = temp
			}
		}
		return r, nil
	}
}

// newMappingRowIter returns a RowIter that maps results from |wrappedIter| to the overridden schema on |t|.
func newMappingRowIter(ctx *sql.Context, t *DoltTable, wrappedIter sql.RowIter) (sql.RowIter, error) {
	rowConvFunc, err := newRowConverterForDoltTable(ctx, t)
	if err != nil {
		return nil, err
	}

	newRowIter := mappingRowIter{
		child:       wrappedIter,
		rowConvFunc: rowConvFunc,
	}
	return &newRowIter, nil
}

// newRowConverterForDoltTable returns a function that converts rows from the original schema of |t| to the overridden
// schema of |t|.
func newRowConverterForDoltTable(ctx *sql.Context, t *DoltTable) (func(row sql.Row) (sql.Row, error), error) {
	// If there is a schema override, then we need to map the results
	// from the old schema to the new schema
	doltSession := dsess.DSessFromSess(ctx.Session)
	roots, ok := doltSession.GetRoots(ctx, t.db.Name())
	if !ok {
		return nil, fmt.Errorf("unable to get roots for database '%s'", t.db.Name())
	}

	doltSchema, err := sqlutil.ToDoltSchema(ctx, roots.Working, t.Name(), t.sqlSch, roots.Head, t.Collation())
	if err != nil {
		return nil, err
	}

	var projectedColNames []string
	for _, tag := range t.projectedCols {
		column, ok := t.overriddenSchema.GetAllCols().GetByTag(tag)
		if !ok {
			return nil, fmt.Errorf("unable to find column with tag %d in overridden schema", tag)
		}
		projectedColNames = append(projectedColNames, column.Name)
	}

	rowConvFunc := rowConverterByColTagAndName(doltSchema, t.overriddenSchema, t.projectedCols, projectedColNames)
	return rowConvFunc, nil
}

// mappingRowIter is a RowIter that maps rows from a child RowIter to a new schema using a row conversion function.
type mappingRowIter struct {
	child       sql.RowIter
	rowConvFunc func(row sql.Row) (sql.Row, error)
}

var _ sql.RowIter = (*mappingRowIter)(nil)

// Next implements the sql.RowIter interface
func (m *mappingRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	next, err := m.child.Next(ctx)
	if err != nil {
		return next, err
	}

	if m.rowConvFunc == nil {
		return next, nil
	} else {
		return m.rowConvFunc(next)
	}
}

// Close implements the sql.RowIter interface
func (m *mappingRowIter) Close(ctx *sql.Context) error {
	return m.child.Close(ctx)
}
