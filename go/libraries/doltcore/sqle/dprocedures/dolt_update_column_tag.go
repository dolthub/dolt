// Copyright 2025 Dolthub, Inc.
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

package dprocedures

import (
	"fmt"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// doltUpdateColumnTag updates the tag for a specified column, leaving the change in the working set to later be
// committed.
func doltUpdateColumnTag(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	tableName, columnName, tag, err := parseUpdateColumnTagArgs(args...)
	if err != nil {
		return nil, err
	}

	doltSession := dsess.DSessFromSess(ctx.Session)
	roots, ok := doltSession.GetRoots(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return nil, fmt.Errorf("unable to load roots")
	}
	root := roots.Working

	tbl, tName, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: tableName})
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	newSch, err := updateColumnTag(sch, columnName, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to update column tag: %w", err)
	}

	tbl, err = tbl.UpdateSchema(ctx, newSch)
	if err != nil {
		return nil, fmt.Errorf("failed to update table schema: %w", err)
	}

	root, err = root.PutTable(ctx, doltdb.TableName{Name: tName}, tbl)
	if err != nil {
		return nil, fmt.Errorf("failed to put table in root: %w", err)
	}

	if err = doltSession.SetWorkingRoot(ctx, ctx.GetCurrentDatabase(), root); err != nil {
		return nil, err
	}

	return rowToIter(int64(0)), nil
}

// parseUpdateColumnTagArgs parses |args| and returns the tableName, columnName, and tag specified, otherwise
// returns an error if there were any problems.
func parseUpdateColumnTagArgs(args ...string) (tableName, columnName string, tag uint64, err error) {
	apr, err := cli.ParseArgs(cli.CreateUpdateTagArgParser(), args, nil)
	if err != nil {
		return "", "", 0, err
	}
	if len(apr.Args) != 3 {
		return "", "", 0,
			fmt.Errorf("incorrect number of arguments: must provide <table> <column> <tag>")
	}

	tableName, columnName, tagStr := apr.Args[0], apr.Args[1], apr.Args[2]

	tag, err = strconv.ParseUint(tagStr, 10, 64)
	if err != nil {
		return "", "", 0, fmt.Errorf("failed to parse tag %s: %w", tagStr, err)
	}

	return tableName, columnName, tag, nil
}

// updateColumnTag updates |sch| by setting the tag for the column named |name| to |tag|.
func updateColumnTag(sch schema.Schema, name string, tag uint64) (schema.Schema, error) {
	var found bool
	columns := sch.GetAllCols().GetColumns()
	// Find column and update its tag
	for i, col := range columns {
		if col.Name == name {
			col.Tag = tag
			columns[i] = col
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("column %s does not exist", name)
	}

	newSch, err := schema.SchemaFromCols(schema.NewColCollection(columns...))
	if err != nil {
		return nil, err
	}

	if err = newSch.SetPkOrdinals(sch.GetPkOrdinals()); err != nil {
		return nil, err
	}
	newSch.SetCollation(sch.GetCollation())

	return newSch, nil
}
