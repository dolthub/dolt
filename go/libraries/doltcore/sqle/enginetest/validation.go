// Copyright 2020 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func ValidateDatabase(ctx context.Context, db sql.Database) (err error) {
	switch tdb := db.(type) {
	case sqle.Database:
		return ValidateDoltDatabase(ctx, tdb)
	case mysql_db.PrivilegedDatabase:
		return ValidateDatabase(ctx, tdb.Unwrap())
	default:
		return nil
	}
}

func ValidateDoltDatabase(ctx context.Context, db sqle.Database) (err error) {
	if !types.IsFormat_DOLT_1(db.GetDoltDB().Format()) {
		return nil
	}
	for _, stage := range validationStages {
		if err = stage(ctx, db); err != nil {
			return err
		}
	}
	return
}

type validator func(ctx context.Context, db sqle.Database) error

var validationStages = []validator{
	validateChunkReferences,
}

// validateChunkReferences checks for dangling chunks.
func validateChunkReferences(ctx context.Context, db sqle.Database) error {
	validateIndex := func(ctx context.Context, idx durable.Index) error {
		pm := durable.ProllyMapFromIndex(idx)
		return pm.WalkNodes(ctx, func(ctx context.Context, nd tree.Node) error {
			if nd.Size() <= 0 {
				return fmt.Errorf("encountered nil tree.Node")
			}
			return nil
		})
	}

	cb := func(n string, t *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		if sch == nil {
			return true, fmt.Errorf("expected non-nil schema: %v", sch)
		}

		rows, err := t.GetRowData(ctx)
		if err != nil {
			return true, err
		}
		if err = validateIndex(ctx, rows); err != nil {
			return true, err
		}

		indexes, err := t.GetIndexSet(ctx)
		if err != nil {
			return true, err
		}
		err = durable.IterAllIndexes(ctx, sch, indexes, func(_ string, idx durable.Index) error {
			return validateIndex(ctx, idx)
		})
		if err != nil {
			return true, err
		}
		return
	}

	return iterDatabaseTables(ctx, db, cb)
}

// iterDatabaseTables is a utility to factor out common validation access patterns.
func iterDatabaseTables(
	ctx context.Context,
	db sqle.Database,
	cb func(name string, t *doltdb.Table, sch schema.Schema) (bool, error),
) error {
	ddb := db.GetDoltDB()
	branches, err := ddb.GetBranches(ctx)
	if err != nil {
		return err
	}

	for i := range branches {
		var c *doltdb.Commit
		var r *doltdb.RootValue

		c, err = ddb.ResolveCommitRef(ctx, branches[i])
		if err != nil {
			return err
		}
		r, err = c.GetRootValue(ctx)
		if err != nil {
			return err
		}
		if err = r.IterTables(ctx, cb); err != nil {
			return err
		}
	}
	return nil
}
