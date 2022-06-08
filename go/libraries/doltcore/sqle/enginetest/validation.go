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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
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
	validateSecondaryIndexes,
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

// validateSecondaryIndexes checks that secondary index contents are consistent
// with primary index contents.
func validateSecondaryIndexes(ctx context.Context, db sqle.Database) error {
	cb := func(n string, t *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		rows, err := t.GetRowData(ctx)
		if err != nil {
			return false, err
		}
		primary := durable.ProllyMapFromIndex(rows)

		for _, def := range sch.Indexes().AllIndexes() {
			set, err := t.GetIndexSet(ctx)
			if err != nil {
				return true, err
			}
			idx, err := set.GetIndex(ctx, sch, def.Name())
			if err != nil {
				return true, err
			}
			secondary := durable.ProllyMapFromIndex(idx)

			err = validateIndexConsistency(ctx, sch, def, primary, secondary)
			if err != nil {
				return true, err
			}
		}
		return false, nil
	}
	return iterDatabaseTables(ctx, db, cb)
}

func validateIndexConsistency(
	ctx context.Context,
	sch schema.Schema,
	def schema.Index,
	primary, secondary prolly.Map,
) error {
	// secondary indexes have empty keys
	idxDesc, _ := secondary.Descriptors()
	builder := val.NewTupleBuilder(idxDesc)
	mapping := ordinalMappingsForSecondaryIndex(sch, def)

	kd, _ := primary.Descriptors()
	pkSize := kd.Count()
	iter, err := primary.IterAll(ctx)
	if err != nil {
		return err
	}

	for {
		key, value, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// make secondary index key
		for i := range mapping {
			j := mapping.MapOrdinal(i)
			if j < pkSize {
				builder.PutRaw(i, key.GetField(j))
			} else {
				builder.PutRaw(i, value.GetField(j-pkSize))
			}
		}
		k := builder.Build(primary.Pool())

		ok, err := secondary.Has(ctx, k)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("index key %v not found in index %s", k, def.Name())
		}
	}
}

func ordinalMappingsForSecondaryIndex(sch schema.Schema, def schema.Index) (ord val.OrdinalMapping) {
	// assert empty values for secondary indexes
	if def.Schema().GetNonPKCols().Size() > 0 {
		panic("expected empty secondary index values")
	}

	secondary := def.Schema().GetPKCols()
	ord = make(val.OrdinalMapping, secondary.Size())

	for i := range ord {
		name := secondary.GetAtIndex(i).Name
		ord[i] = -1

		pks := sch.GetPKCols().GetColumns()
		for j, col := range pks {
			if col.Name == name {
				ord[i] = j
			}
		}
		vals := sch.GetNonPKCols().GetColumns()
		for j, col := range vals {
			if col.Name == name {
				ord[i] = j + len(pks)
			}
		}
		if ord[i] < 0 {
			panic("column " + name + " not found")
		}
	}
	return
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
