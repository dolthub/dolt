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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func ValidateDatabase(ctx context.Context, db sqle.Database) (err error) {
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

func validateChunkReferences(ctx context.Context, db sqle.Database) error {
	ddb := db.GetDoltDB()
	bb, err := ddb.GetBranches(ctx)
	if err != nil {
		return err
	}

	for i := range bb {
		var c *doltdb.Commit
		var r *doltdb.RootValue

		c, err = ddb.ResolveCommitRef(ctx, bb[i])
		if err != nil {
			return err
		}
		r, err = c.GetRootValue(ctx)
		if err != nil {
			return err
		}

		err = r.IterTables(ctx, func(_ string, t *doltdb.Table, sch schema.Schema) (stop bool, err error) {
			if sch == nil {
				return true, fmt.Errorf("expected non-nil schema: %v", sch)
			}

			rows, err := t.GetRowData(ctx)
			if err != nil {
				return true, err
			}
			if err = validateIndexChunkReferences(ctx, rows); err != nil {
				return false, err
			}

			indexes, err := t.GetIndexSet(ctx)
			if err != nil {
				return true, err
			}
			err = durable.IterAllIndexes(ctx, sch, indexes, func(_ string, idx durable.Index) error {
				return validateIndexChunkReferences(ctx, idx)
			})
			if err != nil {
				return false, err
			}
			return
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func validateIndexChunkReferences(ctx context.Context, idx durable.Index) error {
	pm := durable.ProllyMapFromIndex(idx)
	return pm.WalkNodes(ctx, func(ctx context.Context, nd tree.Node) error {
		if nd.Size() <= 0 {
			return fmt.Errorf("encountered nil tree.Node")
		}
		return nil
	})
}
