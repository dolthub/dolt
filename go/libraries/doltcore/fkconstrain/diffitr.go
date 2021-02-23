// Copyright 2021 Dolthub, Inc.
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

package fkconstrain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	nomsdiff "github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

type mapIterAsRowDiffer struct {
	itr types.MapIterator
	ctx context.Context
}

// Start starts the RowDiffer.
func (mitr mapIterAsRowDiffer) Start(_ context.Context, _, _ types.Map) {}

// GetDiffs returns the requested number of diff.Differences, or times out.
func (mitr mapIterAsRowDiffer) GetDiffs(_ int, _ time.Duration) ([]*nomsdiff.Difference, bool, error) {
	k, v, err := mitr.itr.Next(mitr.ctx)

	if err != nil {
		return nil, false, err
	}

	if k == nil {
		return nil, false, nil
	}

	return []*nomsdiff.Difference{{
		ChangeType:  types.DiffChangeAdded,
		OldValue:    nil,
		NewValue:    v,
		NewKeyValue: k,
		KeyValue:    k,
	}}, true, nil
}

// Close closes the RowDiffer.
func (mitr mapIterAsRowDiffer) Close() error {
	return nil
}

func getDiffItr(ctx context.Context, parentCommitRoot, root *doltdb.RootValue, tblName string) (diff.RowDiffer, error) {
	parentOK := true
	pTbl, pSch, err := getTableAndSchema(ctx, parentCommitRoot, tblName)

	if errors.Is(err, doltdb.ErrTableNotFound) {
		parentOK = false
	} else if err != nil {
		return nil, err
	}

	tbl, sch, err := getTableAndSchema(ctx, root, tblName)

	if err != nil {
		return nil, err
	}

	rowData, err := tbl.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	if !parentOK {
		itr, err := rowData.Iterator(ctx)

		if err != nil {
			return nil, err
		}

		return mapIterAsRowDiffer{itr: itr, ctx: ctx}, nil
	}

	pRowData, err := pTbl.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	rd := diff.NewRowDiffer(ctx, pSch, sch, 1024)
	rd.Start(ctx, pRowData, rowData)

	return rd, nil
}

func getTableAndSchema(ctx context.Context, root *doltdb.RootValue, tableName string) (*doltdb.Table, schema.Schema, error) {
	tbl, _, ok, err := root.GetTableInsensitive(ctx, tableName)

	if err != nil {
		return nil, nil, err
	} else if !ok {
		return nil, nil, fmt.Errorf("%w: %s", doltdb.ErrTableNotFound, tableName)
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, nil, err
	}

	return tbl, sch, nil
}
