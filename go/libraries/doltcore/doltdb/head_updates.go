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

package doltdb

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// HeadUpdate is an update to a single head to commit as part of an atomic batch.
type HeadUpdate interface {
	// BuildDatasetUpdate returns the datas.DatasetUpdate update corresponding to this head update
	BuildDatasetUpdate(ctx context.Context, ddb *DoltDB) (datas.DatasetUpdate, error)
}

// WorkingSetUpdate replaces a working set, provided its current hash equals PrevHash.
type WorkingSetUpdate struct {
	WorkingSet *WorkingSet
	PrevHash   hash.Hash
	Meta       *datas.WorkingSetMeta
}

var _ HeadUpdate = WorkingSetUpdate{}

func (u WorkingSetUpdate) BuildDatasetUpdate(ctx context.Context, ddb *DoltDB) (datas.DatasetUpdate, error) {
	spec, err := u.WorkingSet.writeValues(ctx, ddb, u.Meta, false)
	if err != nil {
		return nil, err
	}
	return datas.WorkingSetUpdate{
		WorkingSetDS: u.WorkingSet.Ref().String(),
		WorkingSet:   *spec,
		PrevWsHash:   u.PrevHash,
	}, nil
}

// BranchHeadUpdate creates a commit and updates its branch head.
type BranchHeadUpdate struct {
	HeadRef       ref.DoltRef
	Root          RootValue
	CommitOptions datas.CommitOptions
	// ExpectedHead is the head used to prepare the commit. When nonzero, both
	// this hash and the dataset snapshot taken by BuildDatasetUpdate are checked.
	ExpectedHead hash.Hash
	// WorkingSetRef optionally locks an associated working set at PrevWsHash.
	// A zero reference omits this additional lock; the branch head is always checked.
	WorkingSetRef ref.WorkingSetRef
	PrevWsHash    hash.Hash
}

var _ HeadUpdate = BranchHeadUpdate{}

func (u BranchHeadUpdate) BuildDatasetUpdate(ctx context.Context, ddb *DoltDB) (datas.DatasetUpdate, error) {
	ds, err := ddb.db.GetDataset(ctx, u.HeadRef.String())
	if err != nil {
		return nil, err
	}
	var wsID string
	if u.WorkingSetRef.GetPath() != "" {
		wsID = u.WorkingSetRef.String()
	}
	return datas.CommitUpdate{
		CommitDS:     ds,
		ExpectedHead: u.ExpectedHead,
		CommitOpts:   u.CommitOptions,
		WorkingSetDS: wsID,
		PrevWsHash:   u.PrevWsHash,
		RootVal:      u.Root.NomsValue(),
	}, nil
}
